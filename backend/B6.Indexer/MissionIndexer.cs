using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Nethereum.ABI.FunctionEncoding.Attributes;
using Nethereum.Contracts;
using Nethereum.Contracts.ContractHandlers;
using Nethereum.Hex.HexTypes;
using Nethereum.RPC.Eth.DTOs;      
using Nethereum.Web3;
using Nethereum.Web3.Accounts;      
using Npgsql;
using NpgsqlTypes;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;         
using System.Globalization;
using System.IO; 
using System.Linq; 
using System.Net.Http;        
using System.Net.Http.Json; 
using System.Numerics;
using System.Runtime.CompilerServices;

using B6.Contracts;

namespace B6.Indexer
{
    public class MissionIndexer : BackgroundService
    {
        // Logger on/off ------------------------------------------------------------------------------------------------
        private static readonly bool                    logRpcCalls         = false;                                        // ← toggle RPC file logging
        // --------------------------------------------------------------------------------------------------------------
        private readonly ILogger<MissionIndexer>        _log;                                                               // injected
        private readonly string                         _rpc;                                                               // primary RPC endpoint
        private readonly string                         _factory;                                                           // MissionFactory contract address              
        private readonly string                         _pg;                                                                // Postgres connection string                                  
        private Web3                                    _web3               = default!;                                     // current RPC client
        private readonly List<string>                   _rpcEndpoints       = [];                                           // pool of RPC endpoints
        private int                                     _rpcIndex           = 0;                                            // current RPC endpoint index
        private readonly HttpClient                     _http               = new();                                        // for push notifications
        private readonly string                         _pushBase;                                                          // e.g. https://b6missions.com/api
        private readonly string                         _pushKey;                                                           // e.g. secret key for push auth
        private readonly object                         _rpcLogLock         = new();
        private DateTime                                _rpcLogDay          = DateTime.MinValue;                            // UTC date boundary
        private string                                  _rpcLogPath         = string.Empty;                                 // current log file path
        private readonly Dictionary<string,int>         _rpcCounts          = new(StringComparer.InvariantCulture);         // kind → count
        // ------- NEW: 5-minute RPC summary counters --------------------------------------------------------------------
        private readonly Dictionary<string,int>         _rpc5mByContext     = new(StringComparer.InvariantCulture);         // context → count
        private readonly Dictionary<string, Dictionary<string,int>> _rpc1hByCaller = new(StringComparer.InvariantCulture);  // caller → (context → count)
        private DateTime                                _nextRpcSummaryUtc  = DateTime.MinValue;                            // next summary time
        private bool                                    _firstRPCSummary    = true;                                         // first-ever flag
        private static readonly TimeSpan                _rpcSummaryPeriod   = TimeSpan.FromMinutes(60);                     // every 60 minutes
        // --------------------------------------------------------------------------------------------------------------
        // --- Benign provider hiccup rollup (daily, UTC) ---------------------------------------------------------------
        private DateTime                                _benignDayUtc       = DateTime.MinValue;                            // current day
        private readonly Dictionary<string,int>         _benignCounts       = new(StringComparer.InvariantCulture);         // key = "{kind}.{code}" → count
        private static readonly TimeSpan                _benignRetryDelay   = TimeSpan.FromMilliseconds(800);               // wait before retrying benign errors
        // --------------------------------------------------------------------------------------------------------------      
        private static readonly TimeSpan                RATE_LIMIT_COOLDOWN = TimeSpan.FromSeconds(30);                     // wait after 429 before retrying

        private volatile bool                           _kickRequested      = false;                                        // set by listener
        private readonly ConcurrentQueue<string>        _kickMissions       = new();                                        // mission addresses to refresh
        private ulong                                   _factoryLastSeq     = 0;

        private DateTime                                _activeTickNextUtc;                                                 // next active poll time

        private Account?                                _finalizerAccount;
        private readonly string                         _ownerPk            = string.Empty;

        private static readonly TimeSpan                ActivePoll          = TimeSpan.FromSeconds(5);                      // every 5 seconds

        private readonly ConcurrentDictionary<string, EndWatch> _endWatch = new(StringComparer.OrdinalIgnoreCase);

        private sealed class EndWatch {
            public string      Mission      = "";
            public DateTime    EndAtUtc;       // mission_end
            public DateTime    EnrollStartUtc; // enrollment_start  (Pending ends)
            public DateTime    EnrollEndUtc;   // enrollment_end    (Enrolling ends)
            public DateTime    StartAtUtc;     // mission_start     (Arming ends)
            public bool        Finalized    = false;
            public int         Attempts     = 0;
            public DateTime    NextTryUtc   = DateTime.MinValue; // finalize() backoff
            public DateTime    NextPollUtc  = DateTime.MinValue; // time-based snapshot poll at es/ee/ms/me
            public int         RefundAttempts  = 0;
            public DateTime    RefundNextTryUtc = DateTime.MinValue;
        }

        // --------------------------------------------------------------------------------------------------------------

        [Function("getChangesAfter", typeof(GetChangesAfterOutput))]
        public class GetChangesAfterFunction : FunctionMessage {
            [Parameter("uint64", "lastSeq", 1)]
            public ulong LastSeq { get; set; }
        }

        [FunctionOutput]
        public class GetChangesAfterOutput : IFunctionOutputDTO {
            [Parameter("address[]", "missions",   1)]
            public List<string> Missions   { get; set; } = new();

            [Parameter("uint40[]",  "timestamps", 2)]
            public List<System.Numerics.BigInteger> Timestamps { get; set; } = new();

            [Parameter("uint64[]",  "seqs",       3)]
            public List<System.Numerics.BigInteger> Seqs       { get; set; } = new();

            [Parameter("uint8[]",   "statuses",   4)]
            public List<byte> Statuses   { get; set; } = new();
        }

        [Function("forceFinalizeMission")]
        public class ForceFinalizeMissionFunction : FunctionMessage { }

        private sealed class                            SnapshotChanges {
            public bool HasMeaningfulChange { get; set; }
            public (short From, short To)? StatusTransition { get; set; }
            public short? NewRound { get; set; }
        }

        private static DateTime FromUnix(BigInteger ts) => DateTimeOffset.FromUnixTimeSeconds((long)ts).UtcDateTime;

        public                                          MissionIndexer                      (ILogger<MissionIndexer> log, IConfiguration cfg) {
            _log = log;

            // Global crash logging – helps explain unexpected restarts
            AppDomain.CurrentDomain.UnhandledException += (s, e) =>
            {
                try
                {
                    var ex = e.ExceptionObject as Exception;
                    if (ex != null)
                        _log.LogCritical(ex, "AppDomain.UnhandledException (IsTerminating={term})", e.IsTerminating);
                    else
                        _log.LogCritical("AppDomain.UnhandledException non-Exception payload: {obj}", e.ExceptionObject);
                }
                catch { /* last-chance */ }
            };

            TaskScheduler.UnobservedTaskException += (s, e) =>
            {
                try
                {
                    _log.LogCritical(e.Exception, "TaskScheduler.UnobservedTaskException");
                    e.SetObserved(); // avoid escalation if possible
                }
                catch { }
            };

            _rpc     = cfg["Cronos:Rpc"] 
                    ?? throw new InvalidOperationException("Missing configuration key: Cronos:Rpc");
            _factory = cfg["Contracts:Factory"] 
                    ?? throw new InvalidOperationException("Missing configuration key: Contracts:Factory");
            _pg      = cfg.GetConnectionString("Db") 
                    ?? throw new InvalidOperationException("Missing connection string: Db");

            // NEW: signer preference = Owner:PK (or Owner--PK in Key Vault). Fallback to Cronos:Finalizer:PrivateKey.
            // --- push config (optional; if empty, pushing is disabled) ---
            _pushBase = cfg["Push:BaseUrl"] ?? "";
            _pushKey  = cfg["Push:Key"]     ?? "";            
            _ownerPk  = cfg["Owner:PK"]     ?? cfg["Owner--PK"] ?? string.Empty;

            try
            {
                if (!string.IsNullOrWhiteSpace(_ownerPk))
                {
                    _finalizerAccount = new Account(_ownerPk);
                }
                else
                {
                    _finalizerAccount = null;
                }
            }
            catch (Exception ex)
            {
                _log.LogError(ex, "Signer configuration invalid (Owner:PK). Finalize disabled.");
                _finalizerAccount = null;
            }

            // Normalize addresses
            _factory = _factory.ToLowerInvariant();

            // Build RPC pool: primary + optional backup(s)
            if (!string.IsNullOrWhiteSpace(_rpc)) _rpcEndpoints.Add(_rpc);

            // Try Cronos:Rpc2 as a backup (in Key Vault: Cronos--Rpc2)
            var rpc2 = cfg["Cronos:Rpc2"];
            if (!string.IsNullOrWhiteSpace(rpc2)) _rpcEndpoints.Add(rpc2);

            if (_rpcEndpoints.Count == 0)
                throw new InvalidOperationException("No RPC endpoints configured (Cronos:Rpc and/or Cronos:Rpc2).");

            // start with first endpoint
            UseRpc(0);

            _nextRpcSummaryUtc = DateTime.UtcNow.AddMinutes(1);


        }

        protected override async Task                   ExecuteAsync                        (CancellationToken token) {
            // Snapshot-based indexer with state-aware cadence and POST kicks
            try
            {
                // Keep kick listener (DB NOTIFY + fallback queue)
                _ = Task.Run(() => ListenForKicksAsync(token), token);

                // Warm-up
                _activeTickNextUtc   = DateTime.UtcNow;
                // Load factory change cursor (lastSeq = 0 if table/row missing)
                _factoryLastSeq = await LoadFactoryLastSeqAsync(token);

                // NEW: prime the end-watch with all not-ended missions from DB
                await LoadEndWatchAsync(token);

            }
            catch (Exception ex)
            {
                _log.LogError(ex, "Scheduler bootstrap failed");
            }

            while (!token.IsCancellationRequested)
            {
                try
                {
                    // 0) Process kicks first (mission-created / enroll-succeeded / bank-succeeded)
                    if (_kickRequested || !_kickMissions.IsEmpty)
                    {
                        _kickRequested = false;
                        await ProcessPendingKicksAsync(token);
                        await ProcessKickQueueAsync(token); // refresh snapshots for kicked missions
                    }

                    var now = DateTime.UtcNow;

                    // Single predictable poll every 5 seconds: factory change-set only
                    if (now >= _activeTickNextUtc)
                    {
                        await RefreshFactoryChangesAsync(token);
                        _activeTickNextUtc = now.Add(ActivePoll);
                    }

                    // Time-based snapshot polls at enrollment_start / enrollment_end / mission_start / mission_end
                    if (!_endWatch.IsEmpty)
                    {
                        var polls = _endWatch.Values
                            .Where(w => w.NextPollUtc != DateTime.MinValue && now >= w.NextPollUtc)
                            .Take(20)
                            .ToList();

                        foreach (var w in polls)
                        {
                            try
                            {
                                await RefreshMissionSnapshotAsync(w.Mission, token); // will push if status/pool/round changed
                            }
                            catch { /* best effort */ }

                            // Reschedule to the next future boundary among (es, ee, ms, me); else disable
                            var next = new[] { w.EnrollStartUtc, w.EnrollEndUtc, w.StartAtUtc, w.EndAtUtc }
                                .Where(t => t > DateTime.UtcNow)
                                .DefaultIfEmpty(DateTime.MinValue)
                                .Min();
                            w.NextPollUtc = next;
                        }
                    }

                    // attempt finalize() for missions that just passed mission_end
                    if (!_endWatch.IsEmpty)
                    {
                        var due = _endWatch.Values
                            .Where(w => !w.Finalized && now >= w.EndAtUtc && now >= w.NextTryUtc)
                            .Take(10)
                            .ToList();

                        foreach (var w in due)
                        {
                            // 0) Heal stale DB rows first so ShouldForceFinalizeAsync sees correct status (>5 → skip)
                            try { await RefreshMissionSnapshotAsync(w.Mission, token); } catch { /* best effort */ }

                            var ok = await AttemptFinalizeAsync(w.Mission, token);

                            // 1) Regardless of success, sync snapshot so DB reflects the contract-derived status
                            try { await RefreshMissionSnapshotAsync(w.Mission, token); } catch { /* best effort */ }

                            if (ok)
                            {
                                _endWatch.TryRemove(w.Mission, out _);
                            }
                            else
                            {
                                w.Attempts++;

                                if (w.Attempts >= 3)
                                {
                                    _endWatch.TryRemove(w.Mission, out _);
                                    continue;
                                }

                                var next = w.Attempts switch
                                {
                                    1 => TimeSpan.FromSeconds(5),
                                    2 => TimeSpan.FromSeconds(10),
                                    _ => TimeSpan.FromSeconds(30)
                                };
                                w.NextTryUtc = DateTime.UtcNow + next;
                            }
                        }
                    }

                    // Sweep DB for Failed+pending refunds that are not yet in watch (handles legacy/manual flips)
                    try { await SeedRefundWatchAsync(token); } catch { /* best effort */ }

                    // attempt refundPlayers() for Failed missions with refunds pending
                    if (!_endWatch.IsEmpty)
                    {
                    var dueRefund = _endWatch.Values
                        .Where(w => now >= w.RefundNextTryUtc)
                        .Take(10)
                        .ToList();

                        foreach (var w in dueRefund)
                        {
                            // Ensure DB snapshot is current before deciding
                            try { await RefreshMissionSnapshotAsync(w.Mission, token); } catch { /* best effort */ }

                            var ok = await AttemptRefundAsync(w.Mission, token);

                            // Snapshot after attempt so all_refunded is persisted if it changed
                            try { await RefreshMissionSnapshotAsync(w.Mission, token); } catch { /* best effort */ }

                            if (ok)
                            {
                                _endWatch.TryRemove(w.Mission, out _);
                            }
                            else
                            {
                                w.RefundAttempts++;

                                if (w.RefundAttempts >= 2)
                                {
                                    // After 2 failed attempts, mark finalized = true to stop watching this mission
                                    try
                                    {
                                        await using var conn = new NpgsqlConnection(_pg);
                                        await conn.OpenAsync(token);
                                        await using var fin = new NpgsqlCommand(@"
                                            update missions set finalized = true, updated_at = now()
                                            where mission_address = @a;", conn);
                                        fin.Parameters.AddWithValue("a", w.Mission);
                                        await fin.ExecuteNonQueryAsync(token);
                                    }
                                    catch { /* best effort */ }

                                    _endWatch.TryRemove(w.Mission, out _);
                                    continue;
                                }

                                var next = w.RefundAttempts switch
                                {
                                    1 => TimeSpan.FromSeconds(5),
                                    _ => TimeSpan.FromSeconds(10)
                                };
                                w.RefundNextTryUtc = DateTime.UtcNow + next;
                            }
                        }
                    }
                }
                catch (OperationCanceledException) when (token.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _log.LogError(ex, "Scheduler loop failed");
                }
                FlushRpcSummaryIfDue();
                // Light beat for low-latency kicks; cadence timers throttle the real work
                try { await Task.Delay(TimeSpan.FromSeconds(1), token); } catch { }
            }
        }

        private async Task<ulong>                       LoadFactoryLastSeqAsync             (CancellationToken ct) {
            try
            {
                await using var conn = new Npgsql.NpgsqlConnection(_pg);
                await conn.OpenAsync(ct);
                await using var cmd = new Npgsql.NpgsqlCommand(
                    "select last_seq from indexer_factory_cursor where id = 1;", conn);
                var o = await cmd.ExecuteScalarAsync(ct);
                if (o == null || o is DBNull) return 0UL;

                // PG bigint → long → ulong (non-negative)
                var v = Convert.ToInt64(o);
                return v <= 0 ? 0UL : (ulong)v;
            }
            catch
            {
                // Table may not exist yet; treat as cold start
                return 0UL;
            }
        }

        private async Task                              ListenForKicksAsync                 (CancellationToken token) {
            try
            {
                await using var conn = new Npgsql.NpgsqlConnection(_pg);
                await conn.OpenAsync(token);

                // Ensure the table exists is handled by API migration; just LISTEN here
                await using (var cmd = new Npgsql.NpgsqlCommand("LISTEN b6_indexer_kick;", conn))
                    await cmd.ExecuteNonQueryAsync(token);

                conn.Notification += async (_, e) =>
                {
                    try
                    {
                        var mission = (e.Payload ?? string.Empty).ToLowerInvariant();
                        if (!string.IsNullOrWhiteSpace(mission))
                            _kickMissions.Enqueue(mission);

                        _kickRequested = true;
                        // Optional: also sweep pending kick rows to be safe
                        await ProcessPendingKicksAsync(CancellationToken.None);
                    }
                    catch { /* swallow */ }
                };

                // Wait loop to receive notifications
                while (!token.IsCancellationRequested)
                {
                    await conn.WaitAsync(token);
                }
            }
            catch (OperationCanceledException) when (token.IsCancellationRequested) { }
            catch (Exception ex)
            {
                _log.LogWarning(ex, "Kick listener failed; continuing without NOTIFY/LISTEN");
            }
        }

        private async Task                              ProcessPendingKicksAsync            (CancellationToken token) {
            try
            {
                await using var conn = new Npgsql.NpgsqlConnection(_pg);
                await conn.OpenAsync(token);

                // Fetch and delete pending kicks (best-effort dedupe)
                var kicks = new List<string>();
                await using (var cmd = new Npgsql.NpgsqlCommand(@"
                    delete from indexer_kicks
                    where id in (
                        select id from indexer_kicks
                        order by id
                        limit 200
                    )
                    returning mission_address;", conn))
                await using (var rd = await cmd.ExecuteReaderAsync(token))
                {
                    while (await rd.ReadAsync(token))
                        kicks.Add((rd.GetString(0) ?? string.Empty).ToLowerInvariant());
                }

                foreach (var m in kicks)
                    _kickMissions.Enqueue(m);

                if (kicks.Count > 0)
                    _kickRequested = true;
            }
            catch (Exception ex)
            {
                _log.LogDebug(ex, "ProcessPendingKicksAsync failed");
            }
        }

        private async Task                              RefreshFactoryChangesAsync          (CancellationToken token) {
            // 1) Query factory for changes after our last sequence
            var output = await RunRpc(
                w => w.Eth.GetContractQueryHandler<GetChangesAfterFunction>()
                        .QueryDeserializingToObjectAsync<GetChangesAfterOutput>(
                                new GetChangesAfterFunction { LastSeq = _factoryLastSeq }, _factory, null),
                "Call.getChangesAfter");

            if (output == null || output.Missions == null || output.Missions.Count == 0)
                return;

            // 2) Refresh only the changed missions
            ulong newMaxSeq = _factoryLastSeq;
            var count = output.Missions.Count;

            for (int i = 0; i < count; i++)
            {
                var mission = (output.Missions[i] ?? string.Empty).ToLowerInvariant();
                if (string.IsNullOrWhiteSpace(mission)) continue;

                try
                {
                    // Single source of truth: refresh will UPSERT the row
                    await RefreshMissionSnapshotAsync(mission, token);
                }
                catch (Exception ex)
                {
                    _log.LogWarning(ex, "Factory change refresh failed for {mission}", mission);
                }

                // Track max seq
                if (output.Seqs != null && i < output.Seqs.Count)
                {
                    try
                    {
                        var seqVal = (ulong)output.Seqs[i];
                        if (seqVal > newMaxSeq) newMaxSeq = seqVal;
                    }
                    catch { /* ignore cast issues */ }
                }
            }

            // 3) Advance and persist cursor
            if (newMaxSeq != _factoryLastSeq)
            {
                _factoryLastSeq = newMaxSeq;
                await SaveFactoryLastSeqAsync(_factoryLastSeq, token);
            }
        }

        private async Task                              RefreshMissionSnapshotAsync         (string mission, CancellationToken token) {
            var wrap = await RunRpc(
                w => w.Eth.GetContractQueryHandler<B6.Contracts.GetMissionDataFunction>()
                    .QueryDeserializingToObjectAsync<B6.Contracts.MissionDataWrapper>(
                        new B6.Contracts.GetMissionDataFunction(), mission, null),
                "Call.getMissionData");

            var md = wrap.Data; // includes: players, missionType, schedule, rounds, croStart/current, wins, refunds, pauseTimestamp, name, created

            // Apply the snapshot to DB and derive deltas
            var changes = await ApplySnapshotToDatabaseAsync(mission, md, token);

            // Push only when meaningful deltas exist
            if (changes.HasMeaningfulChange)
            {
                await NotifyMissionUpdatedAsync(mission, token);

                if (changes.StatusTransition != null)
                {
                    await NotifyStatusAsync(mission, changes.StatusTransition.Value.To, token);
                }

                if (changes.NewRound != null)
                {
                    string winner = "";
                    string amountWei = "0"; // snapshot doesn't carry per-round payout

                    try
                    {
                        var ix = changes.NewRound.Value - 1;
                        if (ix >= 0 && ix < md.Players.Count)
                            winner = (md.Players[ix].Player ?? "").ToLower(System.Globalization.CultureInfo.InvariantCulture);
                    }
                    catch { /* best effort */ }

                    await NotifyRoundAsync(mission, changes.NewRound.Value, winner, amountWei, token);
                }

            }

        }

        private async Task<SnapshotChanges>             ApplySnapshotToDatabaseAsync        (string mission, MissionDataTuple md, CancellationToken token) {
            var changes = new SnapshotChanges { HasMeaningfulChange = false };

            short? oldStatus = null;
            short? newStatus = null;
            short? oldRound  = null;
            short? newRound  = (short)md.RoundCount;

            // Read current stored values (status/round/cro/name/type/finalized/etc)
            short? curStatus = null;
            short? curRound  = null;
            string? curName  = null;
            byte?   curType  = null;
            bool    curFinal = false;
            string? curCroNow = null;   // read as text to avoid numeric precision quirks
            long?   curPauseTs = null;

            await using (var conn = new NpgsqlConnection(_pg))
            {
                await conn.OpenAsync(token);

                // Load current
                await using (var read = new NpgsqlCommand(@"
                    select 
                        status, 
                        round_count, 
                        name, 
                        mission_type, 
                        coalesce(finalized,false),
                        cro_current_wei::text,
                        pause_timestamp
                    from missions 
                    where mission_address = @a;", conn))
                {
                    read.Parameters.AddWithValue("a", mission);
                    await using var rdr = await read.ExecuteReaderAsync(token);
                    if (await rdr.ReadAsync(token))
                    {
                        if (!rdr.IsDBNull(0)) curStatus = rdr.GetInt16(0);
                        if (!rdr.IsDBNull(1)) curRound  = rdr.GetInt16(1);
                        if (!rdr.IsDBNull(2)) curName   = rdr.GetString(2);
                        if (!rdr.IsDBNull(3)) curType   = rdr.GetByte(3);
                        curFinal = !rdr.IsDBNull(4) && rdr.GetBoolean(4);
                        if (!rdr.IsDBNull(5)) curCroNow  = rdr.GetString(5);
                        if (!rdr.IsDBNull(6)) curPauseTs = rdr.GetInt64(6);
                    }
                }

                // Persist the contract's real-time status as-is
                newStatus = (short)md.Status;

                oldStatus = curStatus;
                oldRound  = curRound;

                // Durable last-bank: keep last non-zero pause in DB
                long? snapPause = md.PauseTimestamp == 0 ? (long?)null : (long)md.PauseTimestamp;
                long? nextPause = snapPause ?? curPauseTs;

                // Update missions row from snapshot
                await using (var upsert = new NpgsqlCommand(@"
                    insert into missions (
                        mission_address,
                        name,
                        mission_type,
                        status,
                        enrollment_start,
                        enrollment_end,
                        enrollment_amount_wei,
                        enrollment_min_players,
                        enrollment_max_players,
                        mission_start,
                        mission_end,
                        mission_rounds_total,
                        round_count,
                        cro_initial_wei,
                        cro_start_wei,
                        cro_current_wei,
                        pause_timestamp,
                        updated_at,
                        mission_created,
                        round_pause_secs,
                        last_round_pause_secs,
                        creator_address,
                        all_refunded
                    )
                    values (
                        @a,
                        @nmIns,
                        @mt,
                        @st,
                        @es,
                        @ee,
                        @ea,
                        @emin,
                        @emax,
                        @ms,
                        @me,
                        @rt,
                        @rc,
                        @ci,
                        @cs,
                        @cc,
                        @pt,
                        now(),
                        @mc,
                        @rpd,
                        @lrpd,
                        @cr,
                        @ar
                    )
                    on conflict (mission_address) do update set
                        name                     = excluded.name,
                        mission_type             = excluded.mission_type,
                        status                   = excluded.status,
                        enrollment_start         = excluded.enrollment_start,
                        enrollment_end           = excluded.enrollment_end,
                        enrollment_amount_wei    = excluded.enrollment_amount_wei,
                        enrollment_min_players   = excluded.enrollment_min_players,
                        enrollment_max_players   = excluded.enrollment_max_players,
                        mission_start            = excluded.mission_start,
                        mission_end              = excluded.mission_end,
                        mission_rounds_total     = excluded.mission_rounds_total,
                        round_count              = excluded.round_count,
                        cro_initial_wei          = excluded.cro_initial_wei,
                        cro_start_wei            = excluded.cro_start_wei,
                        cro_current_wei          = excluded.cro_current_wei,
                        pause_timestamp          = excluded.pause_timestamp,
                        updated_at               = now(),
                        mission_created          = excluded.mission_created,
                        round_pause_secs         = excluded.round_pause_secs,
                        last_round_pause_secs    = excluded.last_round_pause_secs,
                        creator_address          = excluded.creator_address,
                        all_refunded             = excluded.all_refunded;", conn))
                {
                    upsert.Parameters.AddWithValue("a", mission);
                    upsert.Parameters.AddWithValue("nmIns", (object?)(md.Name ?? string.Empty));
                    upsert.Parameters.AddWithValue("mt", md.MissionType);
                    upsert.Parameters.AddWithValue("st", newStatus ?? (object)DBNull.Value);
                    upsert.Parameters.AddWithValue("es", md.EnrollmentStart);
                    upsert.Parameters.AddWithValue("ee", md.EnrollmentEnd);
                    upsert.Parameters.AddWithValue("ea", md.EnrollmentAmount);
                    upsert.Parameters.AddWithValue("emin", md.EnrollmentMinPlayers);
                    upsert.Parameters.AddWithValue("emax", md.EnrollmentMaxPlayers);
                    upsert.Parameters.AddWithValue("ms", md.MissionStart);
                    upsert.Parameters.AddWithValue("me", md.MissionEnd);
                    upsert.Parameters.AddWithValue("rt", (short)md.MissionRounds);
                    upsert.Parameters.AddWithValue("rc", (short)md.RoundCount);
                    upsert.Parameters.AddWithValue("ci", md.CroInitial);
                    upsert.Parameters.AddWithValue("cs", md.CroStart);
                    upsert.Parameters.AddWithValue("cc", md.CroCurrent);
                    upsert.Parameters.AddWithValue("pt", (object?)nextPause ?? DBNull.Value);
                    upsert.Parameters.AddWithValue("mc", md.MissionCreated);
                    upsert.Parameters.AddWithValue("rpd", md.RoundPauseDuration);
                    upsert.Parameters.AddWithValue("lrpd", md.LastRoundPauseDuration);
                    upsert.Parameters.AddWithValue("cr", (object?)md.Creator?.ToLowerInvariant() ?? DBNull.Value);
                    upsert.Parameters.AddWithValue("ar", md.AllRefunded);
                    await upsert.ExecuteNonQueryAsync(token);
                }

                // --- Sync players (full) from snapshot into players table ---
                var tuples = md.Players ?? new List<B6.Contracts.PlayerTuple>();

                // Current players in DB for this mission
                var existing = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                await using (var enRead = new NpgsqlCommand(
                    "select lower(player) from players where mission_address = @a;", conn))
                {
                    enRead.Parameters.AddWithValue("a", mission);
                    await using var enRdr = await enRead.ExecuteReaderAsync(token);
                    while (await enRdr.ReadAsync(token))
                        existing.Add(enRdr.GetString(0));
                }

                int membershipChanges = 0;

                foreach (var t in tuples)
                {
                    var addr = (t.Player ?? string.Empty).Trim().ToLowerInvariant();
                    if (addr.Length == 0) continue;

                    object DbTs(BigInteger ts) => ts == 0 ? (object)DBNull.Value : (long)ts; // uint256 epoch seconds → bigint
                    object DbWei(BigInteger v) => (object)v;                                 // BigInteger → NUMERIC (or BIGINT if you chose that)

                    await using var up = new NpgsqlCommand(@"
                        insert into players (
                            mission_address, player,
                            ""enrolledTS"", ""amountWon"", ""wonTS"", refunded, ""refundFailed"", ""refundTS""
                        ) values (
                            @a, @p,
                            @en, @aw, @wt, @rf, @rff, @rt
                        )
                        on conflict (mission_address, player) do update set
                            ""enrolledTS""   = excluded.""enrolledTS"",
                            ""amountWon""    = excluded.""amountWon"",
                            ""wonTS""        = excluded.""wonTS"",
                            refunded         = excluded.refunded,
                            ""refundFailed"" = excluded.""refundFailed"",
                            ""refundTS""     = excluded.""refundTS"";", conn);

                    up.Parameters.AddWithValue("a",  mission);
                    up.Parameters.AddWithValue("p",  addr);
                    up.Parameters.AddWithValue("en", DbTs(t.EnrolledTS));
                    up.Parameters.AddWithValue("aw", DbWei(t.AmountWon));
                    up.Parameters.AddWithValue("wt", DbTs(t.WonTS));
                    up.Parameters.AddWithValue("rf", t.Refunded);
                    up.Parameters.AddWithValue("rff", t.RefundFailed);
                    up.Parameters.AddWithValue("rt", DbTs(t.RefundTS));

                    await up.ExecuteNonQueryAsync(token);

                    if (!existing.Contains(addr)) membershipChanges++;
                }

                // Strict mirror: delete rows not present in snapshot
                var keep = tuples.Select(x => (x.Player ?? string.Empty).Trim().ToLowerInvariant())
                                .Where(x => x.Length > 0)
                                .Distinct()
                                .ToArray();

                await using (var del = new NpgsqlCommand(@"
                    delete from players
                    where mission_address = @a
                      and lower(player) <> all (@keep);", conn))
                {
                    del.Parameters.AddWithValue("a", mission);
                    del.Parameters.Add("keep", NpgsqlDbType.Array | NpgsqlDbType.Text).Value = keep;
                    var removed = await del.ExecuteNonQueryAsync(token);
                    membershipChanges += removed;
                }

                if (membershipChanges > 0)
                    changes.HasMeaningfulChange = true;

                try
                {
                    var dbCro   = (curCroNow ?? "").Trim();
                    var snapCro = md.CroCurrent.ToString();
                    if (!string.Equals(dbCro, snapCro, StringComparison.Ordinal))
                        changes.HasMeaningfulChange = true;

                    long? dbPause = curPauseTs;
                    if (dbPause != nextPause)
                        changes.HasMeaningfulChange = true;
                }
                catch { /* best effort */ }

                // Detect status transition
                if (oldStatus.HasValue && newStatus.HasValue && oldStatus.Value != newStatus.Value)
                {
                    changes.StatusTransition = (oldStatus.Value, newStatus.Value);
                    changes.HasMeaningfulChange = true;

                    await using var hist = new NpgsqlCommand(@"
                        insert into mission_status_history (mission_address, from_status, to_status, changed_at, block_number)
                        values (@a,@f,@t, now(), 0)
                        on conflict do nothing;", conn);
                    hist.Parameters.AddWithValue("a", mission);
                    hist.Parameters.AddWithValue("f", oldStatus.Value);
                    hist.Parameters.AddWithValue("t", newStatus.Value);
                    await hist.ExecuteNonQueryAsync(token);
                }

                // Detect round increment
                if (oldRound != newRound)
                {
                    var next = newRound ?? 0;

                    // Build ALL rounds from snapshot (distinct WonTS ↑), then upsert 1..next.
                    // This backfills if the writer was enabled mid-mission.
                    var winsAll = (md.Players ?? new List<B6.Contracts.PlayerTuple>())
                        .Where(p => p.WonTS != 0)
                        .GroupBy(p => (long)p.WonTS)
                        .OrderBy(g => g.Key)
                        .Select((g, i) =>
                        {
                            var w   = g.First();
                            var adr = (w.Player ?? string.Empty).Trim().ToLowerInvariant();
                            return (
                                Round:  (short)(i + 1),
                                Winner: string.IsNullOrWhiteSpace(adr) ? null : adr,
                                Amount: w.AmountWon,
                                Ts:     (long)w.WonTS
                            );
                        })
                        .ToList();

                    foreach (var win in winsAll.Where(w => w.Round <= next))
                    {
                        await using var upRound = new NpgsqlCommand(@"
                            insert into mission_rounds
                                (mission_address, round_number, winner_address, payout_wei, block_number, tx_hash, created_at)
                            values
                                (@a, @n, @w, @p, null, null, to_timestamp(@ts))
                            on conflict (mission_address, round_number) do update
                            set winner_address = excluded.winner_address,
                                payout_wei    = excluded.payout_wei,
                                created_at    = excluded.created_at;", conn);

                        upRound.Parameters.AddWithValue("a",  mission);
                        upRound.Parameters.AddWithValue("n",  win.Round);
                        upRound.Parameters.AddWithValue("w",  (object?)win.Winner ?? DBNull.Value);
                        upRound.Parameters.AddWithValue("p",  (object)win.Amount);
                        upRound.Parameters.AddWithValue("ts", win.Ts);
                        await upRound.ExecuteNonQueryAsync(token);
                    }

                    changes.NewRound = next;               // push latest only, like before
                    changes.HasMeaningfulChange = true;
                }

                // Mark finalized ONLY when ended AND (not Failed pending refunds).
                if (newStatus.HasValue && newStatus.Value > 5 && !curFinal)
                {
                    var endedAndRefundsDone = !(newStatus.Value == 7 && md.AllRefunded == false);
                    if (endedAndRefundsDone)
                    {
                        await using var fin = new NpgsqlCommand(@"
                            update missions set finalized = true, updated_at = now()
                            where mission_address = @a;", conn);
                        fin.Parameters.AddWithValue("a", mission);
                        await fin.ExecuteNonQueryAsync(token);
                    }
                }

                try
                {
                    // Watch while not finalized OR (Failed & refunds pending); keep time boundaries in sync
                    var mustWatch = !curFinal || (newStatus == 7 && md.AllRefunded == false);
                    if (mustWatch)
                    {
                        var esAt  = DateTimeOffset.FromUnixTimeSeconds((long)md.EnrollmentStart).UtcDateTime;
                        var eeAt  = DateTimeOffset.FromUnixTimeSeconds((long)md.EnrollmentEnd).UtcDateTime;
                        var msAt  = DateTimeOffset.FromUnixTimeSeconds((long)md.MissionStart).UtcDateTime;
                        var endAt = DateTimeOffset.FromUnixTimeSeconds((long)md.MissionEnd).UtcDateTime;

                        var w = _endWatch.GetOrAdd(mission, _ => new EndWatch { Mission = mission });
                        w.EnrollStartUtc = esAt;
                        w.EnrollEndUtc   = eeAt;
                        w.StartAtUtc     = msAt;
                        w.EndAtUtc       = endAt;
                        w.Finalized      = false;

                        // finalize attempts begin after mission_end
                        if (DateTime.UtcNow >= endAt && w.NextTryUtc < DateTime.UtcNow)
                            w.NextTryUtc = DateTime.UtcNow;
                        else if (DateTime.UtcNow < endAt)
                            w.NextTryUtc = endAt;

                        // Refunds: if Failed (7) & not all_refunded → try immediately; else, defer to mission_end
                        if (newStatus == 7 && md.AllRefunded == false)
                        {
                            w.RefundNextTryUtc = DateTime.UtcNow;
                        }
                        else
                        {
                            if (DateTime.UtcNow >= endAt && w.RefundNextTryUtc < DateTime.UtcNow)
                                w.RefundNextTryUtc = DateTime.UtcNow;
                            else if (DateTime.UtcNow < endAt)
                                w.RefundNextTryUtc = endAt;
                        }

                        // next time-based poll = nearest future of (es, ee, ms, me)
                        var futurePolls = new[] { esAt, eeAt, msAt, endAt }.Where(t => t > DateTime.UtcNow);
                        w.NextPollUtc = futurePolls.Any() ? futurePolls.Min() : DateTime.MinValue;
                    }
                    else
                    {
                        _endWatch.TryRemove(mission, out _);
                    }
                }
                catch { /* never block */ }
            }

            return changes;
        }

        private async Task                              LoadEndWatchAsync                   (CancellationToken token) {
            try
            {
                await using var conn = new NpgsqlConnection(_pg);
                await conn.OpenAsync(token);

                // Watch all missions that aren’t finalized yet (status < 5 means not terminal in your mapping)
                await using var cmd = new NpgsqlCommand(@"
                    select mission_address, enrollment_start, enrollment_end, mission_start, mission_end, coalesce(finalized,false)
                    from missions
                    where (coalesce(finalized,false) = false
                        or (status = 7 and coalesce(all_refunded,false) = false))
                    and (enrollment_start is not null or enrollment_end is not null or mission_start is not null or mission_end is not null);", conn);

                await using var rdr = await cmd.ExecuteReaderAsync(token);
                while (await rdr.ReadAsync(token))
                {
                    var addr = (rdr.IsDBNull(0) ? "" : rdr.GetString(0)).ToLowerInvariant();
                    if (string.IsNullOrWhiteSpace(addr)) continue;

                    var esSec = rdr.IsDBNull(1) ? 0L : rdr.GetInt64(1);
                    var eeSec = rdr.IsDBNull(2) ? 0L : rdr.GetInt64(2);
                    var msSec = rdr.IsDBNull(3) ? 0L : rdr.GetInt64(3);
                    var meSec = rdr.IsDBNull(4) ? 0L : rdr.GetInt64(4);

                    var esAt  = esSec == 0 ? DateTime.MinValue : DateTimeOffset.FromUnixTimeSeconds(esSec).UtcDateTime;
                    var eeAt  = eeSec == 0 ? DateTime.MinValue : DateTimeOffset.FromUnixTimeSeconds(eeSec).UtcDateTime;
                    var msAt  = msSec == 0 ? DateTime.MinValue : DateTimeOffset.FromUnixTimeSeconds(msSec).UtcDateTime;
                    var endAt = meSec == 0 ? DateTime.MinValue : DateTimeOffset.FromUnixTimeSeconds(meSec).UtcDateTime;

                    var w = _endWatch.GetOrAdd(addr, _ => new EndWatch { Mission = addr });
                    w.EnrollStartUtc = esAt;
                    w.EnrollEndUtc   = eeAt;
                    w.StartAtUtc     = msAt;
                    w.EndAtUtc       = endAt;
                    w.Finalized      = false;

                    // finalize attempts begin after mission_end
                    w.NextTryUtc = (DateTime.UtcNow >= endAt && endAt != DateTime.MinValue) ? DateTime.UtcNow : endAt;

                    // schedule first time-based poll at the nearest future of (es, ee, ms, me)
                    var futurePolls = new[] { esAt, eeAt, msAt, endAt }.Where(t => t > DateTime.UtcNow);
                    w.NextPollUtc = futurePolls.Any() ? futurePolls.Min() : DateTime.MinValue;
                }
                _log.LogInformation("End-watch seeded with {n} mission(s)", _endWatch.Count);
            }
            catch (Exception ex)
            {
                _log.LogWarning(ex, "LoadEndWatchAsync failed");
            }
        }

        private async Task<bool>                        AttemptFinalizeAsync                (string mission, CancellationToken token) {
            if (_finalizerAccount == null)
            {
                _log.LogWarning("Finalize requested for {mission} but no signer configured (Owner:PK). Skipping.", mission);
                return false;
            }

            if (!await ShouldForceFinalizeAsync(mission, token))
            {
                _endWatch.TryRemove(mission, out _); // no more attempts
                return false;
            }

            try
            {
                // 1) pre-flight: estimate gas to catch revert reasons early
                var txHash = await RunRpc(
                    async w => {
                        var handler = w.Eth.GetContractTransactionHandler<ForceFinalizeMissionFunction>();
                        await handler.EstimateGasAsync(mission, new ForceFinalizeMissionFunction());
                        // 2) send finalize() tx
                        return await handler.SendRequestAsync(mission, new ForceFinalizeMissionFunction());
                    },
                    "Tx.forceFinalizeMission");

                // 2) poll for receipt (older Nethereum: no TransactionReceiptService on Web3)
                var receipt = await RunRpc<TransactionReceipt?>(async w =>
                {
                    for (int i = 0; i < 60; i++)
                    {
                        var r = await w.Eth.Transactions.GetTransactionReceipt.SendRequestAsync(txHash);
                        if (r != null) return r;
                        await Task.Delay(TimeSpan.FromSeconds(1), token);
                    }
                    return (TransactionReceipt?)null;
                }, "Tx.waitFinalize");

                // success when status == 1
                var ok = receipt != null && receipt.Status != null && receipt.Status.Value == 1;
                _log.LogInformation("forceFinalizeMission() {mission} -> {status} (tx={tx})", mission, ok ? "OK" : "FAILED", txHash);
                return ok;
            }
            catch (Exception ex)
            {
                _log.LogDebug(ex, "forceFinalizeMission() attempt failed for {mission}", mission);
                return false;
            }
        }

        private async Task<bool>                        ShouldRefundAsync                   (string mission, CancellationToken token) {
            await using var conn = new NpgsqlConnection(_pg);
            await conn.OpenAsync(token);
            await using var cmd = new NpgsqlCommand(@"
                select status, coalesce(finalized,false), coalesce(all_refunded,false)
                from missions where mission_address = @a;", conn);
            cmd.Parameters.AddWithValue("a", mission);

            await using var rdr = await cmd.ExecuteReaderAsync(token);
            if (!await rdr.ReadAsync(token)) return false;

            var status      = rdr.IsDBNull(0) ? (short)99 : rdr.GetInt16(0);
            var finalized   = !rdr.IsDBNull(1) && rdr.GetBoolean(1);
            var allRefunded = !rdr.IsDBNull(2) && rdr.GetBoolean(2);

            if (finalized) return false;
            if (status != 7) return false;
            if (allRefunded) return false;

            // No time gating: Failed (7) + !all_refunded → refund now
            return true;
        }

        private async Task<bool>                        ShouldRefundAsync                   (string mission, CancellationToken token) {
            await using var conn = new NpgsqlConnection(_pg);
            await conn.OpenAsync(token);
            await using var cmd = new NpgsqlCommand(@"
                select status, mission_end, coalesce(finalized,false), coalesce(all_refunded,false)
                from missions where mission_address = @a;", conn);
            cmd.Parameters.AddWithValue("a", mission);

            await using var rdr = await cmd.ExecuteReaderAsync(token);
            if (!await rdr.ReadAsync(token)) return false;

            var status      = rdr.IsDBNull(0) ? (short)99 : rdr.GetInt16(0);
            var endSec      = rdr.IsDBNull(1) ? 0L : rdr.GetInt64(1);
            var finalized   = !rdr.IsDBNull(2) && rdr.GetBoolean(2);
            var allRefunded = !rdr.IsDBNull(3) && rdr.GetBoolean(3);

            if (finalized) return false;
            if (status != 7) return false;
            if (allRefunded) return false;

            var endAt = DateTimeOffset.FromUnixTimeSeconds(endSec).UtcDateTime;
            return DateTime.UtcNow >= endAt;
        }

        private async Task<bool>                        AttemptRefundAsync                  (string mission, CancellationToken token) {
            if (_finalizerAccount == null)
            {
                _log.LogWarning("Refund requested for {mission} but no signer configured (Owner:PK). Skipping.", mission);
                return false;
            }

            if (!await ShouldRefundAsync(mission, token))
                return false;

            try
            {
                var txHash = await RunRpc(
                    async w => {
                        var handler = w.Eth.GetContractTransactionHandler<RefundPlayersFunction>();
                        await handler.EstimateGasAsync(mission, new RefundPlayersFunction());
                        return await handler.SendRequestAsync(mission, new RefundPlayersFunction());
                    },
                    "Tx.refundPlayers");

                var receipt = await RunRpc<TransactionReceipt?>(async w =>
                {
                    for (int i = 0; i < 60; i++)
                    {
                        var r = await w.Eth.Transactions.GetTransactionReceipt.SendRequestAsync(txHash);
                        if (r != null) return r;
                        await Task.Delay(TimeSpan.FromSeconds(1), token);
                    }
                    return (TransactionReceipt?)null;
                }, "Tx.waitRefund");

                var ok = receipt != null && receipt.Status != null && receipt.Status.Value == 1;
                _log.LogInformation("refundPlayers() {mission} -> {status} (tx={tx})", mission, ok ? "OK" : "FAILED", txHash);
                return ok;
            }
            catch (Exception ex)
            {
                _log.LogDebug(ex, "refundPlayers() attempt failed for {mission}", mission);
                return false;
            }
        }

        private async Task                              SeedRefundWatchAsync                (CancellationToken token) {
            try
            {
                await using var conn = new NpgsqlConnection(_pg);
                await conn.OpenAsync(token);

                await using var cmd = new NpgsqlCommand(@"
                    select mission_address, mission_end
                    from missions
                    where coalesce(finalized,false) = false
                    and status = 7
                    and coalesce(all_refunded,false) = false
                    limit 200;", conn);

                await using var rdr = await cmd.ExecuteReaderAsync(token);
                var now = DateTime.UtcNow;

                while (await rdr.ReadAsync(token))
                {
                    var addr = (rdr.IsDBNull(0) ? "" : rdr.GetString(0)).ToLowerInvariant();
                    if (string.IsNullOrWhiteSpace(addr)) continue;

                    var meSec = rdr.IsDBNull(1) ? 0L : rdr.GetInt64(1);
                    var endAt = meSec == 0 ? DateTime.MinValue : DateTimeOffset.FromUnixTimeSeconds(meSec).UtcDateTime;

                    var w = _endWatch.GetOrAdd(addr, _ => new EndWatch { Mission = addr });
                    w.EndAtUtc = endAt;

                    // schedule refund attempts (now if ended; else at end)
                    w.RefundNextTryUtc = now;

                    // keep finalize/backoff fields untouched; refunds path is independent
                }
            }
            catch
            {
                // best effort only
            }
        }

        private async Task                              SaveFactoryLastSeqAsync             (ulong seq, CancellationToken ct) {
            try
            {
                await using var conn = new Npgsql.NpgsqlConnection(_pg);
                await conn.OpenAsync(ct);
                await using var cmd = new Npgsql.NpgsqlCommand(@"
                    insert into indexer_factory_cursor (id, last_seq, updated_at)
                    values (1, @s, now())
                    on conflict (id) do update
                    set last_seq  = greatest(indexer_factory_cursor.last_seq, @s),
                        updated_at = now();", conn);

                var p = cmd.Parameters.Add("s", NpgsqlTypes.NpgsqlDbType.Bigint);
                p.Value = (long)(seq > long.MaxValue ? long.MaxValue : seq);

                await cmd.ExecuteNonQueryAsync(ct);
            }
            catch
            {
                // If table missing, we’ll persist on next call after migration is applied
            }
        }

        private async Task                              ProcessKickQueueAsync               (CancellationToken token) {
            var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            while (_kickMissions.TryDequeue(out var mission))
            {
                if (string.IsNullOrWhiteSpace(mission)) continue;
                if (!seen.Add(mission)) continue;

                try
                {
                    // Try a few times so the chain state (round/pool) is visible after bank.
                    const int maxAttempts = 3;
                    for (int attempt = 0; attempt < maxAttempts; attempt++)
                    {
                        await RefreshMissionSnapshotAsync(mission, token);
                        if (attempt < maxAttempts - 1)
                        {
                            try { await Task.Delay(TimeSpan.FromSeconds(1), token); } catch { }
                        }
                    }

                    await NotifyMissionUpdatedAsync(mission, token);
                }
                catch (Exception ex)
                {
                    _log.LogDebug(ex, "Kick refresh failed for {mission}", mission);
                }
            }
        }

        private async Task                              NotifyMissionUpdatedAsync           (string mission, CancellationToken ct = default) {
            if (string.IsNullOrEmpty(_pushBase) || string.IsNullOrEmpty(_pushKey)) return;
            using var req = new HttpRequestMessage(HttpMethod.Post, $"{_pushBase.TrimEnd('/')}/push/mission");
            req.Content = JsonContent.Create(new { Mission = mission });
            req.Headers.Add("X-Push-Key", _pushKey);
            try
            {
                var resp = await _http.SendAsync(req, ct);
                _log.LogInformation("push/mission {mission} -> {code}", mission, (int)resp.StatusCode);
            }
            catch (Exception ex)
            {
                _log.LogWarning(ex, "push/mission failed for {mission}", mission);
            }
        }

        private async Task                              NotifyStatusAsync                   (string mission, short newStatus, CancellationToken ct = default) {
            if (string.IsNullOrEmpty(_pushBase) || string.IsNullOrEmpty(_pushKey)) return;
            using var req = new HttpRequestMessage(HttpMethod.Post, $"{_pushBase.TrimEnd('/')}/push/status")
            {
                Content = JsonContent.Create(new { Mission = mission, NewStatus = newStatus })
            };
            req.Headers.Add("X-Push-Key", _pushKey);
            try { await _http.SendAsync(req, ct); }
            catch (Exception ex) { _log.LogDebug(ex, "push/status failed for {mission}", mission); }
        }

        private async Task                              NotifyRoundAsync                    (string mission, short round, string winner, string amountWei, CancellationToken ct = default) {
            if (string.IsNullOrEmpty(_pushBase) || string.IsNullOrEmpty(_pushKey)) return;
            using var req = new HttpRequestMessage(HttpMethod.Post, $"{_pushBase.TrimEnd('/')}/push/round")
            {
                Content = JsonContent.Create(new { Mission = mission, Round = round, Winner = winner, AmountWei = amountWei })
            };
            req.Headers.Add("X-Push-Key", _pushKey);
            try { await _http.SendAsync(req, ct); }
            catch (Exception ex) { _log.LogDebug(ex, "push/round failed for {mission} r{round}", mission, round); }
        }

        private async Task<T>                           RunRpc<T>                           (Func<Web3, Task<T>> fn, string context, [CallerMemberName] string caller = "") {
            var kind = NormalizeKind(context);

            // Count this attempt (counts retries too, which reflects real request volume)
            NoteRpc(context, caller);

            var sw = Stopwatch.StartNew();
            try
            {
                var res = await fn(_web3);
                sw.Stop();
                if (logRpcCalls) RpcFileLog(kind, $"{context} ✓ in {sw.ElapsedMilliseconds} ms");
                return res;
            }
            catch (Exception ex) when (IsTransient(ex))
            {
                // Count the retry attempt after we decide to retry (by calling NoteRpc again)
                if (IsRateLimited(ex))
                {
                    _log.LogWarning(ex, "{ctx} hit rate limit (429); cooling down {sec}s", context, (int)RATE_LIMIT_COOLDOWN.TotalSeconds);
                    try { await Task.Delay(RATE_LIMIT_COOLDOWN); } catch { }
                }
                else if (TryGetBenignProviderCode(ex, out var code))
                {
                    NoteBenign(kind, code);
                    try { await Task.Delay(_benignRetryDelay); } catch { }
                    try
                    {
                        // retry on same endpoint → new attempt
                        NoteRpc(context, caller);
                        var res0 = await fn(_web3);
                        if (logRpcCalls) RpcFileLog(kind, $"{context} ✓ after benign {code} in {sw.ElapsedMilliseconds} ms");
                        return res0;
                    }
                    catch (Exception ex2) when (IsTransient(ex2))
                    {
                        ex = ex2; // fall through
                    }
                }

                if (logRpcCalls) RpcFileLog(kind, $"{context} ↻ transient: {ex.GetType().Name}: {ex.Message} (attempting switch)");

                var switched = false;
                try { switched = SwitchRpc(); }
                catch (Exception sx) { _log.LogWarning(sx, "SwitchRpc() failed while handling transient error for {ctx}", context); }

                if (!switched) throw;

                _log.LogWarning(ex, "{ctx} failed; switched RPC and retrying", context);

                // retry after switch → new attempt
                NoteRpc(context, caller);
                var sw2 = Stopwatch.StartNew();
                var res2 = await fn(_web3);
                sw2.Stop();
                if (logRpcCalls) RpcFileLog(kind, $"{context} ✓ in {sw2.ElapsedMilliseconds} ms (after switch)");
                return res2;
            }
            catch (Exception ex)
            {
                sw.Stop();
                if (logRpcCalls) RpcFileLog(kind, $"{context} ✗ in {sw.ElapsedMilliseconds} ms: {ex.GetType().Name}: {ex.Message}");
                throw;
            }
        }

        private void                                    UseRpc                              (int idx) {
            _rpcIndex = idx % _rpcEndpoints.Count;
            var url = _rpcEndpoints[_rpcIndex];
            // NEW: use signer when available so we can send finalize()
            _web3 = _finalizerAccount != null ? new Web3(_finalizerAccount, url)
                                            : new Web3(url);
            //_log.LogInformation("Using RPC[{idx}]: {url}", _rpcIndex, url);
        }

        private bool                                    SwitchRpc                           () {
            if (_rpcEndpoints.Count <= 1) return false;
            var next = (_rpcIndex + 1) % _rpcEndpoints.Count;
            if (next == _rpcIndex) return false;
            var oldUrl = _rpcEndpoints[_rpcIndex];
            UseRpc(next);
            _log.LogWarning("Switched RPC from {old} to {nu}", oldUrl, _rpcEndpoints[_rpcIndex]);
            return true;
        }
    
        private void                                    FlushRpcSummaryIfDue                () {
            var now = DateTime.UtcNow;
            if (_nextRpcSummaryUtc == DateTime.MinValue) _nextRpcSummaryUtc = now + _rpcSummaryPeriod;
            if (now < _nextRpcSummaryUtc) return;

            Dictionary<string,int> byCtx;
            Dictionary<string, Dictionary<string,int>> byCaller;
            int total = 0;

            lock (_rpcLogLock)
            {
                // Snapshot & reset
                byCtx    = new Dictionary<string,int>(_rpc5mByContext, StringComparer.InvariantCulture);
                byCaller = new Dictionary<string, Dictionary<string,int>>(StringComparer.InvariantCulture);
                foreach (var kv in _rpc1hByCaller)
                    byCaller[kv.Key] = new Dictionary<string,int>(kv.Value, StringComparer.InvariantCulture);

                _rpc5mByContext.Clear();
                _rpc1hByCaller.Clear();

                _nextRpcSummaryUtc = now + _rpcSummaryPeriod;
            }

            foreach (var n in byCtx.Values) total += n;

                // Build pretty multi-line message for Event Viewer
                var nl = Environment.NewLine;

                string ctxPretty = (byCtx.Count == 0)
                    ? "  - none"
                    : string.Join(nl, byCtx.OrderByDescending(kv => kv.Value)
                                        .Select(kv => $"  - {kv.Key}={kv.Value}"));

                string callerPretty = (byCaller.Count == 0)
                    ? "  - none"
                    : string.Join(nl, byCaller.OrderByDescending(kv => kv.Value.Values.Sum())
                                            .Select(kv =>
                                                $"  - {kv.Key}:{nl}" +
                                                string.Join(nl, kv.Value.OrderByDescending(x => x.Value)
                                                                        .Select(x => $"    - {x.Key}={x.Value}"))));

                string header = _firstRPCSummary ? "RPC Summary (first minute)" : "RPC Summary (last hour)";
                _firstRPCSummary = false;

                string msg =
                    $"{header} total={total}{nl}" +
                    $"ByContext:{nl}{ctxPretty}{nl}{nl}" +
                    $"ByCaller:{nl}{callerPretty}";

                _log.LogInformation(new EventId(9001, "RpcSummary"), "{msg}", msg);

        }

        private void                                    NoteRpc                             (string context, string caller) {
            var ctx = string.IsNullOrWhiteSpace(context) ? "RPC" : context;
            var who = string.IsNullOrWhiteSpace(caller)  ? "Unknown" : caller;

            lock (_rpcLogLock)
            {
                _rpc5mByContext[ctx] = _rpc5mByContext.TryGetValue(ctx, out var n) ? n + 1 : 1;

                if (!_rpc1hByCaller.TryGetValue(who, out var map))
                {
                    map = new Dictionary<string,int>(StringComparer.InvariantCulture);
                    _rpc1hByCaller[who] = map;
                }
                map[ctx] = map.TryGetValue(ctx, out var c) ? c + 1 : 1;
            }
        }

        private void                                    RpcFileLog                          (string kind, string line) {
            try
            {
                var nowUtc = DateTime.UtcNow;
                var dayUtc = nowUtc.Date;

                lock (_rpcLogLock)
                {
                    // rotate per UTC day
                    if (dayUtc != _rpcLogDay)
                    {
                        _rpcLogDay  = dayUtc;
                        _rpcCounts.Clear();
                        var dir = Path.Combine(AppContext.BaseDirectory, "logs");
                        Directory.CreateDirectory(dir);
                        _rpcLogPath = Path.Combine(dir, $"rpc-{dayUtc:yyyyMMdd}.log");
                        File.AppendAllText(_rpcLogPath, $"===== NEW DAY {dayUtc:yyyy-MM-dd} UTC ====={Environment.NewLine}");
                    }

                    var next = _rpcCounts.TryGetValue(kind, out var n) ? n + 1 : 1;
                    _rpcCounts[kind] = next;

                    var ts = nowUtc.ToString("yyyy-MM-dd HH:mm:ss.fff", CultureInfo.InvariantCulture);
                    File.AppendAllText(_rpcLogPath, $"{ts} [{kind}] #{next} {line}{Environment.NewLine}");
                }
            }
            catch
            {
                // Best-effort logging: never let IO errors affect the caller.
            }
        }

        private static bool                             TryGetBenignProviderCode            (Exception ex, out string code) {
            code = string.Empty;
            if (ex == null) return false;
            var m = (ex.Message ?? string.Empty).ToLowerInvariant();

            // map a few frequent upstream issues
            if (m.Contains("502") || m.Contains("bad gateway"))           { code = "502-BadGateway";      return true; }
            if (m.Contains("503") || m.Contains("service unavailable"))   { code = "503-Unavailable";     return true; }
            if (m.Contains("504") || m.Contains("gateway timeout"))       { code = "504-GatewayTimeout";  return true; }
            if (m.Contains("408") || m.Contains("request timeout"))       { code = "408-Timeout";         return true; }
            if (m.Contains("410") || m.Contains("gone"))                  { code = "410-Gone";            return true; }

            // bubble down to inner exception text if any
            return ex.InnerException != null && TryGetBenignProviderCode(ex.InnerException, out code);
        }

        private void                                    NoteBenign                          (string kind, string code){
            try
            {
                string key;
                var today = DateTime.UtcNow.Date;

                lock (_rpcLogLock)
                {
                    if (_benignDayUtc.Date != today && _benignCounts.Count > 0)
                    {
                        var y = _benignDayUtc.Date.ToString("yyyy-MM-dd");
                        var summary = string.Join(", ", _benignCounts.Select(kv => $"{kv.Key}={kv.Value}"));
                        _log.LogInformation("RPC benign error rollup {day}: {summary}", y, summary);
                        _benignCounts.Clear();
                    }
                    _benignDayUtc = DateTime.UtcNow;

                    key = $"{kind}.{code}";
                    _benignCounts[key] = _benignCounts.TryGetValue(key, out var n) ? n + 1 : 1;
                }

                // Fire-and-forget DB upsert; never block the caller.
                _ = Task.Run(async () =>
                {
                    try
                    {
                        await using var conn = new NpgsqlConnection(_pg);
                        await conn.OpenAsync();

                        await using var up = new NpgsqlCommand(@"
                            insert into indexer_benign_errors (day, err_key, count, updated_at)
                            values (@d, @k, 1, now())
                            on conflict (day, err_key) do update
                            set count = indexer_benign_errors.count + 1,
                                updated_at = now();", conn);

                        up.Parameters.AddWithValue("d", today);
                        up.Parameters.AddWithValue("k", key);
                        await up.ExecuteNonQueryAsync();
                    }
                    catch { /* best-effort only */ }
                });
            }
            catch { /* never block caller */ }
        }

        private static string                           NormalizeKind                       (string context) {
            if (string.IsNullOrWhiteSpace(context)) return "RPC";
            var p = context.IndexOf('(');
            return p > 0 ? context.Substring(0, p) : context;
        }

        private static bool                             IsTransient                         (Exception ex) {
            return ex is Nethereum.JsonRpc.Client.RpcResponseException
                || ex is System.Net.Http.HttpRequestException
                || ex is TaskCanceledException
                || (ex.InnerException != null && IsTransient(ex.InnerException));
        }

        private static bool                             IsRateLimited                       (Exception ex) {
            if (ex == null) return false;
            var msg = ex.Message ?? string.Empty;
            if (msg.IndexOf("429", StringComparison.OrdinalIgnoreCase) >= 0) return true;
            if (msg.IndexOf("Too Many Requests", StringComparison.OrdinalIgnoreCase) >= 0) return true;
            return ex.InnerException != null && IsRateLimited(ex.InnerException);
        }

    }
}

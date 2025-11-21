// #region using directives

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
// #endregion

namespace B6.Indexer
{

// #region architecture comment MissionIndexer 
    /// <summary>
    /// Mission indexer for B6 missions.
    ///
    /// Responsibilities:
    /// - Runs a single time-based core loop once per second (coreLoop) that:
    ///   - Processes frontend "kicks" (JOIN/BANK success notifications) via a kick queue.
    ///   - Periodically polls the MissionFactory (getChangesAfter) to discover new or
    ///     changed missions and refreshes their on-chain snapshots into Postgres.
    ///   - Loads all non-ended missions (status &lt; Success) from the missions table
    ///     and dispatches them to time-based handlers.
    ///
    /// Time-based mission handling:
    /// - For each open mission, coreLoop invokes a set of phase handlers that use only
    ///   mission timestamps and round metadata from the database:
    ///     - processStartPending      : placeholder for "mission created" (currently no-op).
    ///     - processStartEnrollment   : placeholder for enrollmentStart (currently no-op).
    ///     - processEndEnrollment     : within [enrollmentEnd, enrollmentEnd+30s], polls
    ///                                  getMissionData once per second until status becomes
    ///                                  Arming or Failed. On Failed, calls refundPlayers()
    ///                                  and refreshes the snapshot.
    ///     - processMissionStart      : placeholder around missionStart (currently no-op).
    ///     - processStartCooldown     : detects the start of a cooldown based on
    ///                                  pause_timestamp and sends a one-time push so the
    ///                                  frontend can show "Cooldown X minutes".
    ///     - processEndCooldown       : computes the cooldown end from pause_timestamp and
    ///                                  roundPauseSecs/lastRoundPauseSecs; within a 30s
    ///                                  window, polls getMissionData until status becomes
    ///                                  Active again, then notifies the frontend.
    ///     - processMissionEnd        : within [missionEnd, missionEnd+30s], polls
    ///                                  getMissionData and reacts to terminal statuses:
    ///                                  - Failed        → AttemptRefundAsync (refundPlayers),
    ///                                                     then refresh snapshot + push.
    ///                                  - PartlySuccess → AttemptFinalizeAsync (factory
    ///                                                     finalize), then refresh + push.
    ///                                  - Success       → explicit push so the frontend
    ///                                                     always sees the final result.
    ///
    /// Data flow:
    /// - Snapshots from getMissionData are applied via ApplySnapshotToDatabaseAsync,
    ///   which upserts missions, players and rounds, maintains mission_status_history
    ///   and derives mission status transitions and new rounds.
    /// - Derived changes are propagated to the frontend via HTTP push endpoints:
    ///   /push/mission, /push/status and /push/round.
    ///
    /// Chain interaction:
    /// - All on-chain reads/writes go through RunRpc, which centralizes logging,
    ///   retry logic, benign error rollup and endpoint switching.
    /// - Write operations (forceFinalizeMission, refundPlayers) are performed using
    ///   an optional signer account configured via Owner:PK. When not configured,
    ///   these actions are skipped but the indexer continues to read and push state.
    /// </summary>
// #endregion


    public class MissionIndexer : BackgroundService
    {

// #region fields
        // Logger on/off ------------------------------------------------------------------------------------------------
        private static readonly bool                    logRpcCalls         = false;                                        // ← toggle RPC file logging
        // --------------------------------------------------------------------------------------------------------------
        private readonly ILogger<MissionIndexer>        _log;                                                               // injected
        private readonly string                         _rpc;                                                               // primary RPC endpoint
        private readonly string                         _rpc2;                                                              // secondary RPC endpoint
        private readonly string                         _factory;                                                           // MissionFactory contract address              
        private readonly string                         _pg;                                                                // Postgres connection string                                  
        private readonly string                         _pushBase;                                                          // e.g. https://b6missions.com/api
        private readonly string                         _pushKey;                                                           // e.g. secret key for push auth
        private readonly string                         _ownerPk            = string.Empty;
        private Account?                                _finalizerAccount;
        private Web3                                    _web3               = default!;                                     // current RPC client
        private readonly List<string>                   _rpcEndpoints       = [];                                           // pool of RPC endpoints
        private int                                     _rpcIndex           = 0;                                            // current RPC endpoint index
        private readonly HttpClient                     _http               = new();                                        // for push notifications
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

        /// <summary>
        /// Per-mission runtime state used by the time-based core loop to avoid
        /// duplicate work (e.g. only push cooldown-start once per pauseTimestamp).
        /// </summary>
        private readonly ConcurrentDictionary<string, MissionRuntimeState> _runtimeState = new ConcurrentDictionary<string, MissionRuntimeState>(StringComparer.OrdinalIgnoreCase);

        // Core loop configuration --------------------------------------------------------------------------------------
        /// <summary>
        /// Interval in seconds between factory polls for new/changed missions.
        /// </summary>
        private const int                               newMissionsPollFreq = 60;

        // Core loop state ----------------------------------------------------------------------------------------------
        /// <summary>
        /// Simple counter incremented once per second in coreLoop(). When it reaches
        /// newMissionsPollFreq, checkForNewMissions() is called and the counter is reset.
        /// </summary>
        private int                                     _newMissionsPollCounter = 0;

        private volatile bool                           _kickRequested      = false;                                        // set by listener
        private readonly ConcurrentQueue<KickMission>   _kickMissions       = new();                                        // mission addresses to process kicks for 
        private ulong                                   _factoryLastSeq     = 0;

        /// <summary>
        /// Local status enum used throughout the indexer for readability. This mirrors the on-chain Status enum.
        /// </summary>
        private enum Status : short {
            Pending       = 0, // Mission is created but not yet enrolling
            Enrolling     = 1, // Mission is open for enrollment, waiting for players to join
            Arming        = 2, // Mission is armed and ready to start
            Active        = 3, // Mission is currently active and players can participate
            Paused        = 4, // Mission is paused, no further actions can be taken
            PartlySuccess = 5, // Mission ended with some players winning, but not all rounds were claimed
            Success       = 6, // Mission ended successfully, all rounds were claimed
            Failed        = 7  // Mission failed (not enough players enrolled or nobody banked)
        }

        // Helper conversion methods to keep status handling readable and consistent.
        private static short ToDbStatus(Status status) => (short)status;

        private static Status ToStatus(short value) => (Status)value;

        private static Status? ToStatus(short? value)
            => value.HasValue ? (Status?)ToStatus(value.Value) : null;

        enum IdxEvt { 
 
                                                        RpcSummary          =  9001,
                                                        PushMission         =  9002, 
                                                        UseRpc              =  9003,
                                                        RpcBenignError      =  9004,
                                                        RefundEligible      = 21001, 
                                                        RefundSkip          = 21002,
                                                        RefundSkipped       = 21003,
                                                        RefundPlayers       = 21004,
                                                        ForceFinalizeMission= 31001,
        }
// #endregion

// #region nested types
        /// <summary>
        /// In-memory representation of a mission schedule used by the time-based core loop.
        /// All timestamps are stored in UTC for easy comparison with DateTime.UtcNow.
        /// </summary>
        private sealed class MissionSchedule {
            public string   Address               { get; init; } = string.Empty;
            public Status   Status                { get; init; }

            public DateTime MissionCreatedUtc     { get; init; } = DateTime.MinValue;
            public DateTime EnrollmentStartUtc    { get; init; } = DateTime.MinValue;
            public DateTime EnrollmentEndUtc      { get; init; } = DateTime.MinValue;
            public DateTime MissionStartUtc       { get; init; } = DateTime.MinValue;
            public DateTime MissionEndUtc         { get; init; } = DateTime.MinValue;

            /// <summary>
            /// Raw pause timestamp in epoch seconds as stored in the missions table (can be null).
            /// </summary>
            public long?    PauseTimestampSeconds { get; init; }

            /// <summary>
            /// Standard per-round cooldown duration in seconds.
            /// </summary>
            public int      RoundPauseSecs        { get; init; }

            /// <summary>
            /// Cooldown duration for the last round in seconds.
            /// </summary>
            public int      LastRoundPauseSecs    { get; init; }

            /// <summary>
            /// Total number of rounds configured for the mission (mission_rounds_total).
            /// </summary>
            public int      MissionRoundsTotal    { get; init; }

            /// <summary>
            /// Number of rounds that have already been played/claimed (round_count).
            /// </summary>
            public int      RoundCount            { get; init; }
        }

        /// <summary>
        /// In-memory runtime flags for missions used by the time-based scheduler.
        /// This state is not persisted in the database and is only used to avoid
        /// duplicate pushes or repeated work across coreLoop ticks.
        /// </summary>
        private sealed class MissionRuntimeState {
            /// <summary>
            /// The last pause_timestamp (epoch seconds) for which the cooldown-start
            /// notification was already sent to the frontend. When a new cooldown
            /// begins with a different pause_timestamp, the handler will push again.
            /// </summary>
            public long? LastCooldownStartPauseTimestamp { get; set; }
        }

        /// <summary>
        /// Small DTO for queued kick missions, including the originating tx hash (if any)
        /// and the optional frontend event type (Created/Enrolled/Banked/Finalized/...).
        /// </summary>
        private sealed class KickMission {
            public string  Mission   { get; init; } = string.Empty;
            public string? TxHash    { get; init; }
            public string? EventType { get; init; }
        }

        // --------------------------------------------------------------------------------------------------------------

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
// #endregion


// #region Lifecycle

        /// <summary>
        /// Creates a new MissionIndexer instance, wiring logging, configuration,
        /// RPC endpoints and the optional signer account used for finalize/refund
        /// transactions.
        /// </summary>
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
            _rpc2    = cfg["Cronos:Rpc2"] 
                    ?? throw new InvalidOperationException("Missing configuration key: Cronos:Rpc2");
            _factory = cfg["Contracts:Factory"] 
                    ?? throw new InvalidOperationException("Missing configuration key: Contracts:Factory");
            _pg      = cfg.GetConnectionString("Db") 
                    ?? throw new InvalidOperationException("Missing connection string: Db");
            _pushBase = cfg["Push:BaseUrl"] 
                    ?? throw new InvalidOperationException("Missing configuration key: Push:BaseUrl");
            _pushKey  = cfg["Push:Key"]     
                    ?? throw new InvalidOperationException("Missing configuration key: Push:Key");
            _ownerPk  = cfg["Owner:PK"]     
                    ?? throw new InvalidOperationException("Missing configuration key: Owner:PK");

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
            if (!string.IsNullOrWhiteSpace(_rpc2)) _rpcEndpoints.Add(_rpc2);

            if (_rpcEndpoints.Count == 0)
                throw new InvalidOperationException("No RPC endpoints configured (Cronos:Rpc and/or Cronos:Rpc2).");

            // start with first endpoint
            UseRpc(0);

            _nextRpcSummaryUtc = DateTime.UtcNow.AddMinutes(1);

        }

        /// <summary>
        /// Main background entry point. Starts the kick listener, initializes the
        /// factory cursor and then runs the one-second core loop until cancellation.
        /// </summary>
        protected override async Task                   ExecuteAsync                        (CancellationToken token) {
            // Time-based indexer with a single second-level core loop.
            try
            {
                // Keep kick listener (DB NOTIFY + fallback queue) running in the background.
                _ = Task.Run(() => ListenForKicksAsync(token), token);

                // Load factory change cursor (lastSeq = 0 if table/row missing).
                _factoryLastSeq = await LoadFactoryLastSeqAsync(token);
            }
            catch (Exception ex)
            {
                _log.LogError(ex, "Scheduler bootstrap failed");
            }

            while (!token.IsCancellationRequested)
            {
                try
                {
                    // Single second-level heartbeat that orchestrates all time-based work.
                    await coreLoop(token);
                }
                catch (OperationCanceledException) when (token.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _log.LogError(ex, "Core loop failed");
                }

                FlushRpcSummaryIfDue();

                // One-second cadence for the core loop.
                try
                {
                    await Task.Delay(TimeSpan.FromSeconds(1), token);
                }
                catch (OperationCanceledException) when (token.IsCancellationRequested)
                {
                    break;
                }
            }
        }

// #endregion

// #region Helpers

        /// <summary>
        /// Converts a Unix timestamp in seconds (BigInteger) to a UTC DateTime.
        /// </summary>
        private static DateTime                         FromUnix                            (BigInteger ts) => FromUnix((long)ts);

        /// <summary>
        /// Converts a Unix timestamp in seconds (long) to a UTC DateTime.
        /// </summary>
        private static DateTime                         FromUnix                            (long ts)       => DateTimeOffset.FromUnixTimeSeconds(ts).UtcDateTime;

        /// <summary>
        /// Loads the last processed factory change sequence from the
        /// indexer_factory_cursor table. Returns 0 on cold start or when
        /// the row/table does not exist yet.
        /// </summary>
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


        /// <summary>
        /// Persists the last processed factory sequence into the
        /// indexer_factory_cursor table, using greatest() to avoid going
        /// backwards when races occur.
        /// </summary>
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

        /// <summary>
        /// Long-running LISTEN loop on the b6_indexer_kick channel. Incoming
        /// NOTIFY payloads are enqueued into the in-memory kick queue for
        /// processing by the core loop.
        /// </summary>
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
                        // We no longer rely on the NOTIFY payload for the mission address;
                        // instead we read from the indexer_kicks table so we also get tx_hash.
                        _kickRequested = true;

                        // Sweep pending kick rows into the in-memory queue.
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

        /// <summary>
        /// Reads the on-chain mission snapshot via getMissionData and applies it
        /// to the database using ApplySnapshotToDatabaseAsync. Triggers mission,
        /// status and round push notifications when meaningful changes are detected.
        /// </summary>
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
                // Generic snapshot-driven change (factory poll, time-based refresh, etc.)
                // Marked as CoreLoop so the frontend can distinguish from kick-based updates.
                await NotifyMissionUpdatedAsync(mission, reason: "SnapshotChanged.CoreLoop", txHash: null, ct: token);

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

        /// <summary>
        /// Applies a full mission snapshot to the missions and players tables,
        /// updating status, rounds, pool amounts and history. Returns a set of
        /// derived changes that the caller can use to decide which push events
        /// to emit.
        /// </summary>
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

                // Persist the contract's real-time status as-is (with anti-regression guard)
                var snapped = (short)md.Status;

                // Once terminal (>5), never go backwards; after mission_end, never go below 5.
                if (curStatus.HasValue)
                {
                    var endAtUtc = FromUnix((long)md.MissionEnd);

                    // If DB already says Success/Failed, keep it (ignore any lower snapshot from a lagging RPC).
                    if (curStatus.Value > ToDbStatus(Status.PartlySuccess) && snapped < curStatus.Value)
                    {
                        snapped = curStatus.Value;
                    }
                    // After mission_end, do not regress to non-ended (<5) states.
                    else if (DateTime.UtcNow >= endAtUtc && snapped < 5)
                    {
                        snapped = curStatus.Value;
                    }
                }
                newStatus = snapped;

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

                    // Derive current pool from start − sum(payouts so far) to avoid the "last win still included" blip
                    try
                    {
                        var paidTotal = winsAll.Where(w => w.Round <= next)
                                            .Aggregate(System.Numerics.BigInteger.Zero, (s, w) => s + w.Amount);
                        var derived   = md.CroStart - paidTotal;
                        if (derived < 0) derived = System.Numerics.BigInteger.Zero;

                        await using var updPool = new NpgsqlCommand(@"
                            update missions
                            set cro_current_wei = @cc, updated_at = now()
                            where mission_address = @a;", conn);
                        updPool.Parameters.AddWithValue("a",  mission);
                        updPool.Parameters.AddWithValue("cc", (object)derived);
                        await updPool.ExecuteNonQueryAsync(token);
                    }
                    catch { /* best effort: never block round write */ }

                    changes.NewRound = next;               // push latest only, like before
                    changes.HasMeaningfulChange = true;
                }

                // Mark finalized ONLY when ended AND funds are actually settled.
                if (newStatus.HasValue && newStatus.Value > ToDbStatus(Status.PartlySuccess))
                {
                    // Only set pool to zero if the on-chain snapshot says it's zero
                    // (or if a Failed mission has completed all refunds).
                    var shouldZeroPool = md.CroCurrent == 0
                                        || (newStatus.Value == ToDbStatus(Status.Failed) && md.AllRefunded == true);
                    if (shouldZeroPool)
                    {
                        try
                        {
                            await using var zero = new NpgsqlCommand(@"
                                update missions set cro_current_wei = 0, updated_at = now()
                                where mission_address = @a;", conn);
                            zero.Parameters.AddWithValue("a", mission);
                            await zero.ExecuteNonQueryAsync(token);
                        }
                        catch { /* best effort */ }
                    }

                    if (!curFinal)
                    {
                        var settled = (newStatus.Value == ToDbStatus(Status.Success) && md.CroCurrent == 0)
                                    || (newStatus.Value == ToDbStatus(Status.Failed) && md.AllRefunded == true);
                        if (settled)
                        {
                            await using var fin = new NpgsqlCommand(@"
                                update missions set finalized = true, updated_at = now()
                                where mission_address = @a;", conn);
                            fin.Parameters.AddWithValue("a", mission);
                            await fin.ExecuteNonQueryAsync(token);
                        }
                    }
                }
            }

            return changes;
        }
// #endregion

// #region Finalize / Refund

        /// <summary>
        /// Attempts to call forceFinalizeMission on the factory for the given mission
        /// using the configured signer account, after validating preconditions via
        /// ShouldForceFinalizeAsync. Returns true when the transaction succeeds
        /// (receipt.Status == 1).
        /// </summary>
        private async Task<bool>                        AttemptFinalizeAsync                (string mission, CancellationToken token) {
            if (_finalizerAccount == null)
            {
                _log.LogWarning("Finalize requested for {mission} but no signer configured (Owner:PK). Skipping.", mission);
                return false;
            }

            if (!await ShouldForceFinalizeAsync(mission, token))
            {
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
                _log.LogInformation((int)IdxEvt.ForceFinalizeMission, "forceFinalizeMission() {mission} -> {status} (tx={tx})", mission, ok ? "OK" : "FAILED", txHash);
                return ok;
            }
            catch (Exception ex)
            {
                _log.LogDebug(ex, "forceFinalizeMission() attempt failed for {mission}", mission);
                return false;
            }
        }

        /// <summary>
        /// Checks whether a mission should be force-finalized based on database
        /// state (status, mission_end, finalized, pool) and a fresh on-chain
        /// snapshot. Returns true only when there are still funds or refunds
        /// pending after mission_end.
        /// </summary>
        private async Task<bool>                        ShouldForceFinalizeAsync            (string mission, CancellationToken token) {
            await using var conn = new NpgsqlConnection(_pg);
            await conn.OpenAsync(token);
            await using var cmd = new NpgsqlCommand(@"
                select status, mission_end, coalesce(finalized,false), cro_current_wei, coalesce(all_refunded,false)
                from missions where mission_address = @a;", conn);
            cmd.Parameters.AddWithValue("a", mission);

            await using var rdr = await cmd.ExecuteReaderAsync(token);
            if (!await rdr.ReadAsync(token)) return false;

            var statusValue = rdr.IsDBNull(0) ? (short)99 : rdr.GetInt16(0);
            var meSec       = rdr.IsDBNull(1) ? 0L        : rdr.GetInt64(1);
            var finalized   = !rdr.IsDBNull(2) && rdr.GetBoolean(2);
            var croNowDb    = rdr.IsDBNull(3) ? "0"       : rdr.GetString(3);
            var allRefunded = !rdr.IsDBNull(4) && rdr.GetBoolean(4);

            if (finalized) return false;
            if (meSec == 0) return false;

            var endAt = FromUnix(meSec);
            if (DateTime.UtcNow < endAt) return false;

            // Past mission_end → check contract snapshot
            try
            {
                var wrap = await RunRpc(
                    w => w.Eth.GetContractQueryHandler<B6.Contracts.GetMissionDataFunction>()
                            .QueryDeserializingToObjectAsync<B6.Contracts.MissionDataWrapper>(
                                new B6.Contracts.GetMissionDataFunction(), mission, null),
                    "Call.getMissionData,ShouldFinalize");

                if (wrap == null || wrap.Data == null) return false;

                var d             = wrap.Data;
                var onchainStatus = ToStatus((short)d.Status);
                var fundsInPool   = d.CroCurrent > 0;

                // Failed + not all_refunded → refunds still pending on-chain.
                var refundsPending =
                    onchainStatus == Status.Failed &&
                    d.AllRefunded == false;

                // If funds remain in the pool or refunds are still pending, we should try finalize()
                if (fundsInPool || refundsPending)
                    return true;

                // No funds + no refunds pending:
                // - If the status is still <= PartlySuccess we can call finalize()
                //   to cleanly close the mission.
                return onchainStatus <= Status.PartlySuccess;
            }
            catch (Exception ex)
            {
                _log.LogDebug(ex, "ShouldForceFinalizeAsync() snapshot failed for {mission}", mission);
                return false;
            }

        }

        /// <summary>
        /// Determines whether refundPlayers() should be called for a mission
        /// based on database state. Returns true when status == Failed,
        /// finalized == false and all_refunded == false.
        /// </summary>
        private async Task<bool>                        ShouldRefundAsync                   (string mission, CancellationToken token) {
            await using var conn = new NpgsqlConnection(_pg);
            await conn.OpenAsync(token);
            await using var cmd = new NpgsqlCommand(@"
                select status, coalesce(finalized,false), coalesce(all_refunded,false)
                from missions where mission_address = @a;", conn);
            cmd.Parameters.AddWithValue("a", mission);

            await using var rdr = await cmd.ExecuteReaderAsync(token);
            if (!await rdr.ReadAsync(token)) return false;

            var statusValue = rdr.IsDBNull(0) ? (short)99 : rdr.GetInt16(0);
            var status      = ToStatus(statusValue);
            var finalized   = !rdr.IsDBNull(1) && rdr.GetBoolean(1);
            var allRefunded = !rdr.IsDBNull(2) && rdr.GetBoolean(2);

            if (finalized)
            {
                _log.LogInformation((int)IdxEvt.RefundSkip,
                    "Refund skip: {mission} is already finalized", mission);
                return false;
            }

            if (status != Status.Failed)
            {
                _log.LogInformation((int)IdxEvt.RefundSkip,
                    "Refund skip: {mission} status={st} (need Failed/7)", mission, statusValue);
                return false;
            }

            if (allRefunded)
            {
                _log.LogInformation((int)IdxEvt.RefundSkip,
                    "Refund skip: {mission} already refunded", mission);
                return false;
            }

            // No time gating: Failed (7) + !all_refunded → refund now
            _log.LogInformation((int)IdxEvt.RefundEligible,
                "Refund eligible: {mission} (status=Failed/7, all_refunded=false)", mission);
            return true;
        }

        /// <summary>
        /// Attempts to call refundPlayers for the given mission using the signer
        /// account, after validating preconditions via ShouldRefundAsync.
        /// Returns true when the transaction succeeds (receipt.Status == 1).
        /// </summary>
        private async Task<bool>                        AttemptRefundAsync                  (string mission, CancellationToken token) {
            if (_finalizerAccount == null)
            {
                _log.LogWarning("Refund requested for {mission} but no signer configured (Owner:PK). Skipping.", mission);
                _log.LogInformation((int)IdxEvt.RefundSkipped, "Refund SKIPPED (no signer) for {mission}", mission);
                return false;
            }

            if (!await ShouldRefundAsync(mission, token)) {
                return false;
            }

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
                _log.LogInformation((int)IdxEvt.RefundPlayers, "refundPlayers() {mission} -> {status} (tx={tx})", mission, ok ? "OK" : "FAILED", txHash);
                return ok;
            }
            catch (Exception ex)
            {
                _log.LogDebug(ex, "refundPlayers() attempt failed for {mission}", mission);
                return false;
            }
        }

// #endregion

// #region Push notifications

        /// <summary>
        /// Sends a push/mission HTTP notification for the given mission to the
        /// external push API, if push configuration is present, including an
        /// optional textual reason and optional transaction hash.
        /// </summary>
        private async Task                              NotifyMissionUpdatedAsync           (string mission, string? reason, string? txHash, CancellationToken ct = default) {
            if (string.IsNullOrEmpty(_pushBase) || string.IsNullOrEmpty(_pushKey)) return;

            using var req = new HttpRequestMessage(HttpMethod.Post, $"{_pushBase.TrimEnd('/')}/push/mission");
            var payload = new {
                Mission = mission,
                Reason  = reason,
                TxHash  = txHash
            };
            req.Content = JsonContent.Create(payload);
            req.Headers.Add("X-Push-Key", _pushKey);

            try
            {
                var resp = await _http.SendAsync(req, ct);
                _log.LogInformation((int)IdxEvt.PushMission,
                    "push/mission {mission} (reason={reason}, tx={tx}) -> {code}",
                    mission,
                    reason ?? "<none>",
                    txHash ?? "<none>",
                    (int)resp.StatusCode);
            }
            catch (Exception ex)
            {
                _log.LogWarning(ex, "push/mission failed for {mission}", mission);
            }
        }

        /// <summary>
        /// Sends a push/status HTTP notification carrying the new mission status
        /// to the external push API, if push configuration is present.
        /// </summary>
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

        /// <summary>
        /// Sends a push/round HTTP notification for a newly detected round winner
        /// to the external push API, if push configuration is present.
        /// </summary>
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

// #endregion

// #region RPC

        /// <summary>
        /// Executes the given RPC function against the current Web3 client with
        /// rich logging, transient error handling, benign error rollup and
        /// automatic endpoint switching on repeated failures.
        /// </summary>
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

        /// <summary>
        /// Switches the active Web3 instance to the RPC endpoint at the given
        /// index, using the signer account when available.
        /// </summary>
        private void                                    UseRpc                              (int idx) {
            _rpcIndex = idx % _rpcEndpoints.Count;
            var url = _rpcEndpoints[_rpcIndex];
            // NEW: use signer when available so we can send finalize()
            _web3 = _finalizerAccount != null ? new Web3(_finalizerAccount, url)
                                            : new Web3(url);
            //_log.LogInformation((int)IdxEvt.UseRpc, "Using RPC[{idx}]: {url}", _rpcIndex, url);
        }

        /// <summary>
        /// Advances to the next configured RPC endpoint in a round-robin fashion.
        /// Returns true when the endpoint was actually changed.
        /// </summary>
        private bool                                    SwitchRpc                           () {
            if (_rpcEndpoints.Count <= 1) return false;
            var next = (_rpcIndex + 1) % _rpcEndpoints.Count;
            if (next == _rpcIndex) return false;
            var oldUrl = _rpcEndpoints[_rpcIndex];
            UseRpc(next);
            _log.LogWarning("Switched RPC from {old} to {nu}", oldUrl, _rpcEndpoints[_rpcIndex]);
            return true;
        }

        /// <summary>
        /// Periodically logs an aggregated summary of RPC usage by context and
        /// caller when the configured summary interval elapses.
        /// </summary>    
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

                _log.LogInformation((int)IdxEvt.RpcSummary, "{msg}", msg);

        }

        /// <summary>
        /// Records a single RPC attempt for the given context and caller into
        /// the in-memory counters used by FlushRpcSummaryIfDue.
        /// </summary>
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

        /// <summary>
        /// Writes a single RPC log line to the per-day log file for the given
        /// kind, rotating files at the UTC day boundary. Best-effort only.
        /// </summary>
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

// #endregion

// #region Benign errors

        /// <summary>
        /// Attempts to classify a provider-side exception into a short benign
        /// error code (e.g. 502, 503, 504) based on the exception message text.
        /// </summary>
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

        /// <summary>
        /// Increments in-memory and database counters for benign provider errors,
        /// and periodically logs a daily rollup for observability.
        /// </summary>
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
                        _log.LogInformation((int)IdxEvt.RpcBenignError, "RPC benign error rollup {day}: {summary}", y, summary);
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
                        up.Parameters.AddWithValue("k", key ?? string.Empty);
                        await up.ExecuteNonQueryAsync();
                    }
                    catch { /* best-effort only */ }
                });
            }
            catch { /* never block caller */ }
        }

        /// <summary>
        /// Normalizes a context label into a short "kind" string used for RPC
        /// logging and statistics (e.g. stripping argument lists).
        /// </summary>
        private static string                           NormalizeKind                       (string context) {
            if (string.IsNullOrWhiteSpace(context)) return "RPC";
            var p = context.IndexOf('(');
            return p > 0 ? context.Substring(0, p) : context;
        }

        /// <summary>
        /// Returns true when the given exception is considered transient for the
        /// purposes of RPC retry logic (HTTP, timeout, JSON-RPC errors, etc.).
        /// </summary>
        private static bool                             IsTransient                         (Exception ex) {
            return ex is Nethereum.JsonRpc.Client.RpcResponseException
                || ex is System.Net.Http.HttpRequestException
                || ex is TaskCanceledException
                || (ex.InnerException != null && IsTransient(ex.InnerException));
        }

        /// <summary>
        /// Detects 429 / "Too Many Requests" style rate-limiting errors in the
        /// exception chain so that the caller can apply a cooldown.
        /// </summary>
        private static bool                             IsRateLimited                       (Exception ex) {
            if (ex == null) return false;
            var msg = ex.Message ?? string.Empty;
            if (msg.IndexOf("429", StringComparison.OrdinalIgnoreCase) >= 0) return true;
            if (msg.IndexOf("Too Many Requests", StringComparison.OrdinalIgnoreCase) >= 0) return true;
            return ex.InnerException != null && IsRateLimited(ex.InnerException);
        }

// #endregion

// #region Core loop

        /// <summary>
        /// Core time-based loop that is invoked once per second from ExecuteAsync.
        /// This method orchestrates:
        ///  - processing of frontend kick events,
        ///  - periodic polling of the factory for new missions,
        ///  - loading of all non-ended missions,
        ///  - and dispatching to per-mission time-based handlers in parallel.
        /// </summary>
        private async Task                              coreLoop                            (CancellationToken token) {
            var nowUtc = DateTime.UtcNow;

            // 0) Process kicks first (mission-created / enroll-succeeded / bank-succeeded).
            //    This stays sequential because it updates shared queues and DB state.
            if (_kickRequested || !_kickMissions.IsEmpty)
            {
                _kickRequested = false;
                await ProcessPendingKicksAsync(token);
                await ProcessKickQueueAsync(token); // refresh snapshots for kicked missions
            }

            // 1) Periodically poll the factory for new or changed missions.
            _newMissionsPollCounter++;
            if (_newMissionsPollCounter >= newMissionsPollFreq)
            {
                _newMissionsPollCounter = 0;
                await checkForNewMissions(token);
            }

            // 2) Load all non-ended missions (status < Success) into memory
            //    for time-based processing.
            var openMissions = await LoadOpenMissionsAsync(token);

            // 3) Collect all per-mission, time-based handlers into a single task list
            //    so they can run in parallel. Each handler is wrapped in SafeTimeHandler
            //    to ensure that one failing mission/phase does not cancel the others.
            var tasks = new List<Task>();

            foreach (var mission in openMissions)
            {
                // MissionCreated → Pending
                if (mission.MissionCreatedUtc != DateTime.MinValue &&
                    nowUtc >= mission.MissionCreatedUtc)
                {
                    tasks.Add(SafeTimeHandler(
                        () => processStartPending(mission, nowUtc, token),
                        mission.Address,
                        "StartPending",
                        token));
                }

                // EnrollmentStart → Enrolling
                if (mission.EnrollmentStartUtc != DateTime.MinValue &&
                    nowUtc >= mission.EnrollmentStartUtc)
                {
                    tasks.Add(SafeTimeHandler(
                        () => processStartEnrollment(mission, nowUtc, token),
                        mission.Address,
                        "StartEnrollment",
                        token));
                }

                // EnrollmentEnd → start 30s, 1 Hz poll for Arming / Failed
                if (mission.EnrollmentEndUtc != DateTime.MinValue &&
                    nowUtc >= mission.EnrollmentEndUtc)
                {
                    tasks.Add(SafeTimeHandler(
                        () => processEndEnrollment(mission, nowUtc, token),
                        mission.Address,
                        "EndEnrollment",
                        token));
                }

                // MissionStart → mission becomes Active
                if (mission.MissionStartUtc != DateTime.MinValue &&
                    nowUtc >= mission.MissionStartUtc)
                {
                    tasks.Add(SafeTimeHandler(
                        () => processMissionStart(mission, nowUtc, token),
                        mission.Address,
                        "MissionStart",
                        token));
                }

                // Cooldown start & end derived from pauseTimestamp and per-round pause settings.
                if (mission.PauseTimestampSeconds.HasValue && mission.PauseTimestampSeconds.Value > 0)
                {
                    var pauseStartUtc = FromUnix(mission.PauseTimestampSeconds.Value);

                    if (nowUtc >= pauseStartUtc)
                    {
                        tasks.Add(SafeTimeHandler(
                            () => processStartCooldown(mission, nowUtc, token),
                            mission.Address,
                            "StartCooldown",
                            token));
                    }

                    // NOTE: whether this is the last round or not, and which pause duration
                    // to use, will be handled inside processEndCooldown() once we implement it.
                    tasks.Add(SafeTimeHandler(
                        () => processEndCooldown(mission, nowUtc, token),
                        mission.Address,
                        "EndCooldown",
                        token));
                }

                // MissionEnd → terminal outcome (Failed / PartlySuccess / Success)
                if (mission.MissionEndUtc != DateTime.MinValue &&
                    nowUtc >= mission.MissionEndUtc)
                {
                    tasks.Add(SafeTimeHandler(
                        () => processMissionEnd(mission, nowUtc, token),
                        mission.Address,
                        "MissionEnd",
                        token));
                }
            }

            // 4) Execute all time-based handlers in parallel.
            //    SafeTimeHandler ensures that exceptions are logged and swallowed,
            //    so Task.WhenAll will not cancel the remaining handlers.
            if (tasks.Count > 0)
            {
                await Task.WhenAll(tasks);
            }
        }

        /// <summary>
        /// Processes pending kicks from the indexer_kicks table,
        /// moving them into the in-memory kick queue for processing.
        /// Also captures the optional frontend event type for each kick.
        /// </summary>
        private async Task                              ProcessPendingKicksAsync            (CancellationToken token) {
            try
            {
                await using var conn = new Npgsql.NpgsqlConnection(_pg);
                await conn.OpenAsync(token);

                // Fetch and delete pending kicks (best-effort dedupe)
                var kicks = new List<KickMission>();
                await using (var cmd = new Npgsql.NpgsqlCommand(@"
                    delete from indexer_kicks
                    where id in (
                        select id from indexer_kicks
                        order by id
                        limit 200
                    )
                    returning mission_address, tx_hash, event_type;", conn))
                await using (var rd = await cmd.ExecuteReaderAsync(token))
                {
                    while (await rd.ReadAsync(token))
                    {
                        // mission_address is required; normalize to lower-case
                        var mission = (rd.IsDBNull(0) ? string.Empty : rd.GetString(0) ?? string.Empty)
                            .ToLowerInvariant();

                        // tx_hash is optional (can be null)
                        string? txHash = rd.IsDBNull(1) ? null : rd.GetString(1);

                        // event_type is optional (can be null)
                        string? eventType = rd.IsDBNull(2) ? null : rd.GetString(2);

                        kicks.Add(new KickMission
                        {
                            Mission   = mission,
                            TxHash    = txHash,
                            EventType = eventType
                        });
                    }
                }

                foreach (var kick in kicks)
                {
                    if (!string.IsNullOrWhiteSpace(kick.Mission))
                        _kickMissions.Enqueue(kick);
                }

                if (kicks.Count > 0)
                    _kickRequested = true;
            }
            catch (Exception ex)
            {
                _log.LogDebug(ex, "ProcessPendingKicksAsync failed");
            }
        }

        /// <summary>
        /// Processes the kick queue, refreshing the state of each mission.
        /// </summary>
        private async Task                              ProcessKickQueueAsync               (CancellationToken token) {
            var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            while (_kickMissions.TryDequeue(out var kick))
            {
                var mission = kick.Mission;
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

                    // Kick-based refresh: include the originating tx hash and a refined reason for the frontend.
                    // This indicates that the refresh was triggered by a frontend event (/events/* → KickMissionAsync).
                    await NotifyMissionUpdatedAsync(
                        mission,
                        reason: "Kick.FrontendEvent",
                        txHash: kick.TxHash,
                        ct: token
                    );

                }
                catch (Exception ex)
                {
                    _log.LogDebug(ex, "Kick refresh failed for {mission}", mission);
                }
            }
        }

        /// <summary>
        /// Called by the time-based core loop every newMissionsPollFreq seconds.
        /// Polls the MissionFactory for changes after the last known sequence and,
        /// for each changed mission, refreshes the on-chain snapshot into the database.
        /// </summary>
        private async Task                              checkForNewMissions                 (CancellationToken token) {
            try
            {
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
                        // Single source of truth: this call will UPSERT the mission row
                        // (schedule, rounds, amounts, players) into the database.
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
            catch (OperationCanceledException) when (token.IsCancellationRequested)
            {
                // Respect cancellation without logging as an error.
            }
            catch (Exception ex)
            {
                // Best-effort: indexer will simply try again on the next scheduled poll.
                _log.LogWarning(ex, "checkForNewMissions failed; will be retried on next poll.");
            }
        }

        /// <summary>
        /// Runs a time-based mission handler with isolated exception handling so that
        /// failures for a single mission/phase do not cancel other handlers when using
        /// Task.WhenAll in the core loop.
        /// </summary>
        private async Task                              SafeTimeHandler                     (Func<Task> handler, string missionAddress, string phase, CancellationToken token) {
            try
            {
                await handler();
            }
            catch (OperationCanceledException) when (token.IsCancellationRequested)
            {
                // Cooperative cancellation: let the outer ExecuteAsync loop handle shutdown.
            }
            catch (Exception ex)
            {
                _log.LogError(ex, "Time-based handler '{phase}' failed for mission {mission}", phase, missionAddress);
                // Swallow the exception so that other handlers can continue unaffected.
            }
        }

        /// <summary>
        /// Loads all missions with a non-terminal status (status &lt; Success) from the database
        /// and projects them into MissionSchedule objects for the time-based core loop.
        /// </summary>
        private async Task<List<MissionSchedule>>       LoadOpenMissionsAsync               (CancellationToken token) {
            var missions = new List<MissionSchedule>();

            await using var conn = new NpgsqlConnection(_pg);
            await conn.OpenAsync(token);

            // We only load missions that have not reached a terminal Success state yet.
            // The status column is written by ApplySnapshotToDatabaseAsync and matches
            // the local Status enum (Pending..Failed).
            await using var cmd = new NpgsqlCommand(@"
                select
                    mission_address,
                    status,
                    mission_created,
                    enrollment_start,
                    enrollment_end,
                    mission_start,
                    mission_end,
                    pause_timestamp,
                    round_pause_secs,
                    last_round_pause_secs,
                    mission_rounds_total,
                    round_count
                from missions
                where status &lt; @successStatus;", conn);

            cmd.Parameters.AddWithValue("successStatus", (short)Status.Success);

            await using var rdr = await cmd.ExecuteReaderAsync(token);
            while (await rdr.ReadAsync(token))
            {
                var addr = (rdr.IsDBNull(0) ? string.Empty : rdr.GetString(0)).ToLowerInvariant();
                if (string.IsNullOrWhiteSpace(addr))
                    continue;

                short statusVal        = rdr.IsDBNull(1) ? (short)0 : rdr.GetInt16(1);
                long  createdSec       = rdr.IsDBNull(2) ? 0L        : rdr.GetInt64(2);
                long  enrollStartSec   = rdr.IsDBNull(3) ? 0L        : rdr.GetInt64(3);
                long  enrollEndSec     = rdr.IsDBNull(4) ? 0L        : rdr.GetInt64(4);
                long  missionStartSec  = rdr.IsDBNull(5) ? 0L        : rdr.GetInt64(5);
                long  missionEndSec    = rdr.IsDBNull(6) ? 0L        : rdr.GetInt64(6);
                long? pauseSec         = rdr.IsDBNull(7) ? (long?)null : rdr.GetInt64(7);
                int   roundPauseSecs   = rdr.IsDBNull(8) ? 0         : rdr.GetInt32(8);
                int   lastRoundPause   = rdr.IsDBNull(9) ? 0         : rdr.GetInt32(9);
                int   totalRounds      = rdr.IsDBNull(10) ? 0        : rdr.GetInt32(10);
                int   roundCount       = rdr.IsDBNull(11) ? 0        : rdr.GetInt32(11);

                static DateTime ToUtc(long seconds) =>
                    seconds > 0
                        ? FromUnix(seconds)
                        : DateTime.MinValue;

                var schedule = new MissionSchedule
                {
                    Address               = addr,
                    Status                = (Status)statusVal,
                    MissionCreatedUtc     = ToUtc(createdSec),
                    EnrollmentStartUtc    = ToUtc(enrollStartSec),
                    EnrollmentEndUtc      = ToUtc(enrollEndSec),
                    MissionStartUtc       = ToUtc(missionStartSec),
                    MissionEndUtc         = ToUtc(missionEndSec),
                    PauseTimestampSeconds = pauseSec,
                    RoundPauseSecs        = roundPauseSecs,
                    LastRoundPauseSecs    = lastRoundPause,
                    MissionRoundsTotal    = totalRounds,
                    RoundCount            = roundCount
                };

                missions.Add(schedule);
            }

            return missions;
        }

        /// <summary>
        /// Handles the initial Pending phase right after mission creation.
        /// Currently a no-op placeholder: scheduling is fully driven by mission timestamps
        /// and factory snapshots, so we do not need extra work at mission creation time.
        /// This method is still wired into the core loop for future extensibility.
        /// </summary>
        private async Task                              processStartPending                 (MissionSchedule mission, DateTime nowUtc, CancellationToken token) {
            // No work required at mission creation time for now.
            await Task.CompletedTask;
        }

        /// <summary>
        /// Handles the start of the enrollment phase.
        /// Currently a no-op placeholder: scheduling is fully driven by mission timestamps
        /// and factory snapshots, so we do not need extra work at mission creation time.
        /// This method is still wired into the core loop for future extensibility.
        /// </summary>
        private async Task                           processStartEnrollment                (MissionSchedule mission, DateTime nowUtc, CancellationToken token) {
            // No-op for now: enrollment opening is fully time/snapshot-driven.
            // Keeping this handler allows us to easily hook in behaviour later
            // without changing the core loop wiring.
            await Task.CompletedTask;
        }

        /// <summary>
        /// Handles the end of the enrollment window.
        ///
        /// Behaviour:
        /// - For up to 30 seconds after enrollmentEnd, once per second (driven by coreLoop),
        ///   this handler polls the on-chain mission snapshot via RefreshMissionSnapshotAsync.
        /// - After each snapshot it reads the current status from the missions table.
        /// - If the status becomes:
        ///     - Arming:
        ///         -> No further action is required; polling will naturally stop once the
        ///            DB status is >= Arming and MissionSchedule is reloaded.
        ///     - Failed:
        ///         -> Trigger refundPlayers() via AttemptRefundAsync.
        ///         -> After a successful refund tx, refresh the mission snapshot again
        ///            so the DB reflects all_refunded/pool=0, which in turn causes the
        ///            existing push pipeline to notify the frontend.
        ///
        /// Notes:
        /// - Poll frequency is effectively 1 Hz because coreLoop runs once per second.
        /// - Polling is limited to the fixed window [enrollmentEnd, enrollmentEnd+30s].
        /// - We do not introduce additional per-mission state; the window is derived
        ///   solely from the mission's enrollmentEnd timestamp.
        /// </summary>
        private async Task                              processEndEnrollment                 (MissionSchedule mission, DateTime nowUtc, CancellationToken token) {
            // Safety: if no enrollmentEnd timestamp is known, there is nothing to do.
            if (mission.EnrollmentEndUtc == DateTime.MinValue)
                return;

            // Hard 30-second polling window after enrollmentEnd.
            var windowStartUtc = mission.EnrollmentEndUtc;
            var windowEndUtc   = windowStartUtc.AddSeconds(30);

            // Only act inside the window [enrollmentEnd, enrollmentEnd+30s].
            if (nowUtc < windowStartUtc || nowUtc > windowEndUtc)
                return;

            var address = mission.Address;
            if (string.IsNullOrWhiteSpace(address))
                return;

            // 1) Poll on-chain by refreshing the mission snapshot.
            //    This will:
            //      - update the missions & players tables,
            //      - record status history,
            //      - and push mission/status updates when meaningful changes occur.
            await RefreshMissionSnapshotAsync(address, token);

            // 2) Read the current status from the missions table.
            short statusValue = (short)Status.Pending;

            await using (var conn = new NpgsqlConnection(_pg))
            {
                await conn.OpenAsync(token);

                await using var cmd = new NpgsqlCommand(@"
                    select status
                    from missions
                    where mission_address = @a;", conn);

                cmd.Parameters.AddWithValue("a", address);

                await using var rdr = await cmd.ExecuteReaderAsync(token);
                if (await rdr.ReadAsync(token) && !rdr.IsDBNull(0))
                {
                    statusValue = rdr.GetInt16(0);
                }
            }

            // 3) Decide based on the current status.
            if (statusValue == (short)Status.Arming)
            {
                // "Arming" means the mission satisfied minPlayers and is ready to start.
                // No extra work is required here. On the next coreLoop tick, LoadOpenMissionsAsync
                // will see the updated status, so this handler will naturally stop doing work.
                return;
            }

            if (statusValue == (short)Status.Failed)
            {
                // "Failed" at enrollment end means "not enough players".
                // According to the design:
                //  - We must trigger refundPlayers() from the indexer.
                //  - After a successful refund, we refresh the snapshot so the DB (and
                //    existing push pipeline) reflect the refunded state.
                var refunded = await AttemptRefundAsync(address, token);
                if (refunded)
                {
                    // Best-effort: update DB & push pipeline after refund.
                    try
                    {
                        await RefreshMissionSnapshotAsync(address, token);
                    }
                    catch (Exception ex)
                    {
                        _log.LogWarning(ex, "Post-refund snapshot refresh failed for {mission}", address);
                    }
                }

                return;
            }

            // For all other statuses (e.g. still Enrolling due to lag, or already Active),
            // we simply continue polling on subsequent ticks within the 30-second window
            // until one of the terminal conditions (Arming/Failed) is observed.
        }

        /// <summary>
        /// Handles the start of the missionStart phase.
        /// Currently a no-op placeholder: scheduling is fully driven by mission timestamps
        /// and factory snapshots, so we do not need extra work at mission creation time.
        /// This method is still wired into the core loop for future extensibility.
        /// </summary>
        private async Task                              processMissionStart          (MissionSchedule mission, DateTime nowUtc, CancellationToken token) {
            // Intentionally no-op:
            // - We do not need to read from chain or update the database here.
            // - Real-time status changes around missionStart are only interesting
            //   for history/logging and are currently handled elsewhere.
            await Task.CompletedTask;
        }

        /// <summary>
        /// Handles the start of the cooldown period after a successful BANK round.
        ///
        /// Goal:
        /// - Do NOT read from the blockchain again.
        /// - Notify the frontend exactly once per cooldown start, so it can switch
        ///   the UI to something like "Cooldown 1 minute".
        ///
        /// Implementation details:
        /// - The core loop calls this handler once per second for missions that have
        ///   a non-null PauseTimestampSeconds value.
        /// - We use an in-memory runtime state dictionary to ensure that each unique
        ///   pause_timestamp only triggers a single push, regardless of minor delays
        ///   or how many times this handler runs during the cooldown.
        /// </summary>
        private async Task                              processStartCooldown         (MissionSchedule mission, DateTime nowUtc, CancellationToken token) {
            // If no pause timestamp is known, there is nothing to do.
            if (!mission.PauseTimestampSeconds.HasValue || mission.PauseTimestampSeconds.Value <= 0)
                return;

            var pauseTs = mission.PauseTimestampSeconds.Value;

            var pauseStartUtc = FromUnix(pauseTs);

            // We only act once the pause start time has actually been reached.
            // This avoids pushing early in case the snapshot landed slightly before
            // the actual on-chain timestamp.
            if (nowUtc < pauseStartUtc)
                return;

            var address = mission.Address;
            if (string.IsNullOrWhiteSpace(address))
                return;

            // Look up or create the runtime state for this mission.
            var state = _runtimeState.GetOrAdd(address, _ => new MissionRuntimeState());

            // If we have already sent a cooldown-start notification for this specific
            // pause_timestamp, we do nothing. This makes the handler idempotent across
            // coreLoop ticks and resilient to small scheduling delays.
            if (state.LastCooldownStartPauseTimestamp.HasValue &&
                state.LastCooldownStartPauseTimestamp.Value == pauseTs)
            {
                return;
            }

            // Mark this pause_timestamp as notified before pushing, so even if the
            // push itself throws (and is logged by SafeTimeHandler), we do not spam
            // the frontend on subsequent ticks.
            state.LastCooldownStartPauseTimestamp = pauseTs;

            _log.LogInformation(
                "Cooldown start detected for mission {mission} (pauseStartUtc={pauseStartUtc:u}, now={nowUtc:u}, pauseTs={pauseTs})",
                address,
                pauseStartUtc,
                nowUtc,
                pauseTs);

            // Best-effort push:
            // The frontend will reload the mission via the API and can derive
            // the cooldown state purely from timestamps (no extra chain calls).
            await NotifyMissionUpdatedAsync(address, reason: "Cooldown.Start", txHash: null, ct: token);
        }

        /// <summary>
        /// Handles the end of the cooldown period after a BANK round.
        ///
        /// Behaviour:
        /// - Compute the expected cooldown end time from pause_timestamp and the
        ///   configured per-round cooldown duration (roundPauseSecs or lastRoundPauseSecs).
        /// - For up to 30 seconds after that time, once per second (driven by coreLoop),
        ///   poll the mission snapshot from chain via RefreshMissionSnapshotAsync.
        /// - After each snapshot, read the stored status from the missions table.
        /// - When the status becomes Active again, treat the cooldown as ended and
        ///   notify the frontend so it can refresh its view.
        ///
        /// Notes:
        /// - We do not introduce additional per-mission state here; repeated pushes
        ///   for the same cooldown end are harmless because the frontend will simply
        ///   re-render the same "Active" state.
        /// - Terminal outcomes at missionEnd (Failed/PartlySuccess/Success) worden
        ///   afgehandeld in processMissionEnd, niet hier.
        /// </summary>
        private async Task                              processEndCooldown           (MissionSchedule mission, DateTime nowUtc, CancellationToken token) {
            // 1) Basic guards: we need a pause timestamp to reason about cooldown.
            if (!mission.PauseTimestampSeconds.HasValue || mission.PauseTimestampSeconds.Value <= 0)
                return;

            var pauseTs       = mission.PauseTimestampSeconds.Value;
            var pauseStartUtc = FromUnix(pauseTs);

            // 2) Bepaal de cooldownduur: normale ronde of (optioneel) laatste ronde.
            //    Dit volgt dezelfde logica als in je eerdere on-chain code:
            //    - Als de volgende ronde de laatste is, gebruik lastRoundPauseSecs (indien > 0),
            //      anders roundPauseSecs.
            var basePauseSecs = mission.RoundPauseSecs;
            var lastPauseSecs = mission.LastRoundPauseSecs;

            // Default: normale cooldown
            var cooldownSecs = basePauseSecs;

            // Is de volgende ronde de laatste?
            if (mission.MissionRoundsTotal > 0 &&
                (mission.RoundCount + 1) == mission.MissionRoundsTotal &&
                lastPauseSecs > 0)
            {
                cooldownSecs = lastPauseSecs;
            }

            // Geen cooldownduur → niets te doen.
            if (cooldownSecs <= 0)
                return;

            var cooldownEndUtc = pauseStartUtc.AddSeconds(cooldownSecs);

            // 3) Hard 30-second polling window na cooldownEnd.
            var windowStartUtc = cooldownEndUtc;
            var windowEndUtc   = cooldownEndUtc.AddSeconds(30);

            // Alleen binnen het venster [cooldownEnd, cooldownEnd+30s] actief zijn.
            if (nowUtc < windowStartUtc || nowUtc > windowEndUtc)
                return;

            var address = mission.Address;
            if (string.IsNullOrWhiteSpace(address))
                return;

            // 4) Poll on-chain door de mission snapshot te verversen.
            //    Dit:
            //      - schrijft de nieuwste data naar de missions/players tabellen,
            //      - werkt status/history bij,
            //      - triggert bestaande push-logica als er echte veranderingen zijn.
            await RefreshMissionSnapshotAsync(address, token);

            // 5) Status uit de missions-tabel lezen.
            short statusValue = (short)Status.Pending;

            await using (var conn = new NpgsqlConnection(_pg))
            {
                await conn.OpenAsync(token);

                await using var cmd = new Npgsql.NpgsqlCommand(@"
                    select status
                    from missions
                    where mission_address = @a;", conn);

                cmd.Parameters.AddWithValue("a", address);

                await using var rdr = await cmd.ExecuteReaderAsync(token);
                if (await rdr.ReadAsync(token) && !rdr.IsDBNull(0))
                {
                    statusValue = rdr.GetInt16(0);
                }
            }

            // 6) Als de missie weer Active is, is de cooldown voorbij.
            if (statusValue == (short)Status.Active)
            {
                _log.LogInformation(
                    "Cooldown end detected for mission {mission} (cooldownEndUtc={cooldownEndUtc:u}, now={nowUtc:u}, pauseTs={pauseTs})",
                    address,
                    cooldownEndUtc,
                    nowUtc,
                    pauseTs);

                // Best effort: expliciet een push doen, ook al kan RefreshMissionSnapshotAsync
                // al een push veroorzaakt hebben bij status/round-wijzigingen.
                await NotifyMissionUpdatedAsync(address, reason: "Cooldown.End", txHash: null, ct: token);
            }

            // Voor andere statussen (bijv. nog Paused of een kortdurende chain-lag):
            // - doen we niets; de coreLoop zal deze handler opnieuw aanroepen op de
            //   volgende tick binnen dit 30s-window totdat Active wordt bereikt of
            //   het venster afloopt.
        }

        /// <summary>
        /// Handles the missionEnd window for a mission.
        ///
        /// Behaviour:
        /// - For up to 30 seconds after missionEndUtc, once per second (driven by coreLoop),
        ///   this handler:
        ///     1) refreshes the on-chain snapshot via RefreshMissionSnapshotAsync,
        ///     2) reads the stored status/finalized/refund flags from the missions table,
        ///     3) reacts to terminal statuses:
        ///        - Failed:
        ///             -> trigger refundPlayers() via AttemptRefundAsync
        ///             -> refresh snapshot (best-effort) after a successful refund
        ///             -> push frontend (so it sees "Failed – nobody banked, refunded")
        ///        - PartlySuccess:
        ///             -> trigger finalize via AttemptFinalizeAsync
        ///             -> refresh snapshot (best-effort) after a successful finalize
        ///             -> push frontend (so it sees "PartlySuccess")
        ///        - Success:
        ///             -> push frontend (so it sees the final Success state)
        ///
        /// Notes:
        /// - AttemptRefundAsync / AttemptFinalizeAsync already contain their own
        ///   safety checks (finalized flags, all_refunded, status conditions, etc.),
        ///   so repeated calls within the 30s window are safe; they will simply
        ///   no-op once everything is settled.
        /// </summary>
        private async Task                              processMissionEnd              (MissionSchedule mission, DateTime nowUtc, CancellationToken token) {
            // 1) Only act if we have a valid missionEnd timestamp.
            if (mission.MissionEndUtc == DateTime.MinValue)
                return;

            var windowStartUtc = mission.MissionEndUtc;
            var windowEndUtc   = mission.MissionEndUtc.AddSeconds(30);

            // Only work inside the [missionEnd, missionEnd+30s] window.
            if (nowUtc < windowStartUtc || nowUtc > windowEndUtc)
                return;

            var address = mission.Address;
            if (string.IsNullOrWhiteSpace(address))
                return;

            // 2) Poll on-chain by refreshing the mission snapshot.
            //    This updates the missions/players tables, status history and may
            //    already trigger existing push logic if there are meaningful changes.
            await RefreshMissionSnapshotAsync(address, token);

            // 3) Read the latest status/finalization flags from the missions table.
            short statusValue  = (short)Status.Pending;
            bool  finalized    = false;
            bool  allRefunded  = false;

            await using (var conn = new NpgsqlConnection(_pg))
            {
                await conn.OpenAsync(token);

                await using var cmd = new NpgsqlCommand(@"
                    select 
                        status, 
                        coalesce(finalized,false), 
                        coalesce(all_refunded,false)
                    from missions
                    where mission_address = @a;", conn);

                cmd.Parameters.AddWithValue("a", address);

                await using var rdr = await cmd.ExecuteReaderAsync(token);
                if (!await rdr.ReadAsync(token))
                {
                    // Mission not found in DB anymore; nothing we can do.
                    return;
                }

                if (!rdr.IsDBNull(0)) statusValue = rdr.GetInt16(0);
                if (!rdr.IsDBNull(1)) finalized   = rdr.GetBoolean(1);
                if (!rdr.IsDBNull(2)) allRefunded = rdr.GetBoolean(2);
            }

            // 4) Decide based on the current status.
            if (statusValue == (short)Status.Failed)
            {
                // Mission ended in Failed state (e.g. nobody banked).
                // We must trigger refundPlayers() from the indexer.
                _log.LogInformation("MissionEnd: Failed de...r {mission} (finalized={finalized}, allRefunded={allRefunded})",
                    address, finalized, allRefunded);

                var ok = await AttemptRefundAsync(address, token);
                if (ok)
                {
                    // Best-effort: refresh snapshot after a successful refund so that
                    // DB state (all_refunded/finalized/cro_current_wei) is fully up to date.
                    try
                    {
                        await RefreshMissionSnapshotAsync(address, token);
                    }
                    catch (Exception ex)
                    {
                        _log.LogWarning(ex, "Post-refund snapshot refresh failed for {mission}", address);
                    }
                }

                // Always notify the frontend so it can render the final Failed + refunded state.
                // Reason encodes that the mission ended in Failed state and refunds were (attempted) time-based.
                await NotifyMissionUpdatedAsync(address, "MissionEnd.Failed.RefundTriggered", null, token);
                return;
            }

            if (statusValue == (short)Status.PartlySuccess)
            {
                // Mission ended in PartlySuccess: some rounds claimed, some not.
                // We must trigger finalize logic from the indexer.
                _log.LogInformation("MissionEnd: PartlySuccess detected for {mission} (finalized={finalized}, allRefunded={allRefunded})",
                    address, finalized, allRefunded);

                var ok = await AttemptFinalizeAsync(address, token);
                if (ok)
                {
                    // Best-effort: refresh snapshot after a successful finalize so that
                    // DB state (finalized/cro_current_wei) is fully up to date.
                    try
                    {
                        await RefreshMissionSnapshotAsync(address, token);
                    }
                    catch (Exception ex)
                    {
                        _log.LogWarning(ex, "Post-finalize snapshot refresh failed for {mission}", address);
                    }
                }

                // Always notify the frontend so it can render the final PartlySuccess state.
                await NotifyMissionUpdatedAsync(address, "MissionEnd.PartlySuccess.FinalizeTriggered", null, token);
                return;
            }

            if (statusValue == (short)Status.Success)
            {
                // Mission is fully successful: all rounds claimed and funds distributed.
                // Even if snapshot may already have pushed, we explicitly notify so the
                // frontend is guaranteed to see the terminal Success state.
                _log.LogInformation("MissionEnd: Success detected for {mission} (finalized={finalized}, allRefunded={allRefunded})",
                    address, finalized, allRefunded);

                await NotifyMissionUpdatedAsync(address, "MissionEnd.Success.AllRoundsBanked", null, token);
                return;
            }

            // For all other statuses (e.g. still Active/Paused or a transient state):
            // - we do nothing here; the coreLoop will rerun this handler on the next
            //   tick within the 30-second window until a terminal state is observed.
        }

// #endregion

   }
}

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Nethereum.ABI.FunctionEncoding.Attributes;
using Nethereum.Contracts;
using Nethereum.Contracts.ContractHandlers;
using Nethereum.Contracts.CQS;
using Nethereum.Hex.HexTypes;
using Nethereum.RPC.Eth.DTOs;
using Nethereum.Web3;
using Nethereum.Web3.Accounts;
using Npgsql;
using NpgsqlTypes;
using System;
using System.Collections.Generic;
using System.Diagnostics;         
using System.Globalization;
using System.IO;  
using System.Net.Http;        
using System.Net.Http.Json; 
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

using B6.Contracts;

namespace B6.Indexer
{
    public class MissionIndexer : BackgroundService
    {
        // Logger on/off ------------------------------------------------------------------------------------------------
        private static readonly bool                    logRpcCalls = false;                // ← toggle RPC file logging
        // --------------------------------------------------------------------------------------------------------------
        private readonly ILogger<MissionIndexer>        _log;
        private readonly string                         _rpc;
        private readonly string                         _factory;
        private readonly string                         _pg;
        private Web3                                    _web3 = default!;
        private readonly long                           _factoryDeployBlock;
        private readonly List<string>                   _rpcEndpoints = new();
        private int                                     _rpcIndex = 0;
        private const int                               REORG_CUSHION = 3;
        private const int                               MAX_LOG_RANGE = 1800;
        private const bool                              scanMissionStatus = true;           // ← toggle mission-level status logs
        private readonly HttpClient                     _http = new HttpClient();
        private readonly string                         _pushBase;                          // e.g. https://b6missions.com/api
        private readonly string                         _pushKey;
        private readonly Dictionary<long, DateTime>     _blockTsCache = new();
        private readonly object                         _rpcLogLock   = new();
        private DateTime                                _rpcLogDay    = DateTime.MinValue;  // UTC date boundary
        private string                                  _rpcLogPath   = string.Empty;
        private readonly Dictionary<string,int>         _rpcCounts    = new(StringComparer.InvariantCulture);
        // ======== NEW: 5-minute RPC summary counters ========
        private readonly Dictionary<string,int>         _rpc5mByContext = new(StringComparer.InvariantCulture);
        private readonly Dictionary<string, Dictionary<string,int>> _rpc5mByCaller 
            = new(StringComparer.InvariantCulture);
        private DateTime                                _nextRpcSummaryUtc = DateTime.MinValue;
        private static readonly TimeSpan                _rpcSummaryPeriod = TimeSpan.FromMinutes(5);
        // =====================================================
        // --- Benign provider hiccup rollup (daily, UTC) -------------------------------
        private DateTime                                _benignDayUtc = DateTime.MinValue;
        private readonly Dictionary<string,int>         _benignCounts = new(StringComparer.InvariantCulture);
        private static readonly TimeSpan                _benignRetryDelay = TimeSpan.FromMilliseconds(800);
        // -----------------------------------------------------------------------------         
        private readonly int                            _maxWinPerMission;
        private readonly int                            _maxWinTotal;
        private readonly string                         _ownerPk;                           // from Key Vault / config
        // ---- Realtime poll schedule -----------------------------------------------
        private static readonly TimeSpan                _rtPollPeriod = TimeSpan.FromMinutes(5);
        private DateTime                                _nextRtPollUtc = DateTime.MinValue;
        // ---- Circuit breaker -------------------------------------------------------
        private int                                     _consecErrors = 0;
        private DateTime                                _suspendUntilUtc = DateTime.MinValue;
        private readonly int                            _maxConsecErrors;           // default 5
        private static readonly TimeSpan                _suspendFor = TimeSpan.FromSeconds(60);
        private int                                     _circuitTrips = 0;
        private readonly int                            _maxCircuitTrips;           // default 12

        // --- Global RPC throttle (max QPS) -------------------------------------------
        private readonly SemaphoreSlim _rpcThrottle = new(1,1);
        private DateTime _nextRpcEarliestUtc = DateTime.MinValue;
        private static TimeSpan PerRequestGap(int qps) => TimeSpan.FromMilliseconds(Math.Max(1, 1000 / Math.Max(1, qps)));
        private static readonly Random _rpcJitter = new Random();
        private static TimeSpan SmallJitter() => TimeSpan.FromMilliseconds(_rpcJitter.Next(15, 45));
        // ----------------------------------------------------------------------------- 

        private static readonly TimeSpan                RATE_LIMIT_COOLDOWN        = TimeSpan.FromSeconds(30);

        private volatile bool _kickRequested = false;
        private readonly System.Collections.Concurrent.ConcurrentQueue<string> _kickMissions = new();

        [Function("refundPlayers")]
        public class RefundPlayersFunction : FunctionMessage {
            // no args
        }

        [Function("getRealtimeStatus", "uint8")]
        public class GetRealtimeStatusFunction : FunctionMessage {
            // no args
        }

        [Function("forceFinalizeMission")]
        public class ForceFinalizeMissionFunction : FunctionMessage {
            // no args
        }

        private readonly                                RpcCyclePacer _pacer = new();

        private sealed class                            RpcCyclePacer {
            private readonly SemaphoreSlim _mux = new(1,1);
            private DateTime _startUtc = DateTime.MinValue;
            private TimeSpan _budget = TimeSpan.Zero;
            private int _planned = 0;
            private DateTime _nextUtc = DateTime.MinValue;

            public void Start(int planned, TimeSpan budget)
            {
                _planned  = Math.Max(1, planned);
                _budget   = budget;
                _startUtc = DateTime.UtcNow;
                _nextUtc  = _startUtc; // first call can run immediately
            }

            public void Reserve(int more)
            {
                if (more <= 0) return;
                Interlocked.Add(ref _planned, more);
            }

            public async Task GateAsync()
            {
                if (_budget == TimeSpan.Zero || _planned <= 0) return; // not started → no-op

                await _mux.WaitAsync();
                try
                {
                    var gapMs = Math.Max(1.0, _budget.TotalMilliseconds / Math.Max(1, _planned));
                    var gap   = TimeSpan.FromMilliseconds(gapMs);

                    var now   = DateTime.UtcNow;
                    if (_nextUtc < now) _nextUtc = now;
                    var wait = _nextUtc - now;
                    _nextUtc = _nextUtc + gap;

                    if (wait > TimeSpan.Zero)
                    {
                        try { await Task.Delay(wait); } catch { }
                    }
                }
                finally { _mux.Release(); }
            }
        }

        private sealed class                            RpcCycleCache {
            public long? TipBlock;
            public GetMissionsOutput? AllMissions;
            public readonly Dictionary<string, MissionDataWrapper> MissionTuples = new(StringComparer.InvariantCulture);
            public readonly Dictionary<string, byte> RealtimeStatus = new(StringComparer.InvariantCulture);
        }

        private static int                              CountWindows                (long start, long toBlock) {
            if (start > toBlock) return 0;
            int w = 0; long from = start;
            while (from <= toBlock)
            {
                long to = Math.Min(from + MAX_LOG_RANGE, toBlock);
                w++; from = to + 1;
            }
            return w;
        }

        private async Task<int>                         CountFactoryWindowsAsync    (long latest, CancellationToken token) {
            var toBlock = latest - REORG_CUSHION;
            if (toBlock <= 0) return 0;
            var cursor  = await GetCursorAsync("factory", token);
            var start   = cursor + 1;
            return CountWindows(start, toBlock);
        }

        private async Task<int>                         CountMissionWindowsAsync    (long latest, CancellationToken token) {
            var tipSafe = latest - REORG_CUSHION;
            if (tipSafe <= 0) return 0;

            int total = 0;
            await using var conn = new NpgsqlConnection(_pg);
            await conn.OpenAsync(token);
            await using var cmd = new NpgsqlCommand(@"
                select coalesce(last_seen_block,0) as from_block
                from missions
                where coalesce(last_seen_block,0) < @tipSafe
                and (coalesce(status,0) < 6 
                    or coalesce(realtime_status,0) not in (6,7));", conn);
            cmd.Parameters.AddWithValue("tipSafe", tipSafe);
            await using var rd = await cmd.ExecuteReaderAsync(token);
            while (await rd.ReadAsync(token))
            {
                var from = (long)rd["from_block"];
                total += CountWindows(from, tipSafe);
            }
            return total;
        }

        private async Task<int>                         CountRealtimePollMissionsAsync(CancellationToken token) {
            await using var c = new NpgsqlConnection(_pg);
            await c.OpenAsync(token);
            await using var cmd = new NpgsqlCommand(@"
                select count(*) 
                from missions
                where coalesce(status,0) < 5
                or (coalesce(status,0) in (5,6,7) and coalesce(realtime_status,0) not in (6,7));", c);
            var n = (long)(await cmd.ExecuteScalarAsync(token) ?? 0L);
            return (int)n;
        }

        private async Task<long>                        GetLatestBlockCachedAsync   (RpcCycleCache cache) {
            if (cache.TipBlock is long b) return b;
            b = await GetLatestBlockAsync();
            cache.TipBlock = b;
            return b;
        }

        private async Task<GetMissionsOutput?>          GetAllMissionsCachedAsync   (RpcCycleCache cache) {
            if (cache.AllMissions != null) return cache.AllMissions;
            var all = await RunRpc(
                w => w.Eth.GetContractQueryHandler<GetAllMissionsFunction>()
                    .QueryDeserializingToObjectAsync<GetMissionsOutput>(new GetAllMissionsFunction(), _factory, null),
                "Call.getAllMissions");
            cache.AllMissions = all;
            return all;
        }

        private async Task<MissionDataWrapper>          GetMissionDataCachedAsync   (string addr, RpcCycleCache cache) {
            if (cache.MissionTuples.TryGetValue(addr, out var wrap)) return wrap;
            wrap = await RunRpc(
                w => w.Eth.GetContractQueryHandler<GetMissionDataFunction>()
                    .QueryDeserializingToObjectAsync<MissionDataWrapper>(new GetMissionDataFunction(), addr, null),
                "Call.getMissionData");
            cache.MissionTuples[addr] = wrap;
            return wrap;
        }

        private async Task<byte>                        GetRealtimeStatusCachedAsync(string addr, RpcCycleCache cache) {
            if (cache.RealtimeStatus.TryGetValue(addr, out var rt)) return rt;
            rt = await RunRpc(
                w => w.Eth.GetContractQueryHandler<GetRealtimeStatusFunction>()
                        .QueryAsync<byte>(addr, new GetRealtimeStatusFunction()),
                "Call.getRealtimeStatus");
            cache.RealtimeStatus[addr] = rt;
            return rt;
        }

        public                                          MissionIndexer              (ILogger<MissionIndexer> log, IConfiguration cfg) {
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

            // Read from configuration only (Key Vault/appsettings/env) — no hardcoded defaults.
            _rpc     = cfg["Cronos:Rpc"] 
                    ?? throw new InvalidOperationException("Missing configuration key: Cronos:Rpc");
            _factory = cfg["Contracts:Factory"] 
                    ?? throw new InvalidOperationException("Missing configuration key: Contracts:Factory");
            _pg      = cfg.GetConnectionString("Db") 
                    ?? throw new InvalidOperationException("Missing connection string: Db");

            _maxWinPerMission = 2;
            _maxWinTotal      = 200;

            // Circuit-breaker tuning (optional keys)
            _maxConsecErrors  = 5;
            _maxCircuitTrips  = 12;

            // NEW: optional deploy block (0 means disabled)
            _factoryDeployBlock = long.TryParse(cfg["Indexer:FactoryDeployBlock"], out var fb) ? fb : 0L;

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

            // --- push config (optional; if empty, pushing is disabled) ---
            _pushBase = cfg["Push:BaseUrl"] ?? "";
            _pushKey  = cfg["Push:Key"]     ?? "";
            _ownerPk  = cfg["Owner:PK"] ?? cfg["Owner--PK"] ?? string.Empty;
        }

        private async Task<long>                        GetLatestBlockAsync         () {
            var val = await RunRpc(async w => await w.Eth.Blocks.GetBlockNumber.SendRequestAsync(), "GetBlockNumber");
            return (long)val.Value;
        }

        private async Task                              PollRealtimeStatusesAsync   (CancellationToken token) {

            // 1) Load missions that need a realtime refresh:
            //    - Not ended (status < 5) → always poll
            //    - Ended (5 or 7) but realtime_status doesn't match yet → keep polling until it matches
            var missions = new List<string>();
            await using (var c = new NpgsqlConnection(_pg))
            {
                await c.OpenAsync(token);
                await using var cmd = new NpgsqlCommand(@"
                    select mission_address
                    from missions
                    where coalesce(status,0) < 5
                    or (coalesce(status,0) in (5,6,7) and coalesce(realtime_status, 0) not in (6,7))
                    order by mission_address;", c);
                await using var rd = await cmd.ExecuteReaderAsync(token);
                while (await rd.ReadAsync(token))
                    missions.Add((rd["mission_address"] as string ?? "").ToLowerInvariant());
            }
            if (missions.Count == 0) return;

            foreach (var a in missions)
            {
                var cycle = new RpcCycleCache();
                byte rt;
                try
                {
                    // 2) Ask chain: getRealtimeStatus()
                    rt = await GetRealtimeStatusCachedAsync(a, cycle);
                }
                catch
                {
                    continue; // skip this mission this round
                }

                // 3) Upsert realtime_status (optional)
                try
                {
                    await using var c = new NpgsqlConnection(_pg);
                    await c.OpenAsync(token);
                    await using var up = new NpgsqlCommand(@"
                        update missions
                        set realtime_status     = @rt,
                            realtime_checked_at = now(),
                            updated_at          = now()
                        where mission_address     = @a;", c);
                    up.Parameters.AddWithValue("a", a);
                    up.Parameters.AddWithValue("rt", (short)rt);
                    await up.ExecuteNonQueryAsync(token);
                }
                catch { /* ignore schema issues if you skipped the DDL */ }

                // 4) Triggers — rely on existing idempotent guards
                try
                {
                    if (rt == 5)
                    {
                        // Will re-check DB<5 and RT==5 inside NeedsAutoFinalizeAsync
                        await TryAutoFinalizeAsync(a, token, cachedRt: rt);
                    }
                    else if (rt == 7 && !await HasAnyRefundsRecordedAsync(a, token))
                    {
                        await TryAutoRefundAsync(a, token);        // uses RefundPlayersFunction
                    }

                    var potChanged   = await RefreshPotFromChainAsync(a, token, cycle);
                    var fixedPlayers = await ReconcileEnrollmentsFromChainAsync(a, token); // ← add this
                    if (potChanged || fixedPlayers)
                        await NotifyMissionUpdatedAsync(a, token);
                }
                catch (Exception ex)
                { 
                    _log.LogWarning(ex, (rt == 5 ? "TryAutoFinalizeAsync" : "TryAutoRefundAsync") + " failed for mission {mission}; Realtime status: {rt}", a, rt);
                }
            }
        }

        private async Task                              ScanFactoryEventsAsync      (CancellationToken token, long? latestOverride = null) {
            var cycle  = new RpcCycleCache();
            var latest = latestOverride ?? await GetLatestBlockCachedAsync(cycle);
            var toBlock = latest - REORG_CUSHION;
            if (toBlock <= 0) return;

            var cursor = await GetCursorAsync("factory", token);
            var start  = cursor + 1;
            if (start > toBlock) return;

            // declare events here (status/final were missing before)
            var createdEvt = _web3.Eth.GetEvent<MissionCreatedEventDTO>(_factory);
            var statusEvt  = _web3.Eth.GetEvent<MissionStatusUpdatedEventDTO>(_factory);
            var finalEvt   = _web3.Eth.GetEvent<MissionFinalizedEventDTO>(_factory);

            long windowFrom = start;
            while (windowFrom <= toBlock)
            {
                long windowTo       = Math.Min(windowFrom + MAX_LOG_RANGE, toBlock);
                var pushStatuses    = new List<(string addr, short toStatus)>();

                var from            = new BlockParameter(new HexBigInteger(windowFrom));
                var to              = new BlockParameter(new HexBigInteger(windowTo));

                // pull logs for this window with local benign retry before any switch
                List<EventLog<MissionCreatedEventDTO>>         createdLogs = default!;
                List<EventLog<MissionStatusUpdatedEventDTO>>   statusLogs  = default!;
                List<EventLog<MissionFinalizedEventDTO>>       finalLogs   = default!;

                for (var attempt = 0; attempt < 2; attempt++)
                {
                    try
                    {
                        createdLogs = await RunRpc(
                            w => {
                                var e = w.Eth.GetEvent<MissionCreatedEventDTO>(_factory);
                                return e.GetAllChangesAsync(e.CreateFilterInput(from, to));
                            },
                            "GetLogs.Factory.MissionCreated");

                        statusLogs = await RunRpc(
                            w => {
                                var e = w.Eth.GetEvent<MissionStatusUpdatedEventDTO>(_factory);
                                return e.GetAllChangesAsync(e.CreateFilterInput(from, to));
                            },
                            "GetLogs.Factory.StatusUpdated");

                        finalLogs = await RunRpc(
                            w => {
                                var e = w.Eth.GetEvent<MissionFinalizedEventDTO>(_factory);
                                return e.GetAllChangesAsync(e.CreateFilterInput(from, to));
                            },
                            "GetLogs.Factory.Finalized");

                        break; // success
                    }
                    catch (Exception ex) when (IsTransient(ex) && attempt == 0)
                    {
                        // Benign provider blip? -> count + brief delay + retry on same RPC, no warning
                        if (TryGetBenignProviderCode(ex, out var code))
                        {
                            NoteBenign("GetLogs.FactoryWindow", code);
                            try { await Task.Delay(_benignRetryDelay, token); } catch { }
                            continue;
                        }

                        // Otherwise switch once and retry (same behavior you use elsewhere)
                        if (SwitchRpc())
                        {
                            _log.LogWarning(ex, "Factory getLogs window {from}-{to} failed; switched RPC, retrying once", windowFrom, windowTo);
                            continue;
                        }

                        throw;
                    }
                }

                // 1) Seed unknown missions discovered via status/final (optional, same as before)
                var seen    = new HashSet<string>(StringComparer.InvariantCulture);
                var unknown = new HashSet<string>(StringComparer.InvariantCulture);
                foreach (var ev in statusLogs) { var a = (ev.Event.Mission ?? "").ToLower(CultureInfo.InvariantCulture); if (seen.Add(a)) unknown.Add(a); }
                foreach (var ev in finalLogs)  { var a = (ev.Event.Mission ?? "").ToLower(CultureInfo.InvariantCulture); if (seen.Add(a)) unknown.Add(a); }
                if (unknown.Count > 0)
                {
                    await using (var c = new NpgsqlConnection(_pg))
                    {
                        await c.OpenAsync(token);
                        var drop = new List<string>();
                        foreach (var a in unknown)
                        {
                            await using var chk = new NpgsqlCommand("select 1 from missions where mission_address = @a", c);
                            chk.Parameters.AddWithValue("a", a);
                            var has = await chk.ExecuteScalarAsync(token);
                            if (has != null) drop.Add(a);
                        }
                        foreach (var a in drop) unknown.Remove(a);
                    }

                    if (unknown.Count > 0)
                    {
                        // NEW: count these extra RPCs so pacing tightens accordingly
                        _pacer.Reserve(1 + unknown.Count);

                        var nameByAddr = new Dictionary<string,string>(StringComparer.InvariantCulture);
                        try
                        {
                            var all = await GetAllMissionsCachedAsync(cycle);

                            if (all?.Missions != null && all.Names != null)
                            {
                                for (int i = 0; i < all.Missions.Count && i < all.Names.Count; i++)
                                {
                                    var a = (all.Missions[i] ?? "").ToLower(CultureInfo.InvariantCulture);
                                    if (!string.IsNullOrEmpty(a)) nameByAddr[a] = all.Names[i] ?? "";
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            _log.LogWarning(ex, "getAllMissions failed; seeding without names in window {from}-{to}", windowFrom, windowTo);
                        }

                        foreach (var a in unknown)
                        {
                            long firstBlk = long.MaxValue;
                            foreach (var ev in statusLogs) if (((ev.Event.Mission ?? "").ToLower(CultureInfo.InvariantCulture)) == a) firstBlk = Math.Min(firstBlk, (long)ev.Log.BlockNumber.Value);
                            foreach (var ev in finalLogs)  if (((ev.Event.Mission ?? "").ToLower(CultureInfo.InvariantCulture)) == a) firstBlk = Math.Min(firstBlk, (long)ev.Log.BlockNumber.Value);
                            if (firstBlk == long.MaxValue) firstBlk = windowFrom;

                            var name = nameByAddr.TryGetValue(a, out var n) ? n : a;

                            try { await EnsureMissionSeededAsync(a, name, firstBlk - 1, token, alreadyFetchedThisCycle:false, cache:cycle); }
                            catch (Exception ex) { _log.LogError(ex, "EnsureMissionSeeded failed for {addr}", a); }
                        }
                    }
                }

                // 2) Compute timestamps once for this window (status/final)
                var statusBlocks = new HashSet<long>();
                foreach (var e in statusLogs) statusBlocks.Add((long)e.Log.BlockNumber.Value);
                foreach (var e in finalLogs)  statusBlocks.Add((long)e.Log.BlockNumber.Value);
                foreach (var e in createdLogs) statusBlocks.Add((long)e.Log.BlockNumber.Value);
                var tsByBlock = await GetBlockTimestampsAsync(statusBlocks);

                try 
                {
                    // 3) DB writes (single tx per window)
                    await using var conn = new NpgsqlConnection(_pg);
                    await conn.OpenAsync(token);
                    await using var tx = await conn.BeginTransactionAsync(token);

                    // Seed from MissionCreated (idempotent)
                    foreach (var ev in createdLogs)
                    {
                        var a   = (ev.Event.Mission ?? string.Empty).ToLowerInvariant();
                        var blk = (long)ev.Log.BlockNumber.Value;

                        MissionDataWrapper wrap;
                        try
                        {
                            wrap = await RunRpc(
                                w => w.Eth.GetContractQueryHandler<GetMissionDataFunction>()
                                        .QueryDeserializingToObjectAsync<MissionDataWrapper>(
                                            new GetMissionDataFunction(), a, null),
                                "Call.getMissionData");
                        }
                        catch (Exception ex)
                        {
                            _log.LogError(ex, "Factory seed: getMissionData failed for {addr}; skipping this event", a);
                            continue; // or: throw to let the window retry logic handle it
                        }
                        var md = wrap.Data;

                        await using var up = new NpgsqlCommand(@"
                            insert into missions (
                            mission_address, name, mission_type, status,
                            enrollment_start, enrollment_end, enrollment_amount_wei,
                            enrollment_min_players, enrollment_max_players, round_pause_secs, last_round_pause_secs,
                            mission_start, mission_end, mission_rounds_total, round_count,
                            cro_start_wei, cro_current_wei, cro_initial_wei, pause_timestamp, mission_created, last_seen_block, updated_at
                            ) values (
                            @a,@n,@ty,@st,
                            @es,@ee,@amt,
                            @min,@max,@rps,@lrps,
                            @ms,@me,@rt,0,
                            @cs,@cc,@ci,null,@mc,@blkMinus1, now()
                            )
                            on conflict (mission_address) do update set
                            name                    = coalesce(nullif(excluded.name,''), missions.name),
                            mission_type            = excluded.mission_type,
                            enrollment_start        = excluded.enrollment_start,
                            enrollment_end          = excluded.enrollment_end,
                            enrollment_amount_wei   = excluded.enrollment_amount_wei,
                            enrollment_min_players  = excluded.enrollment_min_players,
                            enrollment_max_players  = excluded.enrollment_max_players,
                            mission_start           = excluded.mission_start,
                            mission_end             = excluded.mission_end,
                            mission_rounds_total    = excluded.mission_rounds_total,

                            -- take chain values at creation; keep existing if already nonzero
                            cro_start_wei           = CASE WHEN coalesce(missions.cro_start_wei,0)=0 THEN excluded.cro_start_wei ELSE missions.cro_start_wei END,
                            cro_current_wei         = CASE WHEN coalesce(missions.cro_current_wei,0)=0 THEN excluded.cro_current_wei ELSE missions.cro_current_wei END,
                            cro_initial_wei         = CASE WHEN coalesce(missions.cro_initial_wei,0)=0 THEN excluded.cro_initial_wei ELSE missions.cro_initial_wei END,

                            mission_created         = excluded.mission_created,
                            last_seen_block         = greatest(coalesce(missions.last_seen_block,0), excluded.last_seen_block),
                            updated_at              = now();
                        ", conn, tx);

                        up.Parameters.AddWithValue("a",     a);
                        up.Parameters.AddWithValue("n",     ev.Event.Name ?? string.Empty);
                        up.Parameters.AddWithValue("ty",    (short)ev.Event.MissionType);
                        up.Parameters.AddWithValue("st", 0);
                        up.Parameters.AddWithValue("es",    (long)ev.Event.EnrollmentStart);
                        up.Parameters.AddWithValue("ee",    (long)ev.Event.EnrollmentEnd);
                        up.Parameters.Add("amt", NpgsqlDbType.Numeric).Value = ev.Event.EnrollmentAmount;
                        up.Parameters.AddWithValue("min",   (short)ev.Event.MinPlayers);
                        up.Parameters.AddWithValue("max",   (short)ev.Event.MaxPlayers);
                        up.Parameters.AddWithValue("rps",   (short)ev.Event.RoundPauseDuration);
                        up.Parameters.AddWithValue("lrps",  (short)ev.Event.LastRoundPauseDuration);
                        up.Parameters.AddWithValue("ms",    (long)ev.Event.MissionStart);
                        up.Parameters.AddWithValue("me",    (long)ev.Event.MissionEnd);
                        up.Parameters.AddWithValue("rt",    (short)ev.Event.MissionRounds);

                        // pull initial pool directly from getMissionData() we already queried above
                        up.Parameters.Add("cs", NpgsqlDbType.Numeric).Value = md.CroStart;
                        up.Parameters.Add("cc", NpgsqlDbType.Numeric).Value = md.CroCurrent;
                        up.Parameters.Add("ci", NpgsqlDbType.Numeric).Value = md.CroStart;

                        var createdUnix = tsByBlock.TryGetValue(blk, out var cr)
                            ? new DateTimeOffset(cr).ToUnixTimeSeconds()
                            : DateTimeOffset.UtcNow.ToUnixTimeSeconds();
                        up.Parameters.AddWithValue("mc",        createdUnix);
                        up.Parameters.AddWithValue("blkMinus1", blk - 1);

                        await up.ExecuteNonQueryAsync(token);
                    }

                    // Status updates (with chain time)
                    foreach (var ev in statusLogs)
                    {
                        var mission = (ev.Event.Mission ?? string.Empty).ToLower(CultureInfo.InvariantCulture);
                        short fromS = ev.Event.FromStatus;
                        short toS   = ev.Event.ToStatus;
                        long  blk   = (long)ev.Log.BlockNumber.Value;

                        int inserted;
                        await using (var h = new NpgsqlCommand(@"
                            insert into mission_status_history (mission_address, from_status, to_status, changed_at, block_number)
                            values (@a,@f,@t,@ca,@b)
                            on conflict do nothing;", conn, tx))
                        {
                            h.Parameters.AddWithValue("a",  mission);
                            h.Parameters.AddWithValue("f",  fromS);
                            h.Parameters.AddWithValue("t",  toS);
                            h.Parameters.AddWithValue("b",  blk);
                            h.Parameters.AddWithValue("ca", tsByBlock.TryGetValue(blk, out var ca) ? ca : DateTime.UtcNow);
                            inserted = await h.ExecuteNonQueryAsync(token);
                        }

                        await using (var up = new NpgsqlCommand(@"
                            update missions
                            set status          = @s,
                                realtime_status = case when @s in (6,7) then @s else realtime_status end,
                                last_seen_block = greatest(coalesce(last_seen_block,0), @toBlk),
                                updated_at      = now()
                            where mission_address = @a;", conn, tx))
                        {
                            up.Parameters.AddWithValue("a", mission);
                            up.Parameters.AddWithValue("s", toS);
                            up.Parameters.AddWithValue("toBlk", windowTo);   // safe tip for this factory window
                            await up.ExecuteNonQueryAsync(token);
                        }

                        if (inserted > 0) pushStatuses.Add((mission, toS));
                    }

                    // Finalized (same write; from=to; with chain time)
                    foreach (var ev in finalLogs)
                    {
                        var mission = (ev.Event.Mission ?? string.Empty).ToLower(CultureInfo.InvariantCulture);
                        short toS   = ev.Event.FinalStatus;
                        long  blk   = (long)ev.Log.BlockNumber.Value;

                        int inserted;
                        await using (var h = new NpgsqlCommand(@"
                            insert into mission_status_history (mission_address, from_status, to_status, changed_at, block_number)
                            values (@a,@f,@t,@ca,@b)
                            on conflict do nothing;", conn, tx))
                        {
                            h.Parameters.AddWithValue("a",  mission);
                            h.Parameters.AddWithValue("f",  (short)toS);
                            h.Parameters.AddWithValue("t",  (short)toS);
                            h.Parameters.AddWithValue("b",  blk);
                            h.Parameters.AddWithValue("ca", tsByBlock.TryGetValue(blk, out var ca) ? ca : DateTime.UtcNow);
                            inserted = await h.ExecuteNonQueryAsync(token);
                        }

                        await using (var up = new NpgsqlCommand(@"
                            update missions 
                            set status = @s, 
                                realtime_status = @s,         -- ← ensure realtime mirrors finalized
                                updated_at = now()
                            where mission_address = @a;", conn, tx))
                        {
                            up.Parameters.AddWithValue("a", mission);
                            up.Parameters.AddWithValue("s", toS);
                            await up.ExecuteNonQueryAsync(token);
                        }
                        if (inserted > 0) pushStatuses.Add((mission, toS));
                    }

                    // ======= CHANGED: retry logic without goto =======
                    for (var attempt = 0; attempt < 3; attempt++)
                    {
                        try
                        {
                            await tx.CommitAsync(token);

                            foreach (var (a, s) in pushStatuses)
                            {   
                                try { 
                                    await NotifyStatusAsync(a, s, token); 
                                } catch (Exception ex) {
                                    _log.LogWarning(ex, "Notify status failed for {addr}", a); 
                                }
                                if (s == 7) { 
                                    try { 
                                        await TryAutoRefundAsync(a, token); 
                                    } catch (Exception ex) {
                                        _log.LogWarning(ex, "Auto refund failed for {addr}", a); 
                                    }
                                }
                            }
                            pushStatuses.Clear();

                            await SetCursorAsync("factory", windowTo, token);
                            windowFrom = windowTo + 1;
                            break; // success
                        }
                        catch (OperationCanceledException) when (token.IsCancellationRequested)
                        {
                            _log.LogInformation("ScanFactoryEvents canceled by stop request for window {from}-{to}", windowFrom, windowTo);
                            return; // graceful stop – no error, no retry
                        }
                        catch (Exception ex) when (IsTransient(ex) && attempt == 0)
                        {
                            // Benign upstream blip? Count it and retry on the SAME RPC, no warning noise.
                            if (TryGetBenignProviderCode(ex, out var code))
                            {
                                NoteBenign("GetLogs.FactoryWindow", code);
                                try { await Task.Delay(_benignRetryDelay, token); } catch { }
                                continue;
                            }

                            // Otherwise: switch once and retry (previous behavior).
                            if (SwitchRpc())
                            {
                                _log.LogWarning(ex, "Factory window {from}-{to} failed; switched RPC, retrying once", windowFrom, windowTo);
                                continue;
                            }

                            throw; // couldn't switch; bubble up
                        }
                    }
                }
                catch (OperationCanceledException) when (token.IsCancellationRequested)
                {
                    _log.LogInformation("ScanFactoryEvents canceled while opening/tx for window {from}-{to}", windowFrom, windowTo);
                    return; // graceful stop during DB open/tx
                }    
            }
        }

        private async Task                              ScanMissionEventsAsync      (CancellationToken token, long? latestOverride = null) {
            // load missions + last_seen_block
            var missions = new List<(string addr, long fromBlock)>();
            var latest  = latestOverride ?? await GetLatestBlockAsync();
            var tipSafe = latest - REORG_CUSHION;
            if (tipSafe <= 0) return;

            await using (var conn = new NpgsqlConnection(_pg))
            {
                await conn.OpenAsync(token);
                await using var cmd = new NpgsqlCommand(@"
                    select mission_address, coalesce(last_seen_block,0) as from_block
                    from missions
                    where coalesce(last_seen_block,0) < @safeTip
                    and (coalesce(status,0) < 6 
                        or coalesce(realtime_status,0) not in (6,7))
                    order by mission_address;", conn);
                cmd.Parameters.AddWithValue("safeTip", tipSafe);                        // ← NEW
                await using var rd = await cmd.ExecuteReaderAsync(token);
                while (await rd.ReadAsync(token))
                {
                    var a = (rd["mission_address"] as string ?? "").ToLower(CultureInfo.InvariantCulture);
                    var f = (long)rd["from_block"];
                    missions.Add((a, f));
                }
            }
            if (missions.Count == 0) return;

            var windowsBudget = _maxWinTotal;

            foreach (var (addr, lastSeen) in missions)
            {
                if (windowsBudget <= 0) break;
                var start = Math.Max(0, lastSeen + 1);

                long windowFrom = start;
                int windowsDone = 0;
                while (windowFrom <= tipSafe && windowsDone < _maxWinPerMission && windowsBudget > 0)
                {
                    long windowTo = Math.Min(windowFrom + MAX_LOG_RANGE, tipSafe);

                    // collectors to push AFTER commit
                    var pushRounds   = new List<(short round, string winner, string amountWei)>();
                    var pushStatuses = new List<short>(); // mission-level status changes for this address

                    bool windowSucceeded = false; // track if we completed this window

                    // retry once on transient RPC errors (replacing goto retry_window)
                    for (var attempt = 0; attempt < 2; attempt++)
                    {
                        try
                        {
                            var from = new BlockParameter(new HexBigInteger(windowFrom));
                            var to   = new BlockParameter(new HexBigInteger(windowTo));

                            var statusEvt = _web3.Eth.GetEvent<MissionStatusChangedEventDTO>(addr);
                            var roundEvt  = _web3.Eth.GetEvent<RoundCalledEventDTO>         (addr);
                            var prEvt     = _web3.Eth.GetEvent<PlayerRefundedEventDTO>      (addr);
                            var mrEvt     = _web3.Eth.GetEvent<MissionRefundedEventDTO>     (addr);
                            var peEvt     = _web3.Eth.GetEvent<PlayerEnrolledEventDTO>      (addr);

                            List<EventLog<MissionStatusChangedEventDTO>> statusLogs = new();
                            List<EventLog<RoundCalledEventDTO>>          roundLogs  = new();
                            List<EventLog<PlayerRefundedEventDTO>>       prLogs     = new();
                            List<EventLog<PlayerEnrolledEventDTO>>       peLogs     = new();
                            List<EventLog<MissionRefundedEventDTO>>      mrLogs     = new();

                            var filter = new NewFilterInput
                            {
                                Address   = new[] { addr },
                                FromBlock = from,
                                ToBlock   = to,
                                Topics    = null // all topics; we’ll route by topic[0]
                            };

                            var rawLogs = await RunRpc(
                                w => w.Eth.Filters.GetLogs.SendRequestAsync(filter),
                                "GetLogs.Mission.WindowAll");

                            // Route raw logs to typed DTOs by the first topic (event signature)
                            var sigStatus = statusEvt.EventABI.Sha3Signature;    // topic[0]
                            var sigRound  = roundEvt.EventABI.Sha3Signature;
                            var sigPR     = prEvt.EventABI.Sha3Signature;
                            var sigMR     = mrEvt.EventABI.Sha3Signature;
                            var sigPE     = peEvt.EventABI.Sha3Signature;

                            foreach (var log in rawLogs ?? Array.Empty<FilterLog>())
                            {
                                var topic0 = log.Topics?.Length > 0 ? log.Topics[0] : null;

                                if (scanMissionStatus && string.Equals(topic0, sigStatus, StringComparison.OrdinalIgnoreCase))
                                {
                                    var dec = Event<MissionStatusChangedEventDTO>.DecodeEvent(log);
                                    if (dec != null) statusLogs.Add(dec);
                                }
                                else if (string.Equals(topic0, sigRound, StringComparison.OrdinalIgnoreCase))
                                {
                                    var dec = Event<RoundCalledEventDTO>.DecodeEvent(log);
                                    if (dec != null) roundLogs.Add(dec);
                                }
                                else if (string.Equals(topic0, sigPR, StringComparison.OrdinalIgnoreCase))
                                {
                                    var dec = Event<PlayerRefundedEventDTO>.DecodeEvent(log);
                                    if (dec != null) prLogs.Add(dec);
                                }
                                else if (string.Equals(topic0, sigPE, StringComparison.OrdinalIgnoreCase))
                                {
                                    var dec = Event<PlayerEnrolledEventDTO>.DecodeEvent(log);
                                    if (dec != null) peLogs.Add(dec);
                                }
                                else if (string.Equals(topic0, sigMR, StringComparison.OrdinalIgnoreCase))
                                {
                                    try
                                    {
                                        var dec = Event<MissionRefundedEventDTO>.DecodeEvent(log);
                                        if (dec != null) mrLogs.Add(dec);
                                    }
                                    catch (Exception ex) { _log.LogWarning(ex, "MissionRefunded decode failed for {addr}", addr); }
                                }

                            }

                            // Collect unique block numbers across this window’s logs
                            var blockNums = new HashSet<long>();
                            foreach (var ev in peLogs)     blockNums.Add((long)ev.Log.BlockNumber.Value);
                            foreach (var ev in roundLogs)  blockNums.Add((long)ev.Log.BlockNumber.Value);
                            foreach (var ev in statusLogs) blockNums.Add((long)ev.Log.BlockNumber.Value);
                            foreach (var ev in mrLogs)     blockNums.Add((long)ev.Log.BlockNumber.Value);
                            var tsByBlock = await GetBlockTimestampsAsync(blockNums);

                            await using var conn = new NpgsqlConnection(_pg);
                            await conn.OpenAsync(token);
                            await using var tx = await conn.BeginTransactionAsync(token);

                            // status history (mission-level; factory is canonical but we also keep these)
                            foreach (var ev in statusLogs)
                            {
                                short prev = ev.Event.PreviousStatus;
                                short next = ev.Event.NewStatus;
                                long  blk  = (long)ev.Log.BlockNumber.Value;

                                int inserted;
                                await using var h = new NpgsqlCommand(@"
                                    insert into mission_status_history (mission_address, from_status, to_status, changed_at, block_number)
                                    values (@a,@f,@t,@ca,@b)
                                    on conflict do nothing;", conn, tx);
                                h.Parameters.AddWithValue("a", addr);
                                h.Parameters.AddWithValue("f", prev);
                                h.Parameters.AddWithValue("t", next);
                                h.Parameters.AddWithValue("b", blk);
                                h.Parameters.AddWithValue("ca", tsByBlock.TryGetValue(blk, out var ca) ? ca : DateTime.UtcNow);
                                inserted = await h.ExecuteNonQueryAsync(token);

                                await using var up = new NpgsqlCommand(@"
                                    update missions set status = @s, updated_at = now()
                                    where mission_address = @a;", conn, tx);
                                up.Parameters.AddWithValue("a", addr);
                                up.Parameters.AddWithValue("s", next);
                                await up.ExecuteNonQueryAsync(token);
                                if (inserted > 0) pushStatuses.Add(next);
                            }

                            // rounds
                            short maxRound = 0;
                            foreach (var ev in roundLogs)
                            {
                                var r   = (short)ev.Event.RoundNumber;
                                var w   = (ev.Event.Player ?? "").ToLower(CultureInfo.InvariantCulture);
                                var blk = (long)ev.Log.BlockNumber.Value;
                                var txh = ev.Log.TransactionHash ?? "";
                                var amt = ev.Event.Payout;

                                await using var rr = new NpgsqlCommand(@"
                                    insert into mission_rounds (
                                        mission_address, round_number, winner_address, payout_wei, block_number, tx_hash, created_at
                                    ) values (@a,@no,@w,@amt,@b,@tx,@ca)
                                    on conflict (mission_address, round_number) do nothing;", conn, tx);
                                rr.Parameters.AddWithValue("a",  addr);
                                rr.Parameters.AddWithValue("no", r);
                                rr.Parameters.AddWithValue("w",  w);
                                rr.Parameters.Add("amt", NpgsqlDbType.Numeric).Value = amt;
                                rr.Parameters.AddWithValue("b",  blk);
                                rr.Parameters.AddWithValue("tx", string.IsNullOrEmpty(txh) ? (object)DBNull.Value : txh);
                                rr.Parameters.AddWithValue("ca", tsByBlock.TryGetValue(blk, out var cra) ? cra : DateTime.UtcNow);
                                var rows = await rr.ExecuteNonQueryAsync(token);

                                if (r > maxRound) maxRound = r;

                                // only push if this was a *new* DB row (avoid dup pushes on re-scan)
                                if (rows > 0)
                                {
                                    pushRounds.Add((r, w, amt.ToString()));

                                    // NEW: stamp pause_timestamp to the block time of this round
                                    var pauseUnix = tsByBlock.TryGetValue(blk, out var t)
                                        ? new DateTimeOffset(t).ToUnixTimeSeconds()
                                        : DateTimeOffset.UtcNow.ToUnixTimeSeconds();

                                    await using var upP = new NpgsqlCommand(@"
                                        update missions
                                        set pause_timestamp = @pt, updated_at = now()
                                        where mission_address = @a;", conn, tx);
                                    upP.Parameters.AddWithValue("a",  addr);
                                    upP.Parameters.AddWithValue("pt", pauseUnix);
                                    await upP.ExecuteNonQueryAsync(token);
                                }

                                // keep cro_current_wei in sync using croRemaining
                                await using var upC = new NpgsqlCommand(@"
                                    update missions set cro_current_wei = @c, updated_at = now()
                                    where mission_address = @a;", conn, tx);
                                upC.Parameters.Add("c", NpgsqlDbType.Numeric).Value = ev.Event.CroRemaining;
                                upC.Parameters.AddWithValue("a", addr);
                                await upC.ExecuteNonQueryAsync(token);
                            }

                            if (maxRound > 0)
                            {
                                await using var upR = new NpgsqlCommand(@"
                                    update missions set round_count = greatest(round_count, @rc), updated_at = now()
                                    where mission_address = @a;", conn, tx);
                                upR.Parameters.AddWithValue("a", addr);
                                upR.Parameters.AddWithValue("rc", maxRound);
                                await upR.ExecuteNonQueryAsync(token);
                            }

                            // refunds (single)
                            foreach (var ev in prLogs)
                            {
                                var p   = (ev.Event.Player ?? "").ToLower(CultureInfo.InvariantCulture);
                                var txh = ev.Log.TransactionHash ?? "";

                                await using var u = new NpgsqlCommand(@"
                                    insert into mission_enrollments (mission_address, player_address, refunded, refund_tx_hash)
                                    values (@a,@p, true, @tx)
                                    on conflict (mission_address, player_address) do update set
                                        refunded       = true,
                                        refund_tx_hash = excluded.refund_tx_hash;", conn, tx);
                                u.Parameters.AddWithValue("a",  addr);
                                u.Parameters.AddWithValue("p",  p);
                                u.Parameters.AddWithValue("tx", txh);
                                await u.ExecuteNonQueryAsync(token);
                            }

                            // refunds (batch)
                            foreach (var ev in mrLogs)
                            {
                                var txh = ev.Log.TransactionHash ?? "";
                                if (ev.Event.Players == null) continue;

                                foreach (var raw in ev.Event.Players)
                                {
                                    var p = (raw ?? "").ToLower(CultureInfo.InvariantCulture);
                                    await using var u = new NpgsqlCommand(@"
                                        insert into mission_enrollments (mission_address, player_address, refunded, refund_tx_hash)
                                        values (@a,@p, true, @tx)
                                        on conflict (mission_address, player_address) do update set
                                            refunded = true,
                                            refund_tx_hash = excluded.refund_tx_hash;", conn, tx);
                                    u.Parameters.AddWithValue("a",  addr);
                                    u.Parameters.AddWithValue("p",  p);
                                    u.Parameters.AddWithValue("tx", txh);
                                    await u.ExecuteNonQueryAsync(token);
                                }
                            }

                            bool missionUpdated = false;   // NEW: did we see any enrollment/pool change?

                            // enrollments
                            foreach (var ev in peLogs)
                            {
                                var p   = (ev.Event.Player ?? "").ToLower(CultureInfo.InvariantCulture);
                                var blk = (long)ev.Log.BlockNumber.Value;
                                var enrolledAtUtc = tsByBlock.TryGetValue(blk, out var t) ? t : DateTime.UtcNow;

                                // 1) insert enrollment (idempotent)
                                int rows;
                                await using (var ins = new NpgsqlCommand(@"
                                    insert into mission_enrollments (mission_address, player_address, enrolled_at, refunded, refund_tx_hash)
                                    values (@a, @p, @ea, false, null)
                                    on conflict (mission_address, player_address) do nothing;", conn, tx))
                                {
                                    ins.Parameters.AddWithValue("a",  addr);
                                    ins.Parameters.AddWithValue("p",  p);
                                    ins.Parameters.AddWithValue("ea", enrolledAtUtc);
                                    rows = await ins.ExecuteNonQueryAsync(token);
                                }
                                if (rows > 0) missionUpdated = true;    // NEW

                                // 2) bump CRO balances from the event amount
                                await using (var upCro = new NpgsqlCommand(@"
                                    update missions
                                    set cro_start_wei   = cro_start_wei   + @amt,
                                        cro_current_wei = cro_current_wei + @amt,
                                        updated_at      = now()
                                    where mission_address = @a;", conn, tx))
                                {
                                    upCro.Parameters.AddWithValue("a", addr);
                                    upCro.Parameters.Add("amt", NpgsqlDbType.Numeric).Value = ev.Event.Amount;
                                    var croRows = await upCro.ExecuteNonQueryAsync(token);
                                    if (croRows > 0) missionUpdated = true;   // NEW
                                }
                            }

                            // advance mission cursor to the end of this window
                            await using (var c = new NpgsqlCommand(@"
                                update missions set last_seen_block = @blk, updated_at = now()
                                where mission_address = @a;", conn, tx))
                            {
                                c.Parameters.AddWithValue("a", addr);
                                c.Parameters.AddWithValue("blk", windowTo);
                                await c.ExecuteNonQueryAsync(token);
                            }

                            await tx.CommitAsync(token);

                            // publish after DB commit (push-only)
                            try
                            {
                                foreach (var s in pushStatuses)
                                    await NotifyStatusAsync(addr, s, token);

                                foreach (var (r, w, a) in pushRounds)
                                    await NotifyRoundAsync(addr, r, w, a, token);

                                if (missionUpdated)
                                    await NotifyMissionUpdatedAsync(addr, token);
                            }
                            catch (Exception ex)
                            {
                                _log.LogWarning(ex, "Notify failed for {addr}", addr);
                            }

                            // chain-derived reconciles (quietly handle benign hiccups)
                            try
                            {
                                var cycle = new RpcCycleCache();
                                var fixedPlayers = await ReconcileEnrollmentsFromChainAsync(addr, token, cycle);
                                var potChanged   = await RefreshPotFromChainAsync(addr, token, cycle);
                                if ((potChanged || fixedPlayers) && !missionUpdated)
                                    await NotifyMissionUpdatedAsync(addr, token);
                            }
                            catch (Exception ex) when (IsTransient(ex) && TryGetBenignProviderCode(ex, out var code))
                            {
                                NoteBenign("eth_call.reconcile", code);
                                // no log; next cycle will catch up
                            }
                            catch (Exception ex)
                            {
                                _log.LogWarning(ex, "Reconcile-from-chain failed for {addr}", addr);
                            }

                            try { 
                                await TryAutoFinalizeAsync(addr, token); 
                            } catch (Exception ex) {
                                _log.LogWarning(ex, "Finalize mission failed for {addr}", addr); 
                            }

                            windowSucceeded = true;
                            break; // success for this window
                        }
                        catch (Exception ex) when (IsTransient(ex) && attempt == 0)
                        {
                            // Benign provider blip? -> count + brief delay + retry on same RPC, no warning
                            if (TryGetBenignProviderCode(ex, out var code))
                            {
                                NoteBenign("GetLogs.MissionWindow", code);
                                try { await Task.Delay(_benignRetryDelay, token); } catch { }
                                continue;
                            }

                            // Otherwise switch once and retry, as before
                            if (SwitchRpc())
                            {
                                _log.LogWarning(ex, "Mission {addr} window {from}-{to} failed; switched RPC, retrying once", addr, windowFrom, windowTo);
                                continue;
                            }

                            throw;
                        }
                        catch (OperationCanceledException) when (token.IsCancellationRequested)
                        {
                            _log.LogInformation("ScanMissionEvents canceled by stop request for {addr} in window {from}-{to}", addr, windowFrom, windowTo);
                            return;
                        }
                        catch (Exception ex)
                        {
                            _log.LogError(ex, "ScanMissionEvents failed for {addr} in window {from}-{to}", addr, windowFrom, windowTo);
                            RecordFailure(ex);
                            break;
                        }

                    }

                    if (!windowSucceeded)
                        break; // stop this mission, continue with others

                    windowFrom = windowTo + 1;
                    windowsDone++; 
                    windowsBudget--;  
                }
            }
        }

        private async Task                              EnsureMissionSeededAsync(string missionAddr, string name, long seedLastSeenBlock, CancellationToken token, bool alreadyFetchedThisCycle = false, RpcCycleCache? cache = null) {
            MissionDataWrapper wrap;

            if (alreadyFetchedThisCycle && cache != null && cache.MissionTuples.TryGetValue(missionAddr, out var cachedWrap))
            {
                wrap = cachedWrap;                                // NEW: reuse tuple fetched earlier this cycle
            }
            else if (cache != null)
            {
                wrap = await GetMissionDataCachedAsync(missionAddr, cache);   // NEW
            }
            else
            {
                wrap = await RunRpc(
                    w => w.Eth.GetContractQueryHandler<GetMissionDataFunction>()
                        .QueryDeserializingToObjectAsync<MissionDataWrapper>(
                            new GetMissionDataFunction(), missionAddr, null),
                    "Call.getMissionData");
            }

            var md = wrap.Data;

            await using var conn = new NpgsqlConnection(_pg);
            await conn.OpenAsync(token);
            await using var tx = await conn.BeginTransactionAsync(token);

            // upsert mission row (same mapping you used in the poller)
            await using (var up = new NpgsqlCommand(@"
                insert into missions (
                mission_address, name, mission_type, status,
                enrollment_start, enrollment_end, enrollment_amount_wei,
                enrollment_min_players, enrollment_max_players,
                mission_start, mission_end, mission_rounds_total, round_count,
                cro_start_wei, cro_current_wei, cro_initial_wei, pause_timestamp, mission_created, last_seen_block, updated_at
                ) values (
                @a,@n,@ty,@st,
                @es,@ee,@amt,
                @min,@max,
                @ms,@me,@rt,@rc,
                @cs,@cc,@ci,@pt,@mc,@blk, now()
                )
                on conflict (mission_address) do update set
                name                    = excluded.name,
                mission_type            = excluded.mission_type,
                status                  = excluded.status,
                enrollment_start        = excluded.enrollment_start,
                enrollment_end          = excluded.enrollment_end,
                enrollment_amount_wei   = excluded.enrollment_amount_wei,
                enrollment_min_players  = excluded.enrollment_min_players,
                enrollment_max_players  = excluded.enrollment_max_players,
                mission_start           = excluded.mission_start,
                mission_end             = excluded.mission_end,
                mission_rounds_total    = excluded.mission_rounds_total,
                round_count             = excluded.round_count,
                cro_start_wei           = excluded.cro_start_wei,
                cro_current_wei         = excluded.cro_current_wei,
                cro_initial_wei         = CASE WHEN coalesce(missions.cro_initial_wei,0)=0 THEN excluded.cro_initial_wei ELSE missions.cro_initial_wei END,
                pause_timestamp         = excluded.pause_timestamp,
                mission_created         = excluded.mission_created,
                last_seen_block         = GREATEST(COALESCE(missions.last_seen_block,0), excluded.last_seen_block),
                updated_at              = now();
            ", conn, tx))
            {
                up.Parameters.AddWithValue("a",  missionAddr);
                up.Parameters.AddWithValue("n",  string.IsNullOrEmpty(name) ? missionAddr : name);

                up.Parameters.AddWithValue("ty", (short)md.MissionType);
                up.Parameters.AddWithValue("st", (short)md.RoundCount); // initial status unknown here; roundCount is mapped elsewhere by events; keep status updated by factory events

                up.Parameters.AddWithValue("es", ToInt64(md.EnrollmentStart));
                up.Parameters.AddWithValue("ee", ToInt64(md.EnrollmentEnd));
                up.Parameters.Add("amt", NpgsqlDbType.Numeric).Value = md.EnrollmentAmount;

                up.Parameters.AddWithValue("min", (short)md.EnrollmentMinPlayers);
                up.Parameters.AddWithValue("max", (short)md.EnrollmentMaxPlayers);

                up.Parameters.AddWithValue("ms", ToInt64(md.MissionStart));
                up.Parameters.AddWithValue("me", ToInt64(md.MissionEnd));

                up.Parameters.AddWithValue("rt", (short)md.MissionRounds);
                up.Parameters.AddWithValue("rc", (short)md.RoundCount);

                up.Parameters.Add("cs", NpgsqlDbType.Numeric).Value = md.CroStart;
                up.Parameters.Add("cc", NpgsqlDbType.Numeric).Value = md.CroCurrent;
                up.Parameters.Add("ci", NpgsqlDbType.Numeric).Value = md.CroStart;

                up.Parameters.AddWithValue("pt",  ToInt64(md.PauseTimestamp));
                up.Parameters.AddWithValue("mc",  ToInt64(md.MissionCreated));
                up.Parameters.AddWithValue("blk", seedLastSeenBlock);

                await up.ExecuteNonQueryAsync(token);
            }

            // seed enrollments (idempotent; keeps enrolled_at if already set)
            if (md.Players.Count > 0)
            {
                var refunded = new HashSet<string>(md.RefundedPlayers.ConvertAll(
                    p => (p ?? "").ToLower(CultureInfo.InvariantCulture)));

                foreach (var p in md.Players)
                {
                    var pa = (p ?? "").ToLower(CultureInfo.InvariantCulture);
                    await using var en = new NpgsqlCommand(@"
                        insert into mission_enrollments (mission_address, player_address, enrolled_at, refunded)
                        values (@a,@p, now(), @r)
                        on conflict (mission_address, player_address) do update set
                            refunded    = excluded.refunded,
                            enrolled_at = COALESCE(mission_enrollments.enrolled_at, excluded.enrolled_at)
                    ", conn, tx);
                    en.Parameters.AddWithValue("a", missionAddr);
                    en.Parameters.AddWithValue("p", pa);
                    en.Parameters.AddWithValue("r", refunded.Contains(pa));
                    await en.ExecuteNonQueryAsync(token);
                }
            }

            // seed historical rounds present in the tuple (ON CONFLICT DO NOTHING)
            if (md.PlayersWon.Count > 0)
            {
                for (var i = 0; i < md.PlayersWon.Count; i++)
                {
                    var rw = md.PlayersWon[i];
                    await using var rr = new NpgsqlCommand(@"
                        insert into mission_rounds (
                            mission_address, round_number, winner_address, payout_wei, block_number
                        ) values (@a,@no,@w,@amt,@b)
                        on conflict (mission_address, round_number) do nothing
                    ", conn, tx);
                    rr.Parameters.AddWithValue("a",  missionAddr);
                    rr.Parameters.AddWithValue("no", (short)(i + 1));
                    rr.Parameters.AddWithValue("w",  (rw.Player ?? "").ToLower(CultureInfo.InvariantCulture));
                    rr.Parameters.Add("amt", NpgsqlDbType.Numeric).Value = rw.Amount;
                    rr.Parameters.AddWithValue("b",  seedLastSeenBlock); // best-effort block tag
                    await rr.ExecuteNonQueryAsync(token);
                }
            }

            await tx.CommitAsync(token);
        }

        private static long                             ToInt64                     (BigInteger v) {
            if (v < long.MinValue) return long.MinValue;
            if (v > long.MaxValue) return long.MaxValue;
            return (long)v;
        }

        private static string                           NormalizeKind               (string context) {
            if (string.IsNullOrWhiteSpace(context)) return "RPC";
            var p = context.IndexOf('(');
            return p > 0 ? context.Substring(0, p) : context;
        }

        private void                                    RpcFileLog                  (string kind, string line) {
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

        private void                                    UseRpc                      (int idx) {
            _rpcIndex = idx % _rpcEndpoints.Count;
            var url = _rpcEndpoints[_rpcIndex];
            _web3 = new Web3(url);
            _log.LogInformation("Using RPC[{idx}]: {url}", _rpcIndex, url);
        }

        private async Task<T>                           RunRpc<T>(Func<Web3, Task<T>> fn, string context, [CallerMemberName] string caller = "") {
            var kind = NormalizeKind(context);

            // Count this attempt (counts retries too, which reflects real request volume)
            NoteRpc(context, caller);

            // Evenly space this call within the current cycle
            await _pacer.GateAsync();

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

        private void                                    NoteRpc(string context, string caller) {
            var ctx = string.IsNullOrWhiteSpace(context) ? "RPC" : context;
            var who = string.IsNullOrWhiteSpace(caller)  ? "Unknown" : caller;

            lock (_rpcLogLock)
            {
                _rpc5mByContext[ctx] = _rpc5mByContext.TryGetValue(ctx, out var n) ? n + 1 : 1;

                if (!_rpc5mByCaller.TryGetValue(who, out var map))
                {
                    map = new Dictionary<string,int>(StringComparer.InvariantCulture);
                    _rpc5mByCaller[who] = map;
                }
                map[ctx] = map.TryGetValue(ctx, out var c) ? c + 1 : 1;
            }
        }

        private void                                    FlushRpcSummaryIfDue() {
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
                foreach (var kv in _rpc5mByCaller)
                    byCaller[kv.Key] = new Dictionary<string,int>(kv.Value, StringComparer.InvariantCulture);

                _rpc5mByContext.Clear();
                _rpc5mByCaller.Clear();

                _nextRpcSummaryUtc = now + _rpcSummaryPeriod;
            }

            foreach (var n in byCtx.Values) total += n;

            // Build compact messages
            string ctxPart = (byCtx.Count == 0)
                ? "none"
                : string.Join(", ", byCtx.OrderByDescending(kv => kv.Value)
                                        .Select(kv => $"{kv.Key}={kv.Value}"));

            string callerPart = (byCaller.Count == 0)
                ? "none"
                : string.Join(" | ", byCaller.OrderByDescending(kv => kv.Value.Values.Sum())
                                            .Select(kv =>
                                                $"{kv.Key}: " + string.Join(", ", kv.Value.OrderByDescending(x => x.Value)
                                                                                        .Select(x => $"{x.Key}={x.Value}"))));

            _log.LogWarning("RPC Summary (last 5m) total={total}; ByContext: {ctx}; ByCaller: {caller}", total, ctxPart, callerPart);
        }

        private bool                                    SwitchRpc                   () {
            if (_rpcEndpoints.Count <= 1) return false;
            var next = (_rpcIndex + 1) % _rpcEndpoints.Count;
            if (next == _rpcIndex) return false;
            var oldUrl = _rpcEndpoints[_rpcIndex];
            UseRpc(next);
            _log.LogWarning("Switched RPC from {old} to {nu}", oldUrl, _rpcEndpoints[_rpcIndex]);
            return true;
        }

        private void                                    RecordSuccess() {
            _consecErrors = 0;
        }

        private void                                    RecordFailure(Exception ex) {
            if (ex is OperationCanceledException) return; // normal during stop
            _consecErrors++;
            if (_consecErrors >= _maxConsecErrors)
            {
                _suspendUntilUtc = DateTime.UtcNow + _suspendFor;
                _circuitTrips++;
                _log.LogWarning("Circuit opened after {cnt} consecutive failures; suspending scans until {until:u} (trips={trips})",
                    _consecErrors, _suspendUntilUtc, _circuitTrips);
                _consecErrors = 0;

                // Optional hard escalation after many trips (lets Windows Service Recovery restart us)
                if (_circuitTrips >= _maxCircuitTrips)
                {
                    _log.LogError("Too many circuit trips ({trips}); terminating process to trigger service recovery.", _circuitTrips);
                    // Fail fast so the service is marked as failed (recovery will restart it)
                    Environment.FailFast("B6.Indexer circuit-breaker hard trip");
                }
            }
        }
    
        private static bool                             IsTransient                 (Exception ex) {
            return ex is Nethereum.JsonRpc.Client.RpcResponseException
                || ex is System.Net.Http.HttpRequestException
                || ex is TaskCanceledException
                || (ex.InnerException != null && IsTransient(ex.InnerException));
        }

        private static bool                             IsRateLimited              (Exception ex) {
            if (ex == null) return false;
            var msg = ex.Message ?? string.Empty;
            if (msg.IndexOf("429", StringComparison.OrdinalIgnoreCase) >= 0) return true;
            if (msg.IndexOf("Too Many Requests", StringComparison.OrdinalIgnoreCase) >= 0) return true;
            return ex.InnerException != null && IsRateLimited(ex.InnerException);
        }

        private static bool                             TryGetBenignProviderCode(Exception ex, out string code) {
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

        private void                                    NoteBenign(string kind, string code){
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

        private async Task<Dictionary<long, DateTime>>  GetBlockTimestampsAsync     (IEnumerable<long> blockNumbers) {
            var result = new Dictionary<long, DateTime>();
            var unique = new HashSet<long>(blockNumbers);
            var misses = new List<long>();

            // hit cache first
            foreach (var bn in unique)
            {
                if (_blockTsCache.TryGetValue(bn, out var cached))
                    result[bn] = cached;
                else
                    misses.Add(bn);
            }

            // fetch only misses
            _pacer.Reserve(misses.Count); // NEW: add to the plan for pacing
            foreach (var bn in misses)
            {
                try
                {
                    var block = await RunRpc(
                        w => w.Eth.Blocks.GetBlockWithTransactionsByNumber.SendRequestAsync(
                                new Nethereum.RPC.Eth.DTOs.BlockParameter(new Nethereum.Hex.HexTypes.HexBigInteger(bn))),
                        "GetBlock");

                    var unix = (long)block.Timestamp.Value;
                    var dt   = DateTimeOffset.FromUnixTimeSeconds(unix).UtcDateTime;
                    _blockTsCache[bn] = dt;  // cache
                    result[bn]        = dt;
                }
                catch (Exception ex)
                {
                    _log.LogWarning(ex, "Failed to fetch block timestamp for {bn}", bn);
                }
            }
            return result;
        }

        private async Task                              EnsureFactoryCursorMinAsync (long minLastBlock, CancellationToken token) {
            await using var conn = new NpgsqlConnection(_pg);
            await conn.OpenAsync(token);

            // Try read existing
            long? existing = null;
            await using (var read = new NpgsqlCommand("select last_block from indexer_cursors where cursor_key = 'factory'", conn))
            {
                var val = await read.ExecuteScalarAsync(token);
                if (val is long l) existing = l;
            }

            if (existing is null)
            {
                // No row yet → insert at minLastBlock
                await using var ins = new NpgsqlCommand(@"
                    insert into indexer_cursors (cursor_key, last_block)
                    values ('factory', @b)
                    on conflict (cursor_key) do nothing;", conn);
                ins.Parameters.AddWithValue("b", minLastBlock);
                await ins.ExecuteNonQueryAsync(token);
                _log.LogInformation("Factory cursor initialized to {block}", minLastBlock);
                return;
            }

            if (existing.Value < minLastBlock)
            {
                await SetCursorAsync("factory", minLastBlock, token);
                _log.LogInformation("Factory cursor raised from {old} to {new}", existing.Value, minLastBlock);
            }
            else
            {
                _log.LogDebug("Factory cursor kept at {old} (>= {min})", existing.Value, minLastBlock);
            }
        }

        protected override async Task                   ExecuteAsync                (CancellationToken token) {
            _log.LogInformation("Mission indexer started (events-only). Factory={factory} RPC={rpc}", _factory, _rpc);

            try
            {
                await EnsureCursorsTableAsync(token);

                // NEW: if configured, make sure factory cursor >= (deployBlock - 1)
                if (_factoryDeployBlock > 0)
                    await EnsureFactoryCursorMinAsync(_factoryDeployBlock - 1, token);

                // NEW: start a lightweight listener for DB kicks
                _ = Task.Run(() => ListenForKicksAsync(token), token);
            }
            catch (Exception ex)
            {
                _log.LogError(ex, "Failed to ensure/boostrap cursors");
                // Event tailing may be disabled if this fails.
            }

            while (!token.IsCancellationRequested)
            {
                var cycle = new RpcCycleCache();

                // If we recently tripped the circuit, idle briefly
                if (DateTime.UtcNow < _suspendUntilUtc)
                {
                    try { await Task.Delay(TimeSpan.FromSeconds(5), token); } catch { }
                    continue;
                }

                if (_kickRequested || !_kickMissions.IsEmpty)
                {
                    _kickRequested = false;

                    // In case NOTIFY failed, sweep pending rows too
                    await ProcessPendingKicksAsync(token);

                    try
                    {
                        var latest = await GetLatestBlockCachedAsync(cycle);
                        await ScanMissionEventsAsync(token, latestOverride: latest);
                        // (No need to force a specific mission; ScanMissionEventsAsync picks up any with work)
                    }
                    catch (Exception ex)
                    {
                        _log.LogDebug(ex, "Kick-triggered scan failed");
                    }
                }

                try
                {
                    // 0) Need latest first (counts depend on it)
                    var latest = await GetLatestBlockCachedAsync(cycle);

                    // 1) Count planned RPCs for this 15s cycle
                    var factoryWins  = await CountFactoryWindowsAsync(latest, token);
                    var missionWins  = await CountMissionWindowsAsync(latest, token);
                    var pollMissions = DateTime.UtcNow >= _nextRtPollUtc ? await CountRealtimePollMissionsAsync(token) : 0;

                    // Per window: Factory=3 logs, Mission=5 logs (StatusChanged enabled)
                    // Realtime poll (when due): ~3 per mission (getRealtimeStatus + getMissionData*2)
                    var planned = 1 /* we just did GetBlockNumber via GetLatestBlock... */
                                + factoryWins * 3
                                + missionWins * 5
                                + pollMissions * 3;

                    // 2) Pace all subsequent RPCs evenly across ~12 seconds
                    _pacer.Start(planned, TimeSpan.FromSeconds(12));

                    await ScanFactoryEventsAsync(token, latest);
                    await ScanMissionEventsAsync(token, latest);
                    RecordSuccess();
                }
                catch (OperationCanceledException) when (token.IsCancellationRequested)
                {
                    // Normal shutdown – do not treat as failure
                    break;
                }
                catch (Exception ex)
                {
                    _log.LogError(ex, "Sync cycle failed");
                    RecordFailure(ex);
                }

                try
                {
                    var nowUtc = DateTime.UtcNow;
                    if (nowUtc >= _nextRtPollUtc)
                    {
                        await PollRealtimeStatusesAsync(token);
                        _nextRtPollUtc = nowUtc + _rtPollPeriod;
                    }
                }
                catch (OperationCanceledException) when (token.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _log.LogError(ex, "Realtime status poll failed");
                    RecordFailure(ex);
                }
                FlushRpcSummaryIfDue();
                try { await Task.Delay(TimeSpan.FromSeconds(15), token); }
                catch { /* cancelled */ }
                FlushRpcSummaryIfDue();
                _log.LogInformation("Mission indexer stopping (cancellation requested).");
            }

        }

        private async Task                              EnsureCursorsTableAsync     (CancellationToken token) {
            await using var conn = new NpgsqlConnection(_pg);
            await conn.OpenAsync(token);

            const string sql = @"
            INSERT INTO indexer_cursors (cursor_key, last_block)
            VALUES ('factory', 0)
            ON CONFLICT (cursor_key) DO NOTHING;";

            await using var cmd = new NpgsqlCommand(sql, conn);
            await cmd.ExecuteNonQueryAsync(token);
        }

        private async Task<long>                        GetCursorAsync              (string key, CancellationToken token) {
            await using var conn = new NpgsqlConnection(_pg);
            await conn.OpenAsync(token);
            await using var cmd = new NpgsqlCommand(
                "select last_block from indexer_cursors where cursor_key = @k", conn);
            cmd.Parameters.AddWithValue("k", key);
            var val = await cmd.ExecuteScalarAsync(token);
            return val is long l ? l : 0L;
        }

        private async Task                              SetCursorAsync              (string key, long block, CancellationToken token) {
            await using var conn = new NpgsqlConnection(_pg);
            await conn.OpenAsync(token);
            await using var cmd = new NpgsqlCommand(@"
                insert into indexer_cursors(cursor_key, last_block, updated_at)
                values(@k, @b, now())
                on conflict (cursor_key) do update set last_block = excluded.last_block, updated_at = now();
            ", conn);
            cmd.Parameters.AddWithValue("k", key);
            cmd.Parameters.AddWithValue("b", block);
            await cmd.ExecuteNonQueryAsync(token);
        }

        private async Task                              NotifyStatusAsync           (string mission, short newStatus, CancellationToken ct = default) {
            if (string.IsNullOrEmpty(_pushBase) || string.IsNullOrEmpty(_pushKey)) return;
            using var req = new HttpRequestMessage(HttpMethod.Post, $"{_pushBase.TrimEnd('/')}/push/status")
            {
                Content = JsonContent.Create(new { Mission = mission, NewStatus = newStatus })
            };
            req.Headers.Add("X-Push-Key", _pushKey);
            try { await _http.SendAsync(req, ct); }
            catch (Exception ex) { _log.LogDebug(ex, "push/status failed for {mission}", mission); }
        }

        private async Task                              NotifyMissionUpdatedAsync   (string mission, CancellationToken ct = default) {
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

        private async Task                              NotifyRoundAsync            (string mission, short round, string winner, string amountWei, CancellationToken ct = default) {
            if (string.IsNullOrEmpty(_pushBase) || string.IsNullOrEmpty(_pushKey)) return;
            using var req = new HttpRequestMessage(HttpMethod.Post, $"{_pushBase.TrimEnd('/')}/push/round")
            {
                Content = JsonContent.Create(new { Mission = mission, Round = round, Winner = winner, AmountWei = amountWei })
            };
            req.Headers.Add("X-Push-Key", _pushKey);
            try { await _http.SendAsync(req, ct); }
            catch (Exception ex) { _log.LogDebug(ex, "push/round failed for {mission} r{round}", mission, round); }
        }

        private Web3?                                   BuildSignerWeb3             () {
            if (string.IsNullOrWhiteSpace(_ownerPk)) return null;
            var url = _rpcEndpoints[_rpcIndex];  // current active RPC
            var acct = new Account(_ownerPk);
            return new Web3(acct, url);
        }

        private async Task<bool>                        RefreshPotFromChainAsync(string mission, CancellationToken token, RpcCycleCache? cache = null) {
            var wrap = cache is null ? 
                await RunRpc(
                    w => w.Eth.GetContractQueryHandler<GetMissionDataFunction>()
                        .QueryDeserializingToObjectAsync<MissionDataWrapper>(
                            new GetMissionDataFunction(), mission, null),
                    "Call.getMissionData")
                : await GetMissionDataCachedAsync(mission, cache);
            var md = wrap.Data;

            // 2) Update only when changed (uses PostgreSQL IS DISTINCT FROM to avoid false positives)
            await using var c = new NpgsqlConnection(_pg);
            await c.OpenAsync(token);
            await using var up = new NpgsqlCommand(@"
                update missions
                set cro_start_wei   = @cs,
                    cro_current_wei = @cc,
                    updated_at      = now()
                where mission_address = @a
                and (cro_start_wei   is distinct from @cs
                    or cro_current_wei is distinct from @cc);", c);

            up.Parameters.AddWithValue("a", mission);
            up.Parameters.Add("cs", NpgsqlTypes.NpgsqlDbType.Numeric).Value = md.CroStart;
            up.Parameters.Add("cc", NpgsqlTypes.NpgsqlDbType.Numeric).Value = md.CroCurrent;

            var rows = await up.ExecuteNonQueryAsync(token);
            return rows > 0; // true → pot changed and DB was updated
        }

        private async Task<bool>                        ReconcileEnrollmentsFromChainAsync(string addr, CancellationToken token, RpcCycleCache? cache = null) {
            var wrap = cache is null ? 
                await RunRpc(
                    w => w.Eth.GetContractQueryHandler<GetMissionDataFunction>()
                            .QueryDeserializingToObjectAsync<MissionDataWrapper>(
                                new GetMissionDataFunction(), addr, null),
                    "Call.getMissionData")
                : await GetMissionDataCachedAsync(addr, cache);
            var md = wrap.Data;

            // build set of chain players (lowercased)
            var chain = new HashSet<string>(StringComparer.InvariantCulture);
            foreach (var p in md.Players)
            {
                var s = (p ?? "").ToLower(System.Globalization.CultureInfo.InvariantCulture);
                if (!string.IsNullOrEmpty(s)) chain.Add(s);
            }
            if (chain.Count == 0) return false;

            // read DB players
            var db = new HashSet<string>(StringComparer.InvariantCulture);
            await using (var c = new NpgsqlConnection(_pg))
            {
                await c.OpenAsync(token);
                await using var cmd = new NpgsqlCommand(
                    "select player_address from mission_enrollments where mission_address=@a;", c);
                cmd.Parameters.AddWithValue("a", addr);
                await using var rd = await cmd.ExecuteReaderAsync(token);
                while (await rd.ReadAsync(token))
                {
                    var s = (rd[0] as string ?? "").ToLower(System.Globalization.CultureInfo.InvariantCulture);
                    if (!string.IsNullOrEmpty(s)) db.Add(s);
                }
            }

            // insert missing
            var insertedAny = false;
            await using (var c2 = new NpgsqlConnection(_pg))
            {
                await c2.OpenAsync(token);
                await using var tx = await c2.BeginTransactionAsync(token);
                foreach (var p in chain)
                {
                    if (db.Contains(p)) continue;
                    await using var ins = new NpgsqlCommand(@"
                        insert into mission_enrollments
                            (mission_address, player_address, enrolled_at, refunded, refund_tx_hash)
                        values (@a, @p, now(), false, null)
                        on conflict (mission_address, player_address) do nothing;", c2, tx);
                    ins.Parameters.AddWithValue("a", addr);
                    ins.Parameters.AddWithValue("p", p);
                    var rows = await ins.ExecuteNonQueryAsync(token);
                    if (rows > 0) insertedAny = true;
                }
                await tx.CommitAsync(token);
            }
            return insertedAny;
        }

        private async Task<bool>                        HasAnyRefundsRecordedAsync  (string mission, CancellationToken token) {
            await using var c = new NpgsqlConnection(_pg);
            await c.OpenAsync(token);
            await using var cmd = new NpgsqlCommand(
                "select 1 from mission_enrollments where mission_address=@a and refunded = true limit 1;", c);
            cmd.Parameters.AddWithValue("a", mission);
            var v = await cmd.ExecuteScalarAsync(token);
            _log.LogInformation("HasAnyRefundsRecordedAsync - {mission} - Refunded?: {v}", mission, v != null);
            return v != null;
        }

        private async Task                              TryAutoRefundAsync          (string mission, CancellationToken token) {
            _log.LogInformation("TryAutoRefundAsync - {mission}", mission);
            if (string.IsNullOrWhiteSpace(_ownerPk)) {
                _log.LogWarning("Auto-refund skipped for {mission}: missing Owner--PK/Owner:PK in configuration", mission);
                return;
            }

            // Quick idempotency guard: if DB already has any refund rows, skip.
            bool hasRefunds;
            try
            {
                hasRefunds = await HasAnyRefundsRecordedAsync(mission, token);
            }
            catch (Exception ex)
            {
                _log.LogWarning(ex, "Auto-refund precheck failed for {mission}; skipping this round", mission);
                return;
            }
            if (hasRefunds) {
                _log.LogInformation("Auto-refund skipped for {mission}: refunds already recorded", mission);
                return;
            }

            var w3 = BuildSignerWeb3();
            if (w3 == null) return;

            try
            {
                var handler = w3.Eth.GetContractTransactionHandler<RefundPlayersFunction>();
                var receipt = await handler.SendRequestAndWaitForReceiptAsync(mission, new RefundPlayersFunction());
                _log.LogInformation("refundPlayers() sent for {mission}. Tx: {tx} Status: {st}",
                    mission, receipt?.TransactionHash, receipt?.Status?.Value);
            }
            catch (Exception ex)
            {
                _log.LogError(ex, "refundPlayers() failed for {mission}", mission);
                // Best-effort: the scanner loop / RT poll will retry later.
            }
        }

       private async Task<bool>                         NeedsAutoFinalizeAsync(string mission, CancellationToken token, byte? cachedRt = null) {

            // 1) DB guard: skip only if already fully final (status in 6,7)
            short? dbStatus = null;
            await using (var c = new NpgsqlConnection(_pg))
            {
                await c.OpenAsync(token);
                const string sql = @"select status from missions where mission_address=@a limit 1;";
                await using var cmd = new NpgsqlCommand(sql, c);
                cmd.Parameters.AddWithValue("a", mission);
                var v = await cmd.ExecuteScalarAsync(token);
                if (v is short s) dbStatus = s;
            }
            // If DB already fully final or refunded, no need to finalize.
            if (dbStatus == 6 || dbStatus == 7) return false;

            // 2) finalize iff getRealtimeStatus() reports PartlySuccess (5).
            var rt = cachedRt ?? await RunRpc(
                w => w.Eth.GetContractQueryHandler<GetRealtimeStatusFunction>()
                        .QueryAsync<byte>(mission, new GetRealtimeStatusFunction()),
                "Call.getRealtimeStatus");

            return rt == 5; // PartlySuccess
        }


        private async Task                              TryAutoFinalizeAsync(string mission, CancellationToken token, byte? cachedRt = null) {
            _log.LogInformation("TryAutoFinalizeAsync - {mission}", mission);
            if (string.IsNullOrWhiteSpace(_ownerPk)) {
                _log.LogDebug("Auto-finalize skipped for {mission}: missing Owner--PK/Owner:PK", mission);
                return;
            }

            bool shouldFinalize;
            try
            {
                shouldFinalize = await NeedsAutoFinalizeAsync(mission, token, cachedRt);
            }
            catch (Exception ex)
            {
                _log.LogWarning(ex, "Auto-finalize precheck failed for {mission}; skipping this round", mission);
                return;
            }
            if (!shouldFinalize) return;

            var w3 = BuildSignerWeb3();
            if (w3 == null) return;

            try
            {
                var handler = w3.Eth.GetContractTransactionHandler<ForceFinalizeMissionFunction>();
                var receipt = await handler.SendRequestAndWaitForReceiptAsync(mission, new ForceFinalizeMissionFunction());
                _log.LogInformation("forceFinalizeMission() sent for {mission}. Tx: {tx} Status: {st}",
                    mission, receipt?.TransactionHash, receipt?.Status?.Value);
            }
            catch (Exception ex)
            {
                _log.LogError(ex, "forceFinalizeMission() failed for {mission}", mission);
                // best-effort; loop/RT poll will retry later.
            }
        }

        private async Task                              ListenForKicksAsync         (CancellationToken token) {
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

        private async Task                              ProcessPendingKicksAsync    (CancellationToken token) {
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

    }
}

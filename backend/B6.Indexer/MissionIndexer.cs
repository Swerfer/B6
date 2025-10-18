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
        private static readonly bool                    logRpcCalls         = false;                                        // ← toggle RPC file logging
        // --------------------------------------------------------------------------------------------------------------
        private readonly ILogger<MissionIndexer>        _log;                                                               // injected
        private readonly string                         _rpc;                                                               // primary RPC endpoint
        private readonly string                         _factory;                                                           // MissionFactory contract address              
        private readonly string                         _pg;                                                                // Postgres connection string                                  
        private Web3                                    _web3               = default!;                                     // current RPC client
        private readonly List<string>                   _rpcEndpoints       = new();                                        // pool of RPC endpoints
        private int                                     _rpcIndex           = 0;                                            // current RPC endpoint index
        private readonly HttpClient                     _http               = new HttpClient();                             // for push notifications
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
        private readonly string                         _ownerPk;                                                           // from Key Vault 

        private static readonly TimeSpan                RATE_LIMIT_COOLDOWN        = TimeSpan.FromSeconds(30);              // wait after 429 before retrying

        private volatile bool _kickRequested = false;                                                                       // set by listener
        private readonly System.Collections.Concurrent.ConcurrentQueue<string> _kickMissions = new();                       // mission addresses to refresh

        private DateTime _factorySweepNextUtc;                                                                              // getMissionsNotEnded every 1 minute
        private DateTime _enrollTickNextUtc;                                                                                // enrollment-open missions, 1 minute
        private DateTime _activeTickNextUtc;                                                                                // active missions, 5 seconds
        private DateTime _finalizingNextUtc;                                                                                // ended-but-not-finalized, 1 minute

        private static readonly TimeSpan PendingArmingBuffer = TimeSpan.FromSeconds(2);                                     // -2s before enrollmentStart/missionStart
        private static readonly TimeSpan FactorySweep        = TimeSpan.FromMinutes(1);                                     // every 1 minute                     
        private static readonly TimeSpan EnrollPoll          = TimeSpan.FromMinutes(1);                                     // every 1 minute
        private static readonly TimeSpan ActivePoll          = TimeSpan.FromSeconds(5);                                     // every 5 seconds
        private static readonly TimeSpan FinalizingPoll      = TimeSpan.FromMinutes(1);                                     // every 1 minute
        // --------------------------------------------------------------------------------------------------------------

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


        private sealed class                            RpcCyclePacer   {
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

            // Read from configuration only (Key Vault/appsettings/env) — no hardcoded defaults.
            _rpc     = cfg["Cronos:Rpc"] 
                    ?? throw new InvalidOperationException("Missing configuration key: Cronos:Rpc");
            _factory = cfg["Contracts:Factory"] 
                    ?? throw new InvalidOperationException("Missing configuration key: Contracts:Factory");
            _pg      = cfg.GetConnectionString("Db") 
                    ?? throw new InvalidOperationException("Missing connection string: Db");

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

            await using (var conn = new NpgsqlConnection(_pg))
            {
                await conn.OpenAsync(token);

                // Load current
                await using (var read = new NpgsqlCommand(@"
                    select status, round_count, name, mission_type, coalesce(finalized,false)
                    from missions where mission_address = @a;", conn))
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
                    }
                }

                // Derive newStatus from schedule windows (snapshot is the source of truth)
                // (0..7 mapping should match your existing usage; finalized when now >= mission_end and payouts settled)
                var now = DateTime.UtcNow;
                var es  = FromUnix(md.EnrollmentStart);
                var ee  = FromUnix(md.EnrollmentEnd);
                var ms  = FromUnix(md.MissionStart);
                var me  = FromUnix(md.MissionEnd);

                if (now < es)           newStatus = 0; // Pending
                else if (now < ee)      newStatus = 1; // Enrollment
                else if (now < ms)      newStatus = 2; // Arming
                else if (now < me)      newStatus = 3; // Active
                else                    newStatus = 5; // Ended/Finalizing (5..7 range in your system)

                oldStatus = curStatus;
                oldRound  = curRound;

                // Update missions row from snapshot
                await using (var upd = new NpgsqlCommand(@"
                    update missions
                    set
                        name                     = coalesce(@nm, name),
                        mission_type             = @mt,
                        status                   = @st,
                        enrollment_start         = @es,
                        enrollment_end           = @ee,
                        enrollment_amount_wei    = @ea,
                        enrollment_min_players   = @emin,
                        enrollment_max_players   = @emax,
                        mission_start            = @ms,
                        mission_end              = @me,
                        mission_rounds_total     = @rt,
                        round_count              = @rc,
                        cro_initial_wei          = @ci,
                        cro_start_wei            = @cs,
                        cro_current_wei          = @cc,
                        pause_timestamp          = @pt,
                        updated_at               = now(),
                        mission_created          = @mc,
                        round_pause_secs         = @rpd,
                        last_round_pause_secs    = @lrpd,
                        creator_address          = @cr,
                        all_refunded             = @ar
                    where mission_address = @a;", conn))
                {
                    upd.Parameters.AddWithValue("a", mission);
                    upd.Parameters.AddWithValue("nm", (object?)md.Name ?? DBNull.Value);
                    upd.Parameters.AddWithValue("mt", md.MissionType);
                    upd.Parameters.AddWithValue("st", newStatus ?? (object)DBNull.Value);
                    upd.Parameters.AddWithValue("es", md.EnrollmentStart);
                    upd.Parameters.AddWithValue("ee", md.EnrollmentEnd);
                    upd.Parameters.AddWithValue("ea", md.EnrollmentAmount);
                    upd.Parameters.AddWithValue("emin", md.EnrollmentMinPlayers);
                    upd.Parameters.AddWithValue("emax", md.EnrollmentMaxPlayers);
                    upd.Parameters.AddWithValue("ms", md.MissionStart);
                    upd.Parameters.AddWithValue("me", md.MissionEnd);
                    upd.Parameters.AddWithValue("rt", (short)md.MissionRounds);
                    upd.Parameters.AddWithValue("rc", (short)md.RoundCount);
                    upd.Parameters.AddWithValue("ci", md.CroInitial);
                    upd.Parameters.AddWithValue("cs", md.CroStart);
                    upd.Parameters.AddWithValue("cc", md.CroCurrent);
                    upd.Parameters.AddWithValue("pt", md.PauseTimestamp == 0 ? (object)DBNull.Value : (object)md.PauseTimestamp);
                    upd.Parameters.AddWithValue("mc", md.MissionCreated);
                    upd.Parameters.AddWithValue("rpd", md.RoundPauseDuration);
                    upd.Parameters.AddWithValue("lrpd", md.LastRoundPauseDuration);
                    upd.Parameters.AddWithValue("cr", (object?)md.Creator?.ToLowerInvariant() ?? DBNull.Value);
                    upd.Parameters.AddWithValue("ar", md.AllRefunded);
                    await upd.ExecuteNonQueryAsync(token);
                }

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
                    changes.NewRound = newRound ?? 0;     // ← assign to the nullable short
                    changes.HasMeaningfulChange = true;
                }

                // If ended and snapshot indicates finality, you can set finalized = true here as appropriate.
                if (now >= me && !curFinal)
                {
                    await using var fin = new NpgsqlCommand(@"
                        update missions set finalized = true, updated_at = now()
                        where mission_address = @a;", conn);
                    fin.Parameters.AddWithValue("a", mission);
                    await fin.ExecuteNonQueryAsync(token);
                }
            }

            return changes;
        }

        private static string                           NormalizeKind                       (string context) {
            if (string.IsNullOrWhiteSpace(context)) return "RPC";
            var p = context.IndexOf('(');
            return p > 0 ? context.Substring(0, p) : context;
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

        private void                                    UseRpc                              (int idx) {
            _rpcIndex = idx % _rpcEndpoints.Count;
            var url = _rpcEndpoints[_rpcIndex];
            _web3 = new Web3(url);
            //_log.LogInformation("Using RPC[{idx}]: {url}", _rpcIndex, url);
        }

        private async Task<T>                           RunRpc<T>                           (Func<Web3, Task<T>> fn, string context, [CallerMemberName] string caller = "") {
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

        private bool                                    SwitchRpc                           () {
            if (_rpcEndpoints.Count <= 1) return false;
            var next = (_rpcIndex + 1) % _rpcEndpoints.Count;
            if (next == _rpcIndex) return false;
            var oldUrl = _rpcEndpoints[_rpcIndex];
            UseRpc(next);
            _log.LogWarning("Switched RPC from {old} to {nu}", oldUrl, _rpcEndpoints[_rpcIndex]);
            return true;
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

        protected override async Task                   ExecuteAsync                        (CancellationToken token) {
            // Snapshot-based indexer with state-aware cadence and POST kicks
            try
            {
                // Keep kick listener (DB NOTIFY + fallback queue)
                _ = Task.Run(() => ListenForKicksAsync(token), token);

                // Warm-up: run the sweeps immediately
                _factorySweepNextUtc = DateTime.UtcNow;
                _enrollTickNextUtc   = DateTime.UtcNow.Add(EnrollPoll);
                _activeTickNextUtc   = DateTime.UtcNow;
                _finalizingNextUtc   = DateTime.UtcNow.Add(FinalizingPoll);
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

                    // 1) Factory sweep: discover not-ended missions every 1 minute
                    if (now >= _factorySweepNextUtc)
                    {
                        await RefreshFactorySweepAsync(token);  // seeds/updates using getMissionsNotEnded + per-mission snapshots
                        _factorySweepNextUtc = now.Add(FactorySweep);
                    }

                    // 2) Pending: no scans until enrollmentStart − 2s (handled via boundary crossing helper)
                    await RefreshPendingBufferCrossingsAsync(token);

                    // 3) Enrollment: light sweep every 1 minute (plus instant kicks after enroll POST)
                    if (now >= _enrollTickNextUtc)
                    {
                        await RefreshEnrollmentMissionsAsync(token);
                        _enrollTickNextUtc = now.Add(EnrollPoll);
                    }

                    // 4) Arming: no scans until missionStart − 2s
                    await RefreshArmingBufferCrossingsAsync(token);

                    // 5) Active: poll every 5 seconds
                    if (now >= _activeTickNextUtc)
                    {
                        await RefreshActiveMissionsAsync(token);
                        _activeTickNextUtc = now.Add(ActivePoll);
                    }

                    // 6) Finalizing: poll every 1 minute until finalized
                    if (now >= _finalizingNextUtc)
                    {
                        await RefreshFinalizingMissionsAsync(token);
                        _finalizingNextUtc = now.Add(FinalizingPoll);
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

        private async Task                              ProcessKickQueueAsync               (CancellationToken token) {
            var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            while (_kickMissions.TryDequeue(out var mission))
            {
                if (string.IsNullOrWhiteSpace(mission)) continue;
                if (!seen.Add(mission)) continue;

                try
                {
                    await RefreshMissionSnapshotAsync(mission, token);
                }
                catch (Exception ex)
                {
                    _log.LogDebug(ex, "Kick refresh failed for {mission}", mission);
                }
            }
        }

        private async Task                              RefreshFactorySweepAsync            (CancellationToken token) {
            // Query not-ended missions (cheap, compared to logs)
            var output = await RunRpc(
                w => w.Eth.GetContractQueryHandler<B6.Contracts.GetMissionsNotEndedFunction>()
                    .QueryDeserializingToObjectAsync<B6.Contracts.GetMissionsOutput>(
                        new B6.Contracts.GetMissionsNotEndedFunction(), _factory, null),
                "Call.getMissionsNotEnded");

            foreach (var addr in output.Missions)
            {
                var mission = addr?.ToLowerInvariant();
                if (string.IsNullOrEmpty(mission)) continue;

                try
                {
                    await EnsureMissionRowAsync(mission, token);        // insert-if-missing using your existing helper
                    await RefreshMissionSnapshotAsync(mission, token);  // single source of truth
                }
                catch (Exception ex)
                {
                    _log.LogDebug(ex, "Factory sweep refresh failed for {mission}", mission);
                }
            }
        }

        private async Task                              RefreshPendingBufferCrossingsAsync  (CancellationToken token) {
            // Missions with now < enrollment_start and (enrollment_start - now) <= 2s
            var list = await GetPendingMissionsAsync(PendingArmingBuffer, token);
            foreach (var m in list) await RefreshMissionSnapshotAsync(m, token);
        }

        private async Task                              RefreshEnrollmentMissionsAsync      (CancellationToken token) {
            // Missions with now in [enrollment_start, enrollment_end)
            var list = await GetEnrollingMissionsAsync(token);
            foreach (var m in list) await RefreshMissionSnapshotAsync(m, token);
        }

        private async Task                              RefreshArmingBufferCrossingsAsync   (CancellationToken token) {
            // Missions with now < mission_start and (mission_start - now) <= 2s
            var list = await GetArmingMissionsAsync(PendingArmingBuffer, token);
            foreach (var m in list) await RefreshMissionSnapshotAsync(m, token);
        }

        private async Task                              RefreshActiveMissionsAsync          (CancellationToken token) {
            // Missions with now in [mission_start, mission_end)
            var list = await GetActiveMissionsAsync(token);
            foreach (var m in list) await RefreshMissionSnapshotAsync(m, token);
        }

        private async Task                              RefreshFinalizingMissionsAsync      (CancellationToken token) {
            // Missions with now >= mission_end AND finalized = false
            var list = await GetFinalizingMissionsAsync(token);
            foreach (var m in list) await RefreshMissionSnapshotAsync(m, token);
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
                    string amountWei = "0";
                    try
                    {
                        var ix = changes.NewRound.Value - 1;
                        if (ix >= 0 && ix < md.PlayersWon.Count)
                        {
                            var rw = md.PlayersWon[ix];
                            winner    = (rw.Player ?? "").ToLower(System.Globalization.CultureInfo.InvariantCulture);
                            amountWei = (rw.Amount  ).ToString(System.Globalization.CultureInfo.InvariantCulture);
                        }
                    }
                    catch { /* best effort */ }

                    await NotifyRoundAsync(mission, changes.NewRound.Value, winner, amountWei, token);
                }
            }

        }

        private async Task<List<string>>                GetPendingMissionsAsync             (TimeSpan buffer, CancellationToken token) {
            var result = new List<string>();
            await using var conn = new NpgsqlConnection(_pg);
            await conn.OpenAsync(token);
            await using var cmd = new NpgsqlCommand(@"
                select mission_address
                from missions
                where now() < enrollment_start
                and (enrollment_start - now()) <= @buf
                and coalesce(finalized, false) = false;", conn);
            cmd.Parameters.AddWithValue("buf", NpgsqlDbType.Interval, buffer);
            await using var rdr = await cmd.ExecuteReaderAsync(token);
            while (await rdr.ReadAsync(token)) result.Add(rdr.GetString(0));
            return result;
        }

        private async Task<List<string>>                GetEnrollingMissionsAsync           (CancellationToken token) {
            var result = new List<string>();
            await using var conn = new NpgsqlConnection(_pg);
            await conn.OpenAsync(token);
            await using var cmd = new NpgsqlCommand(@"
                select mission_address
                from missions
                where now() >= enrollment_start
                and now() <  enrollment_end
                and coalesce(finalized, false) = false;", conn);
            await using var rdr = await cmd.ExecuteReaderAsync(token);
            while (await rdr.ReadAsync(token)) result.Add(rdr.GetString(0));
            return result;
        }

        private async Task<List<string>>                GetArmingMissionsAsync              (TimeSpan buffer, CancellationToken token) {
            var result = new List<string>();
            await using var conn = new NpgsqlConnection(_pg);
            await conn.OpenAsync(token);
            await using var cmd = new NpgsqlCommand(@"
                select mission_address
                from missions
                where now() < mission_start
                and (mission_start - now()) <= @buf
                and coalesce(finalized, false) = false;", conn);
            cmd.Parameters.AddWithValue("buf", NpgsqlDbType.Interval, buffer);
            await using var rdr = await cmd.ExecuteReaderAsync(token);
            while (await rdr.ReadAsync(token)) result.Add(rdr.GetString(0));
            return result;
        }

        private async Task<List<string>>                GetActiveMissionsAsync              (CancellationToken token) {
            var result = new List<string>();
            await using var conn = new NpgsqlConnection(_pg);
            await conn.OpenAsync(token);
            await using var cmd = new NpgsqlCommand(@"
                select mission_address
                from missions
                where now() >= mission_start
                and now() <  mission_end
                and coalesce(finalized, false) = false;", conn);
            await using var rdr = await cmd.ExecuteReaderAsync(token);
            while (await rdr.ReadAsync(token)) result.Add(rdr.GetString(0));
            return result;
        }

        private async Task<List<string>>                GetFinalizingMissionsAsync          (CancellationToken token) {
            var result = new List<string>();
            await using var conn = new NpgsqlConnection(_pg);
            await conn.OpenAsync(token);
            await using var cmd = new NpgsqlCommand(@"
                select mission_address
                from missions
                where now() >= mission_end
                and coalesce(finalized, false) = false;", conn);
            await using var rdr = await cmd.ExecuteReaderAsync(token);
            while (await rdr.ReadAsync(token)) result.Add(rdr.GetString(0));
            return result;
        }

        private async Task                              EnsureMissionRowAsync               (string mission, CancellationToken ct) {
            await using var conn = new NpgsqlConnection(_pg);
            await conn.OpenAsync(ct);
            await using var cmd = new NpgsqlCommand(@"
                insert into missions (mission_address, created_at, updated_at)
                values (@a, now(), now())
                on conflict (mission_address) do nothing;", conn);
            cmd.Parameters.AddWithValue("a", mission);
            await cmd.ExecuteNonQueryAsync(ct);
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

    }
}

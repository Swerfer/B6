using System;
using System.Numerics;
using System.Globalization;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using System.Net.Http;        
using System.Net.Http.Json; 
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Nethereum.Web3;
using Nethereum.Contracts;
using Nethereum.ABI.FunctionEncoding.Attributes;
using Nethereum.RPC.Eth.DTOs;
using Nethereum.Hex.HexTypes;
using Npgsql;
using NpgsqlTypes;

using B6.Contracts;


namespace B6.Indexer
{
    public class MissionIndexer : BackgroundService
    {
        private readonly ILogger<MissionIndexer>        _log;
        private readonly string                         _rpc;
        private readonly string                         _factory;
        private readonly string                         _pg;
        private Web3                                    _web3;
        private readonly long                           _factoryDeployBlock;
        private readonly List<string>                   _rpcEndpoints = new();
        private int                                     _rpcIndex = 0;
        private const int                               REORG_CUSHION = 3;
        private const int                               MAX_LOG_RANGE = 1800;
        private readonly HttpClient                     _http = new HttpClient();
        private readonly string                         _pushBase;   // e.g. https://b6missions.com/api
        private readonly string                         _pushKey;

        public                                          MissionIndexer          (ILogger<MissionIndexer> log, IConfiguration cfg) {
            _log = log;

            // Read from configuration only (Key Vault/appsettings/env) — no hardcoded defaults.
            _rpc     = cfg["Cronos:Rpc"] 
                    ?? throw new InvalidOperationException("Missing configuration key: Cronos:Rpc");
            _factory = cfg["Contracts:Factory"] 
                    ?? throw new InvalidOperationException("Missing configuration key: Contracts:Factory");
            _pg      = cfg.GetConnectionString("Db") 
                    ?? throw new InvalidOperationException("Missing connection string: Db");

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

            // --- push config (optional; if empty, pushing is disabled) ---
            _pushBase = cfg["Push:BaseUrl"] ?? "";
            _pushKey  = cfg["Push:Key"]     ?? "";

        }

        private async Task<long>                        GetLatestBlockAsync() {
            var val = await RunRpc(async w => await w.Eth.Blocks.GetBlockNumber.SendRequestAsync(), "GetBlockNumber");
            return (long)val.Value;
        }

        private async Task                              ScanFactoryEventsAsync(CancellationToken token) {
            var latest  = await GetLatestBlockAsync();
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

                var createdLogs     = await createdEvt.GetAllChangesAsync(createdEvt.CreateFilterInput(from, to));
                var statusLogs      = await statusEvt .GetAllChangesAsync(statusEvt .CreateFilterInput(from, to));
                var finalLogs       = await finalEvt  .GetAllChangesAsync(finalEvt  .CreateFilterInput(from, to));

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
                        var nameByAddr = new Dictionary<string,string>(StringComparer.InvariantCulture);
                        try
                        {
                            var all = await _web3.Eth.GetContractQueryHandler<GetAllMissionsFunction>()
                                .QueryDeserializingToObjectAsync<GetMissionsOutput>(new GetAllMissionsFunction(), _factory, null)
                                .ConfigureAwait(false);

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

                            try { await EnsureMissionSeededAsync(a, name, firstBlk - 1, token); }
                            catch (Exception ex) { _log.LogError(ex, "EnsureMissionSeeded failed for {addr}", a); }
                        }
                    }
                }

                // 2) Compute timestamps once for this window (status/final)
                var statusBlocks = new HashSet<long>();
                foreach (var e in statusLogs) statusBlocks.Add((long)e.Log.BlockNumber.Value);
                foreach (var e in finalLogs)  statusBlocks.Add((long)e.Log.BlockNumber.Value);
                var tsByBlock = await GetBlockTimestampsAsync(statusBlocks);

                // 3) DB writes (single tx per window)
                await using var conn = new NpgsqlConnection(_pg);
                await conn.OpenAsync(token);
                await using var tx = await conn.BeginTransactionAsync(token);

                // Seed from MissionCreated (idempotent)
                foreach (var ev in createdLogs)
                {
                    var a   = (ev.Event.Mission ?? string.Empty).ToLowerInvariant();
                    var blk = (long)ev.Log.BlockNumber.Value;

                    await using var up = new NpgsqlCommand(@"
                        insert into missions (
                        mission_address, name, mission_type, status,
                        enrollment_start, enrollment_end, enrollment_amount_wei,
                        enrollment_min_players, enrollment_max_players,
                        mission_start, mission_end, mission_rounds_total, round_count,
                        cro_start_wei, cro_current_wei, pause_timestamp, last_seen_block, updated_at
                        ) values (
                        @a,@n,@ty,@st,
                        @es,@ee,@amt,
                        @min,@max,
                        @ms,@me,@rt,0,
                        0,0,null,@blkMinus1, now()
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
                        last_seen_block         = greatest(coalesce(missions.last_seen_block,0), excluded.last_seen_block),
                        updated_at              = now();
                    ", conn, tx);
                    up.Parameters.AddWithValue("a",  a);
                    up.Parameters.AddWithValue("n",  ev.Event.Name ?? string.Empty);
                    up.Parameters.AddWithValue("ty", (short)ev.Event.MissionType);
                    up.Parameters.AddWithValue("st", 0);
                    up.Parameters.AddWithValue("es", (long)ev.Event.EnrollmentStart);
                    up.Parameters.AddWithValue("ee", (long)ev.Event.EnrollmentEnd);
                    up.Parameters.Add("amt", NpgsqlDbType.Numeric).Value = ev.Event.EnrollmentAmount;
                    up.Parameters.AddWithValue("min", (short)ev.Event.MinPlayers);
                    up.Parameters.AddWithValue("max", (short)ev.Event.MaxPlayers);
                    up.Parameters.AddWithValue("ms", (long)ev.Event.MissionStart);
                    up.Parameters.AddWithValue("me", (long)ev.Event.MissionEnd);
                    up.Parameters.AddWithValue("rt", (short)ev.Event.MissionRounds);
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
                        update missions set status = @s, updated_at = now()
                        where mission_address = @a;", conn, tx))
                    {
                        up.Parameters.AddWithValue("a", mission);
                        up.Parameters.AddWithValue("s", toS);
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
                        update missions set status = @s, updated_at = now()
                        where mission_address = @a;", conn, tx))
                    {
                        up.Parameters.AddWithValue("a", mission);
                        up.Parameters.AddWithValue("s", toS);
                        await up.ExecuteNonQueryAsync(token);
                    }
                    if (inserted > 0) pushStatuses.Add((mission, toS));
                }

                bool retried = false;

                retry_window:
                try
                {
                    // ... create from/to, fetch logs, compute tsByBlock, write DB ...
                    await tx.CommitAsync(token);

                    foreach (var (a, s) in pushStatuses)
                    {
                        try { await NotifyStatusAsync(a, s, token); } catch { /* best-effort */ }
                    }
                    pushStatuses.Clear();

                    // advance cursor per window and continue
                    await SetCursorAsync("factory", windowTo, token);
                    windowFrom = windowTo + 1;
                }
                catch (Exception ex) when (IsTransient(ex) && !retried && SwitchRpc())
                {
                    retried = true;
                    _log.LogWarning(ex, "Factory window {from}-{to} failed; switched RPC, retrying once", windowFrom, windowTo);
                    goto retry_window;
                }
            }
        }

        private async Task                              ScanMissionEventsAsync(CancellationToken token) {
            // load missions + last_seen_block
            var missions = new List<(string addr, long fromBlock)>();
            await using (var conn = new NpgsqlConnection(_pg))
            {
                await conn.OpenAsync(token);
                await using var cmd = new NpgsqlCommand(@"
                    select mission_address, coalesce(last_seen_block,0) as from_block
                    from missions
                    order by mission_address;", conn);
                await using var rd = await cmd.ExecuteReaderAsync(token);
                while (await rd.ReadAsync(token))
                {
                    var a = (rd["mission_address"] as string ?? "").ToLower(CultureInfo.InvariantCulture);
                    var f = (long)rd["from_block"];
                    missions.Add((a, f));
                }
            }
            if (missions.Count == 0) return;

            var latest  = await GetLatestBlockAsync();
            var tipSafe = latest - REORG_CUSHION;
            if (tipSafe <= 0) return;

            foreach (var (addr, lastSeen) in missions)
            {
                var start = Math.Max(0, lastSeen + 1);
                if (start > tipSafe) continue;

                long windowFrom = start;
                while (windowFrom <= tipSafe)
                {
                    long windowTo = Math.Min(windowFrom + MAX_LOG_RANGE, tipSafe);

                    bool retried = false;

                    // collectors to push AFTER commit
                    var pushRounds   = new List<(short round, string winner, string amountWei)>();
                    var pushStatuses = new List<short>(); // mission-level status changes for this address

                    retry_window:
                    try
                    {
                        var from = new BlockParameter(new HexBigInteger(windowFrom));
                        var to   = new BlockParameter(new HexBigInteger(windowTo));

                        var statusEvt = _web3.Eth.GetEvent<MissionStatusChangedEventDTO>(addr);
                        var roundEvt  = _web3.Eth.GetEvent<RoundCalledEventDTO>         (addr);
                        var prEvt     = _web3.Eth.GetEvent<PlayerRefundedEventDTO>      (addr);
                        var mrEvt     = _web3.Eth.GetEvent<MissionRefundedEventDTO>     (addr);
                        var peEvt     = _web3.Eth.GetEvent<PlayerEnrolledEventDTO>      (addr);

                        var statusLogs = await statusEvt.GetAllChangesAsync(statusEvt.CreateFilterInput(from, to));
                        var roundLogs  = await roundEvt .GetAllChangesAsync(roundEvt .CreateFilterInput(from, to));
                        var prLogs     = await prEvt    .GetAllChangesAsync(prEvt    .CreateFilterInput(from, to));
                        var peLogs     = await peEvt    .GetAllChangesAsync(peEvt    .CreateFilterInput(from, to));

                        List<EventLog<MissionRefundedEventDTO>> mrLogs = new();
                        try { mrLogs = await mrEvt.GetAllChangesAsync(mrEvt.CreateFilterInput(from, to)); }
                        catch (Exception ex) { _log.LogWarning(ex, "MissionRefunded decode failed for {addr}", addr); }

                        // Collect unique block numbers across this window’s logs
                        var blockNums = new HashSet<long>();
                        foreach (var ev in peLogs)     blockNums.Add((long)ev.Log.BlockNumber.Value);
                        foreach (var ev in roundLogs)  blockNums.Add((long)ev.Log.BlockNumber.Value);
                        foreach (var ev in statusLogs) blockNums.Add((long)ev.Log.BlockNumber.Value);
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
                                    refunded = true,
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

                        // enrollments
                        foreach (var ev in peLogs)
                        {
                            var p   = (ev.Event.Player ?? "").ToLower(CultureInfo.InvariantCulture);
                            var blk = (long)ev.Log.BlockNumber.Value;
                            var enrolledAtUtc = tsByBlock.TryGetValue(blk, out var t) ? t : DateTime.UtcNow;

                            // 1) insert enrollment (idempotent)
                            await using (var ins = new NpgsqlCommand(@"
                                insert into mission_enrollments (mission_address, player_address, enrolled_at, refunded, refund_tx_hash)
                                values (@a, @p, @ea, false, null)
                                on conflict (mission_address, player_address) do nothing;", conn, tx))
                            {
                                ins.Parameters.AddWithValue("a",  addr);
                                ins.Parameters.AddWithValue("p",  p);
                                ins.Parameters.AddWithValue("ea", enrolledAtUtc);
                                await ins.ExecuteNonQueryAsync(token);
                            }

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
                                await upCro.ExecuteNonQueryAsync(token);
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

                        // publish after DB commit
                        try
                        {
                            foreach (var s in pushStatuses)
                                await NotifyStatusAsync(addr, s, token);

                            foreach (var (r, w, a) in pushRounds)
                                await NotifyRoundAsync(addr, r, w, a, token);
                        }
                        catch { /* best-effort */ }
                    }
                    catch (Exception ex) when (IsTransient(ex) && !retried && SwitchRpc())
                    {
                        retried = true;
                        _log.LogWarning(ex, "Mission {addr} window {from}-{to} failed; switched RPC, retrying once", addr, windowFrom, windowTo);
                        goto retry_window;
                    }
                    catch (Exception ex)
                    {
                        _log.LogError(ex, "ScanMissionEvents failed for {addr} in window {from}-{to}", addr, windowFrom, windowTo);
                        break; // stop this mission, continue with others
                    }

                    windowFrom = windowTo + 1;
                }
            }
        }

        private async Task                              EnsureMissionSeededAsync(string missionAddr, string name, long seedLastSeenBlock, CancellationToken token) {
            // call mission.getMissionData (single tuple wrapper)
            var wrap = await _web3.Eth
                .GetContractQueryHandler<GetMissionDataFunction>()
                .QueryDeserializingToObjectAsync<MissionDataWrapper>(
                    new GetMissionDataFunction(),
                    missionAddr,
                    null)
                .ConfigureAwait(false);

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
                cro_start_wei, cro_current_wei, pause_timestamp, last_seen_block, updated_at
                ) values (
                @a,@n,@ty,@st,
                @es,@ee,@amt,
                @min,@max,
                @ms,@me,@rt,@rc,
                @cs,@cc,@pt,@blk, now()
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
                pause_timestamp         = excluded.pause_timestamp,
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

                up.Parameters.Add("cs", NpgsqlDbType.Numeric).Value = md.EthStart;
                up.Parameters.Add("cc", NpgsqlDbType.Numeric).Value = md.EthCurrent;

                up.Parameters.AddWithValue("pt",  ToInt64(md.PauseTimestamp));
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

        private static long                             ToInt64                 (BigInteger v) {
            if (v < long.MinValue) return long.MinValue;
            if (v > long.MaxValue) return long.MaxValue;
            return (long)v;
        }

        private void                                    UseRpc(int idx) {
            _rpcIndex = idx % _rpcEndpoints.Count;
            var url = _rpcEndpoints[_rpcIndex];
            _web3 = new Web3(url);
            _log.LogInformation("Using RPC[{idx}]: {url}", _rpcIndex, url);
        }

        private bool                                    SwitchRpc() {
            if (_rpcEndpoints.Count <= 1) return false;
            var next = (_rpcIndex + 1) % _rpcEndpoints.Count;
            if (next == _rpcIndex) return false;
            var oldUrl = _rpcEndpoints[_rpcIndex];
            UseRpc(next);
            _log.LogWarning("Switched RPC from {old} to {nu}", oldUrl, _rpcEndpoints[_rpcIndex]);
            return true;
        }

        private static bool                             IsTransient(Exception ex) {
            return ex is Nethereum.JsonRpc.Client.RpcResponseException
                || ex is System.Net.Http.HttpRequestException
                || ex is TaskCanceledException
                || (ex.InnerException != null && IsTransient(ex.InnerException));
        }

        private async Task<T>                           RunRpc<T>(Func<Web3, Task<T>> fn, string context) {
            try
            {
                return await fn(_web3);
            }
            catch (Exception ex) when (IsTransient(ex) && SwitchRpc())
            {
                _log.LogWarning(ex, "{ctx} failed; switched RPC and retrying", context);
                return await fn(_web3);
            }
        }

        private async Task<Dictionary<long, DateTime>>  GetBlockTimestampsAsync(IEnumerable<long> blockNumbers) {
            var result = new Dictionary<long, DateTime>();
            foreach (var bn in new HashSet<long>(blockNumbers))
            {
                try
                {
                    var block = await RunRpc(
                        w => w.Eth.Blocks.GetBlockWithTransactionsByNumber.SendRequestAsync(
                                new Nethereum.RPC.Eth.DTOs.BlockParameter(new Nethereum.Hex.HexTypes.HexBigInteger(bn))),
                        $"GetBlock({bn})");

                    var unix = (long)block.Timestamp.Value;
                    result[bn] = DateTimeOffset.FromUnixTimeSeconds(unix).UtcDateTime;
                }
                catch (Exception ex)
                {
                    _log.LogWarning(ex, "Failed to fetch block timestamp for {bn}", bn);
                }
            }
            return result;
        }

        private async Task                              EnsureFactoryCursorMinAsync(long minLastBlock, CancellationToken token) {
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

        protected override async Task                   ExecuteAsync(CancellationToken token) {
            _log.LogInformation("Mission indexer started (events-only). Factory={factory} RPC={rpc}", _factory, _rpc);

            try
            {
                await EnsureCursorsTableAsync(token);

                // NEW: if configured, make sure factory cursor >= (deployBlock - 1)
                if (_factoryDeployBlock > 0)
                    await EnsureFactoryCursorMinAsync(_factoryDeployBlock - 1, token);
            }
            catch (Exception ex)
            {
                _log.LogError(ex, "Failed to ensure/boostrap cursors");
                // Event tailing may be disabled if this fails.
            }

            while (!token.IsCancellationRequested)
            {
                try
                {
                    await ScanFactoryEventsAsync(token);
                    await ScanMissionEventsAsync(token);
                }
                catch (Exception ex)
                {
                    _log.LogError(ex, "Sync cycle failed");
                }

                try { await Task.Delay(TimeSpan.FromSeconds(15), token); }
                catch { /* cancelled */ }
            }

        }

        private async Task                              EnsureCursorsTableAsync (CancellationToken token) {
            await using var conn = new NpgsqlConnection(_pg);
            await conn.OpenAsync(token);

            const string sql = @"
            CREATE TABLE IF NOT EXISTS indexer_cursors (
            cursor_key     TEXT PRIMARY KEY,
            last_block     BIGINT NOT NULL,
            updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
            );
            INSERT INTO indexer_cursors (cursor_key, last_block)
            VALUES ('factory', 0)
            ON CONFLICT (cursor_key) DO NOTHING;";

            await using var cmd = new NpgsqlCommand(sql, conn);
            await cmd.ExecuteNonQueryAsync(token);
        }

        private async Task<long>                        GetCursorAsync          (string key, CancellationToken token) {
            await using var conn = new NpgsqlConnection(_pg);
            await conn.OpenAsync(token);
            await using var cmd = new NpgsqlCommand(
                "select last_block from indexer_cursors where cursor_key = @k", conn);
            cmd.Parameters.AddWithValue("k", key);
            var val = await cmd.ExecuteScalarAsync(token);
            return val is long l ? l : 0L;
        }

        private async Task                              SetCursorAsync          (string key, long block, CancellationToken token) {
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

        private async Task NotifyStatusAsync(string mission, short newStatus, CancellationToken ct = default) {
            if (string.IsNullOrEmpty(_pushBase) || string.IsNullOrEmpty(_pushKey)) return;
            using var req = new HttpRequestMessage(HttpMethod.Post, $"{_pushBase.TrimEnd('/')}/push/status")
            {
                Content = JsonContent.Create(new { Mission = mission, NewStatus = newStatus })
            };
            req.Headers.Add("X-Push-Key", _pushKey);
            try { await _http.SendAsync(req, ct); }
            catch (Exception ex) { _log.LogDebug(ex, "push/status failed for {mission}", mission); }
        }

        private async Task NotifyRoundAsync(string mission, short round, string winner, string amountWei, CancellationToken ct = default) {
            if (string.IsNullOrEmpty(_pushBase) || string.IsNullOrEmpty(_pushKey)) return;
            using var req = new HttpRequestMessage(HttpMethod.Post, $"{_pushBase.TrimEnd('/')}/push/round")
            {
                Content = JsonContent.Create(new { Mission = mission, Round = round, Winner = winner, AmountWei = amountWei })
            };
            req.Headers.Add("X-Push-Key", _pushKey);
            try { await _http.SendAsync(req, ct); }
            catch (Exception ex) { _log.LogDebug(ex, "push/round failed for {mission} r{round}", mission, round); }
        }

    }
}

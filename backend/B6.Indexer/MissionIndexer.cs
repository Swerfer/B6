using System;
using System.Numerics;
using System.Globalization;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Nethereum.Web3;
using Nethereum.Contracts;
using Npgsql;
using NpgsqlTypes;

using B6.Contracts;

namespace B6.Indexer
{
    public class MissionIndexer : BackgroundService
    {
        private readonly ILogger<MissionIndexer> _log;
        private readonly string _rpc;
        private readonly string _factory;
        private readonly string _pg;
        private readonly Web3 _web3;

        public MissionIndexer(ILogger<MissionIndexer> log, IConfiguration cfg)
        {
            _log = log;

            // Read from configuration only (Key Vault/appsettings/env) — no hardcoded defaults.
            _rpc     = cfg["Cronos:Rpc"] 
                    ?? throw new InvalidOperationException("Missing configuration key: Cronos:Rpc");
            _factory = cfg["Contracts:Factory"] 
                    ?? throw new InvalidOperationException("Missing configuration key: Contracts:Factory");
            _pg      = cfg.GetConnectionString("Db") 
                    ?? throw new InvalidOperationException("Missing connection string: Db");

            // Normalize addresses
            _factory = _factory.ToLowerInvariant();

            _web3 = new Web3(_rpc);
        }

        // ---- safe numeric downcasts for DB columns ----
        private static long  ToInt64(BigInteger v)
        {
            if (v < long.MinValue) return long.MinValue;
            if (v > long.MaxValue) return long.MaxValue;
            return (long)v;
        }

        protected override async Task ExecuteAsync(CancellationToken token)
        {
            _log.LogInformation("Mission indexer started. Factory={factory} RPC={rpc}", _factory, _rpc);

            while (!token.IsCancellationRequested)
            {
                try
                {
                    _log.LogInformation("Indexer tick...");
                    await SyncNotEndedMissions(token);
                }
                catch (Exception ex)
                {
                    _log.LogError(ex, "Sync cycle failed");
                }

                try { await Task.Delay(TimeSpan.FromSeconds(15), token); }
                catch { /* cancelled */ }
            }
        }

        private async Task SyncNotEndedMissions(CancellationToken token)
        {
            var handler = _web3.Eth.GetContractQueryHandler<GetMissionsNotEndedFunction>();

            var result = await handler
                .QueryDeserializingToObjectAsync<GetMissionsOutput>(
                    new GetMissionsNotEndedFunction(),
                    _factory,
                    null)
                .ConfigureAwait(false);

            var blockHex     = await _web3.Eth.Blocks.GetBlockNumber.SendRequestAsync();
            var currentBlock = (long)blockHex.Value;

            if (result?.Missions == null || result.Missions.Count == 0)
            {
                _log.LogInformation("getMissionsNotEnded -> 0 at block {blk}", currentBlock);
                return;
            }

            _log.LogInformation("getMissionsNotEnded -> {cnt} at block {blk}", result.Missions.Count, currentBlock);

            await using var conn = new NpgsqlConnection(_pg);
            await conn.OpenAsync(token);

            for (var idx = 0; idx < result.Missions.Count; idx++)
            {
                var addr        = result.Missions[idx] ?? "";
                var missionAddr = addr.ToLower(CultureInfo.InvariantCulture);
                var status      = (short) (result.Statuses[idx] > short.MaxValue ? short.MaxValue : (int)result.Statuses[idx]);
                var name        = result.Names[idx] ?? "";

                try
                {
                    var wrap = await _web3.Eth
                        .GetContractQueryHandler<GetMissionDataFunction>()
                        .QueryDeserializingToObjectAsync<MissionDataWrapper>(
                            new GetMissionDataFunction(),
                            missionAddr,
                            null)
                        .ConfigureAwait(false);

                    var md = wrap.Data;

                    await using var tx = await conn.BeginTransactionAsync(token);

                    // previous status (if any)
                    short? previousStatus = null;
                    await using (var cmd = new NpgsqlCommand(
                        "select status from missions where mission_address = @a", conn, tx))
                    {
                        cmd.Parameters.AddWithValue("a", missionAddr);
                        var val = await cmd.ExecuteScalarAsync(token);
                        if (val is short s) previousStatus = s;
                        else if (val is int i) previousStatus = (short)i;
                    }
                    if (previousStatus.HasValue && previousStatus.Value != status)
                    {
                        await using var h = new NpgsqlCommand(@"
                            insert into mission_status_history (mission_address, from_status, to_status, block_number)
                            values (@a,@f,@t,@b)", conn, tx);
                        h.Parameters.AddWithValue("a", missionAddr);
                        h.Parameters.AddWithValue("f", previousStatus.Value);
                        h.Parameters.AddWithValue("t", status);
                        h.Parameters.AddWithValue("b", currentBlock);
                        await h.ExecuteNonQueryAsync(token);
                    }

                    // upsert mission row
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
                          last_seen_block         = excluded.last_seen_block,
                          updated_at              = now();
                    ", conn, tx))
                    {
                        up.Parameters.AddWithValue("a",  missionAddr);
                        up.Parameters.AddWithValue("n",  name);

                        // bytes -> SMALLINT
                        up.Parameters.AddWithValue("ty", (short)md.MissionType);
                        up.Parameters.AddWithValue("st", status);

                        // BIGINT timestamps
                        up.Parameters.AddWithValue("es", ToInt64(md.EnrollmentStart));
                        up.Parameters.AddWithValue("ee", ToInt64(md.EnrollmentEnd));

                        // NUMERIC(78,0)
                        up.Parameters.Add("amt", NpgsqlDbType.Numeric).Value = md.EnrollmentAmount;

                        // SMALLINTs
                        up.Parameters.AddWithValue("min", (short)md.EnrollmentMinPlayers);
                        up.Parameters.AddWithValue("max", (short)md.EnrollmentMaxPlayers);

                        // BIGINT timestamps
                        up.Parameters.AddWithValue("ms", ToInt64(md.MissionStart));
                        up.Parameters.AddWithValue("me", ToInt64(md.MissionEnd));

                        // SMALLINTs
                        up.Parameters.AddWithValue("rt", (short)md.MissionRounds);
                        up.Parameters.AddWithValue("rc", (short)md.RoundCount);

                        // NUMERIC(78,0)
                        up.Parameters.Add("cs", NpgsqlDbType.Numeric).Value = md.EthStart;
                        up.Parameters.Add("cc", NpgsqlDbType.Numeric).Value = md.EthCurrent;

                        // BIGINT + block
                        up.Parameters.AddWithValue("pt",  ToInt64(md.PauseTimestamp));
                        up.Parameters.AddWithValue("blk", currentBlock);

                        await up.ExecuteNonQueryAsync(token);
                    }

                    // upsert enrollments
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

                    // upsert rounds (1-based)
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
                            rr.Parameters.Add("amt", NpgsqlDbType.Numeric).Value = rw.Amount; // NUMERIC(78,0)
                            rr.Parameters.AddWithValue("b",  currentBlock);
                            await rr.ExecuteNonQueryAsync(token);
                        }
                    }

                    await tx.CommitAsync(token);
                }
                catch (Exception ex)
                {
                    _log.LogError(ex, "Decode/DB upsert failed for mission {addr}", missionAddr);
                    continue;
                }
            }
        }
    }
}

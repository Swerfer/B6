
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Nethereum.ABI.FunctionEncoding.Attributes;
using Nethereum.Contracts;
using Nethereum.Web3;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace B6.Indexer
{
    // Mission.getRealtimeStatus() returns uint8
    [Function("getRealtimeStatus", "uint8")]
    public class GetRealtimeStatusFunction : FunctionMessage { }

    public class RealtimeStatusRefresher : BackgroundService
    {
        private readonly ILogger<RealtimeStatusRefresher> _log;
        private readonly IConfiguration _cfg;
        private Web3 _web3 = default!;

        public RealtimeStatusRefresher(ILogger<RealtimeStatusRefresher> log, IConfiguration cfg)
        {
            _log = log;
            _cfg = cfg;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var rpc = _cfg["Cronos:Rpc"];
            if (string.IsNullOrWhiteSpace(rpc))
                throw new InvalidOperationException("Missing Cronos:Rpc.");

            _web3 = new Web3(rpc);
            _log.LogInformation("RealtimeStatusRefresher running (once per minute at mm:01).");

            while (!stoppingToken.IsCancellationRequested)
            {
                try { await RefreshStatuses(stoppingToken); }
                catch (Exception ex) { _log.LogError(ex, "Realtime refresh failed."); }

                await Task.Delay(DelayUntilNextMinutePlusOne(), stoppingToken);
            }
        }

        private static TimeSpan DelayUntilNextMinutePlusOne()
        {
            var now = DateTimeOffset.UtcNow;
            var next = new DateTimeOffset(now.Year, now.Month, now.Day, now.Hour, now.Minute, 0, TimeSpan.Zero)
                       .AddMinutes(1).AddSeconds(1);
            return next - now;
        }

        private async Task RefreshStatuses(CancellationToken ct)
        {
            var cs = _cfg.GetConnectionString("Db");
            if (string.IsNullOrWhiteSpace(cs))
                throw new InvalidOperationException("Missing ConnectionStrings:Db.");

            // Align with your API: "not ended" is status < 5. :contentReference[oaicite:0]{index=0}
            const string selectSql = @"
                select mission_address, status
                from missions
                where status < 5;";

            var pending = new List<(string Addr, int DbStatus)>();

            await using var conn = new NpgsqlConnection(cs);
            await conn.OpenAsync(ct);

            await using (var cmd = new NpgsqlCommand(selectSql, conn))
            await using (var rd = await cmd.ExecuteReaderAsync(ct))
            {
                while (await rd.ReadAsync(ct))
                {
                    var addr = rd.GetString(0);
                    var st   = Convert.ToInt32(rd.GetInt16(1)); // SMALLINT -> int
                    pending.Add((addr, st));
                }
            }

            var handler = _web3.Eth.GetContractQueryHandler<GetRealtimeStatusFunction>();

            foreach (var m in pending)
            {
                ct.ThrowIfCancellationRequested();

                byte rt;
                try { rt = await handler.QueryAsync<byte>(m.Addr, new GetRealtimeStatusFunction()); }
                catch (Exception ex)
                {
                    _log.LogWarning(ex, "getRealtimeStatus failed for {Addr}", m.Addr);
                    continue;
                }

                var newStatus = (int)rt;
                if (newStatus == m.DbStatus) continue;

                await using var tx = await conn.BeginTransactionAsync(ct);

                const string updSql = @"update missions set status=@s, updated_at=now() where mission_address=@a;";
                await using (var upd = new NpgsqlCommand(updSql, conn, tx))
                {
                    upd.Parameters.AddWithValue("s", newStatus);
                    upd.Parameters.AddWithValue("a", m.Addr);
                    await upd.ExecuteNonQueryAsync(ct);
                }

                const string histSql = @"
                    insert into mission_status_history (mission_address, from_status, to_status, block_number)
                    values (@a, @from, @to, null);";
                await using (var hist = new NpgsqlCommand(histSql, conn, tx))
                {
                    hist.Parameters.AddWithValue("a",    m.Addr);
                    hist.Parameters.AddWithValue("from", m.DbStatus);
                    hist.Parameters.AddWithValue("to",   newStatus);
                    await hist.ExecuteNonQueryAsync(ct);
                }

                await tx.CommitAsync(ct);
                _log.LogInformation("Status updated {Addr}: {From} -> {To}", m.Addr, m.DbStatus, newStatus);
            }
        }
    }
}

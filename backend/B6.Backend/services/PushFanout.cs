using System.Text.Json;
using Npgsql;
using WebPush;

namespace B6.Backend.Services
{
    public class PushFanout
    {
        private readonly IConfiguration _cfg;
        private readonly VapidDetails   _vapid;
        private readonly WebPushClient  _web;
        private readonly string         _cs;

        public sealed record PushAttempt(string Endpoint, bool Ok, int? Status, string? Error);

        public                          PushFanout(IConfiguration cfg, VapidDetails vapid) {
            _cfg = cfg;
            _vapid = vapid;
            _web = new WebPushClient();
            _cs = cfg.GetConnectionString("Db") ?? throw new InvalidOperationException("Db connection missing");
        }

        public async Task               OnStatusChangedAsync(string mission, short newStatus) {
            // Schedule: 5 minutes before mission_start → notify enrolled players (all tabs)
            if (newStatus == 2 || newStatus == 1)
            {
                var ms = await GetMissionTsAsync(mission, "mission_start");
                if (ms.HasValue)
                {
                    var fireAt = ms.Value.AddMinutes(-5);
                    NotificationScheduler.ScheduleOnce(fireAt, async () =>
                    {
                        var players = await GetEnrolledAsync(mission);
                        foreach (var p in players)
                            await SendToWalletAsync(p,
                                "Mission starts soon",
                                "5 minutes to mission start.",
                                new { type = "prestart", mission, at = ms.Value });
                    });
                }
            }
        }

        public async Task               OnMissionUpdatedAsync(string mission) {
            // Schedule: 1 minute before mission_end → notify players who didn’t bank yet (all tabs)
            var me = await GetMissionTsAsync(mission, "mission_end");
            if (me.HasValue)
            {
                var fireAt = me.Value.AddMinutes(-1);
                NotificationScheduler.ScheduleOnce(fireAt, async () =>
                {
                    var players = await GetEnrolledAsync(mission);
                    foreach (var p in players)
                    {
                        if (await HasWonAsync(mission, p)) continue;
                        await SendToWalletAsync(p,
                            "1 minute left",
                            "You haven’t banked yet.",
                            new { type = "preend", mission, at = me.Value });
                    }
                });
            }
        }

        public async Task               OnRoundAsync(string mission, short round, string winner, string amountWei) {
            // Notify others (all tabs)
            var players = await GetEnrolledAsync(mission);
            foreach (var p in players)
            {
                if (string.Equals(p, winner, StringComparison.InvariantCultureIgnoreCase)) continue;
                await SendToWalletAsync(p,
                    $"Round {round} banked",
                    "Another player banked this round.",
                    new { type = "round", mission, round, winner, amountWei });
            }

            // Schedule: 10 seconds before cooldown end (all watchers)
            var coolEnd = await GetCooldownEndAsync(mission);
            if (coolEnd.HasValue)
            {
                var fireAt = coolEnd.Value.AddSeconds(-10);
                NotificationScheduler.ScheduleOnce(fireAt, async () =>
                {
                    var watchers = await GetEnrolledAsync(mission);
                    foreach (var w in watchers)
                    {
                        if (await HasWonAsync(mission, w)) continue;
                        await SendToWalletAsync(w,
                            "Cooldown ending",
                            "10 seconds until banking resumes.",
                            new { type = "cooldown", mission, at = coolEnd.Value });
                    }
                });
            }
        }

        public async Task               UpsertSubscriptionAsync(PushSubscribeDto dto) {
            await using var c = new NpgsqlConnection(_cs);
            await c.OpenAsync();
            await using var cmd = new NpgsqlCommand(@"
                insert into push_subscriptions(wallet_address, endpoint, p256dh, auth, last_seen_at)
                values(@w, @e, @p, @a, now())
                on conflict (endpoint)
                do update set wallet_address = excluded.wallet_address, p256dh=excluded.p256dh, auth=excluded.auth, last_seen_at=now();", c);
            cmd.Parameters.AddWithValue("w", (dto.Address ?? "").ToLowerInvariant());
            cmd.Parameters.AddWithValue("e", dto.Endpoint ?? "");
            cmd.Parameters.AddWithValue("p", dto.P256dh ?? "");
            cmd.Parameters.AddWithValue("a", dto.Auth ?? "");
            await cmd.ExecuteNonQueryAsync();
        }

        private async Task<List<string>> GetEnrolledAsync(string mission) {
            await using var c = new NpgsqlConnection(_cs);
            await c.OpenAsync();
            await using var cmd = new NpgsqlCommand("select player from players where mission_address=@a;", c);
            cmd.Parameters.AddWithValue("a", mission.ToLowerInvariant());
            var list = new List<string>();
            await using var rd = await cmd.ExecuteReaderAsync();
            while (await rd.ReadAsync()) list.Add((rd["player"] as string ?? "").ToLowerInvariant());
            return list;
        }

        private async Task<bool>        HasWonAsync(string mission, string wallet) {
            await using var c = new NpgsqlConnection(_cs);
            await c.OpenAsync();
            await using var cmd = new NpgsqlCommand(@"
                select 1 from mission_rounds where mission_address=@a and lower(winner_address)=@w limit 1;", c);
            cmd.Parameters.AddWithValue("a", mission.ToLowerInvariant());
            cmd.Parameters.AddWithValue("w", (wallet ?? "").ToLowerInvariant());
            var val = await cmd.ExecuteScalarAsync();
            return val != null;
        }

        private async Task<DateTime?>   GetMissionTsAsync(string mission, string col) {
            await using var c = new NpgsqlConnection(_cs);
            await c.OpenAsync();
            await using var cmd = new NpgsqlCommand($@"
                select to_timestamp({col}) at time zone 'utc' from missions where mission_address=@a;", c);
            cmd.Parameters.AddWithValue("a", mission.ToLowerInvariant());
            var o = await cmd.ExecuteScalarAsync();
            return o as DateTime?;
        }

        private async Task<DateTime?>   GetCooldownEndAsync(string mission) {
            await using var c = new NpgsqlConnection(_cs);
            await c.OpenAsync();
            await using var cmd = new NpgsqlCommand(@"
                select pause_timestamp, round_count, mission_rounds_total, round_pause_secs, last_round_pause_secs
                from missions where mission_address=@a;", c);
            cmd.Parameters.AddWithValue("a", mission.ToLowerInvariant());
            await using var rd = await cmd.ExecuteReaderAsync();
            if (!await rd.ReadAsync()) return null;

            var pause = rd["pause_timestamp"]               is DBNull ?  0 : (long)rd["pause_timestamp"];
            var rc    = rd["round_count"]                   is DBNull ?  0 : (short)rd["round_count"];
            var total = rd["mission_rounds_total"]          is DBNull ?  0 : (short)rd["mission_rounds_total"];
            if (pause <= 0) return null;

            var roundPause = rd["round_pause_secs"]         is DBNull ? 60 : (short)rd["round_pause_secs"];
            var lastPause  = rd["last_round_pause_secs"]    is DBNull ? 60 : (short)rd["last_round_pause_secs"];
            var secs       = (rc == (total - 1)) ? lastPause : roundPause;
            return DateTimeOffset.FromUnixTimeSeconds(pause + secs).UtcDateTime;
        }

        private async Task              SendToWalletAsync(string wallet, string title, string body, object payload) {
            await SendWebPushAsync(wallet, title, body, payload);
        }

        private async Task              SendWebPushAsync(string wallet, string title, string body, object payload) {
            static string ToUrlB64(string x) =>
                string.IsNullOrWhiteSpace(x) ? "" : x.Replace('+','-').Replace('/','_').TrimEnd('=');

            var subs = await GetWebPushSubsAsync(wallet);
            foreach (var s in subs)
            {
                var sub = new PushSubscription(s.Endpoint, ToUrlB64(s.P256dh), ToUrlB64(s.Auth));
                var json = JsonSerializer.Serialize(new { title, body, data = payload });
                try
                {
                    var options = new Dictionary<string, object>
                    {
                        ["vapidDetails"] = _vapid,                               // required
                        ["TTL"] = 60,                                            // optional
                        ["headers"] = new Dictionary<string, object> {           // optional
                            ["Urgency"] = "high"
                        }
                    };

                    await _web.SendNotificationAsync(sub, json, options);
                }
                catch (WebPushException ex)
                {
                    if (ex.StatusCode == System.Net.HttpStatusCode.Gone || ex.StatusCode == System.Net.HttpStatusCode.NotFound)
                        await RemoveSubAsync(s.Endpoint);
                }
            }
        }

        private async Task              RemoveSubAsync(string endpoint) {
            await using var c = new NpgsqlConnection(_cs);
            await c.OpenAsync();
            await using var cmd = new NpgsqlCommand("delete from push_subscriptions where endpoint=@e;", c);
            cmd.Parameters.AddWithValue("e", endpoint);
            await cmd.ExecuteNonQueryAsync();
        }

        public async Task<int>          PruneSubsAsync(string wallet, string? keepEndpoint){
            await using var c = new NpgsqlConnection(_cs);
            await c.OpenAsync();
            var sql = keepEndpoint == null
                ? "delete from push_subscriptions where wallet_address=@w;"
                : "delete from push_subscriptions where wallet_address=@w and endpoint<>@k;";
            await using var cmd = new NpgsqlCommand(sql, c);
            cmd.Parameters.AddWithValue("w", (wallet ?? "").ToLowerInvariant());
            if (keepEndpoint != null) cmd.Parameters.AddWithValue("k", keepEndpoint);
            return await cmd.ExecuteNonQueryAsync();
        }

        public async Task<(int Count, List<PushAttempt> Attempts)>              DebugPushVerboseAsync(string wallet, bool noPayload = false) {
            var attempts = new List<PushAttempt>();
            var subs = await GetWebPushSubsAsync(wallet);
            foreach (var s in subs)
            {
                static string ToUrlB64(string x) =>
                    string.IsNullOrWhiteSpace(x) ? "" : x.Replace('+','-').Replace('/','_').TrimEnd('=');

                var sub = new PushSubscription(s.Endpoint, ToUrlB64(s.P256dh), ToUrlB64(s.Auth));
                var json = JsonSerializer.Serialize(new { title = "B6 test", body = "It works!", data = new { type = "debug", at = DateTime.UtcNow } });

                try {
                var options = new Dictionary<string, object> {
                    ["vapidDetails"] = _vapid,
                    ["TTL"] = 60,
                    ["headers"] = new Dictionary<string, object> { ["Urgency"] = "high" }
                };

                if (noPayload) {
                    await _web.SendNotificationAsync(sub, null, options);   // <— send no payload
                } else {
                    await _web.SendNotificationAsync(sub, json, options);
                }
                attempts.Add(new PushAttempt(s.Endpoint, true, 200, null));

                }
                catch (WebPushException ex)
                {
                    // include HTTP status + message body so we can see Unauthorized, InvalidToken, etc.
                    attempts.Add(new PushAttempt(s.Endpoint, false, (int)ex.StatusCode, ex.Message));
                    if (ex.StatusCode == System.Net.HttpStatusCode.Gone || ex.StatusCode == System.Net.HttpStatusCode.NotFound)
                    {
                        await RemoveSubAsync(s.Endpoint);
                    }
                }
                catch (Exception ex)
                {
                    attempts.Add(new PushAttempt(s.Endpoint, false, null, ex.Message));
                }
            }
            return (subs.Count, attempts);
        }

        private async Task<List<(string Endpoint, string P256dh, string Auth)>> GetWebPushSubsAsync(string wallet) {
            await using var c = new NpgsqlConnection(_cs);
            await c.OpenAsync();
            await using var cmd = new NpgsqlCommand("select endpoint,p256dh,auth from push_subscriptions where wallet_address=@w;", c);
            cmd.Parameters.AddWithValue("w", (wallet ?? "").ToLowerInvariant());
            var list = new List<(string, string, string)>();
            await using var rd = await cmd.ExecuteReaderAsync();
            while (await rd.ReadAsync())
                list.Add((rd.GetString(0), rd.GetString(1), rd.GetString(2)));
            return list;
        }
    }
}

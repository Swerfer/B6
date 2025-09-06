using Azure.Identity;
using Azure.Extensions.AspNetCore.Configuration.Secrets;
using Azure.Security.KeyVault.Secrets;
using B6.Backend;
using B6.Backend.Hubs;
using B6.Contracts; 
using B6.Backend.Services;   // PushFanout & NotificationScheduler (new)
using Microsoft.AspNetCore.SignalR;                       
using Microsoft.Extensions.Logging.EventLog;
using Nethereum.Web3;
using Nethereum.Contracts;
using Nethereum.ABI.FunctionEncoding.Attributes;
using Npgsql;
using System.IO;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using WebPush;               // Web Push VAPID support (new)

// The base uri is https://b6missions.com/api It is set to 'api' in ISS server.

var builder = WebApplication.CreateBuilder(args);

builder.Configuration.AddAzureKeyVault(
    new Uri("https://B6Missions.vault.azure.net/"),
    new DefaultAzureCredential()
);

// Validate required keys early (so we don't start with bad config)
string[] requiredKeys = {
    "Cronos:Rpc",
    "Contracts:Factory",
    "ConnectionStrings:Db"
};

foreach (var k in requiredKeys) {
    if (string.IsNullOrWhiteSpace(builder.Configuration[k]))
        throw new InvalidOperationException($"Missing configuration key on startup: {k}");
}

builder.Services.AddSignalR();

builder.Services.AddCors(options => {
    options.AddPolicy("AllowFrontend", policy =>
    {
        policy.WithOrigins("https://b6missions.com") 
              .AllowAnyHeader()
              .AllowAnyMethod()
              .AllowCredentials(); // Only if you're using cookies or auth headers
    });
});

builder.Services.AddHttpClient();

builder.Logging.ClearProviders();
builder.Logging.SetMinimumLevel(LogLevel.Information);
builder.Logging.AddConsole();

// Web Push VAPID keys
builder.Services.AddSingleton(sp =>{
    var cfg = sp.GetRequiredService<IConfiguration>();
    var subject = cfg["WebPush:Subject"]    ?? "mailto:ops@b6missions.com";
    var pub     = cfg["WebPush:PublicKey"]  ?? throw new InvalidOperationException("Missing WebPush:PublicKey");
    var priv    = cfg["WebPush:PrivateKey"] ?? throw new InvalidOperationException("Missing WebPush:PrivateKey");
    return new VapidDetails(subject, pub, priv);
});

// Notification services
builder.Services.AddSingleton<PushFanout>();
builder.Services.AddHostedService<NotificationScheduler>();

var app = builder.Build();

app.UseCors("AllowFrontend");

/* --------------------- Helpers ---------------------*/
static long     ToUnixSeconds(DateTime dtUtc){
    if (dtUtc.Kind != DateTimeKind.Utc)
        dtUtc = DateTime.SpecifyKind(dtUtc, DateTimeKind.Utc);
    return new DateTimeOffset(dtUtc).ToUnixTimeSeconds();
}

static string   GetRequired(IConfiguration cfg, string key){
    var v = cfg[key];
    if (string.IsNullOrWhiteSpace(v))
        throw new InvalidOperationException($"Missing configuration key: {key}");
    return v;
}

/* ------------------- API endpoints ----------------- */

app.MapGet("/",                               () => 
    Results.Ok("OK")
);

// /api/config -> shared runtime config for frontend
app.MapGet("/config",                         (IConfiguration cfg) => {
    var rpc     = GetRequired(cfg, "Cronos:Rpc");
    var factory = GetRequired(cfg, "Contracts:Factory");
    return Results.Ok(new { rpc, factory });
});

// /api/rpc -> reverse-proxy JSON-RPC to Cronos node (to avoid CORS issues)
app.MapPost("/rpc",                     async (HttpRequest req, IHttpClientFactory f, IConfiguration cfg) => {
    using var reader = new StreamReader(req.Body);
    var body = await reader.ReadToEndAsync();

    var upstream = cfg["Cronos:Rpc"] ?? "https://evm.cronos.org/";

    var client = f.CreateClient();
    using var msg = new HttpRequestMessage(HttpMethod.Post, upstream);
    msg.Content = new StringContent(body, Encoding.UTF8, "application/json");
    msg.Headers.UserAgent.ParseAdd("B6MissionsProxy/1.0");

    using var resp = await client.SendAsync(msg, HttpCompletionOption.ResponseHeadersRead);
    var text = await resp.Content.ReadAsStringAsync();

    // ✅ Option A: preserve upstream status code & JSON
    try
    {
        using var doc = JsonDocument.Parse(text);
        var clone = doc.RootElement.Clone();                    // ← keeps data alive after dispose
        return Results.Json(clone, statusCode: (int)resp.StatusCode);
    }
    catch
    {
        return Results.Content(text, "application/json", Encoding.UTF8, (int)resp.StatusCode);
    }

});

// /api/secrets -> list all Key Vault secrets (for admin use only)
app.MapGet("/secrets", async (HttpRequest req, IConfiguration cfg) =>{
    // --- 1) Ask the browser for Basic credentials if none present
    var auth = req.Headers["Authorization"].ToString();
    if (string.IsNullOrWhiteSpace(auth) || !auth.StartsWith("Basic ", StringComparison.OrdinalIgnoreCase))
    {
        req.HttpContext.Response.Headers["WWW-Authenticate"] = "Basic realm=\"B6 Secrets\"";
        return Results.Unauthorized();
    }

    // --- 2) Extract password from Basic <base64(user:pass)>
    try
    {
        var raw     = Encoding.UTF8.GetString(Convert.FromBase64String(auth.Substring("Basic ".Length).Trim()));
        var colonIx = raw.IndexOf(':');
        var pass    = colonIx >= 0 ? raw[(colonIx + 1)..] : raw;   // username ignored

        var expected = cfg["Azure:PW"]; // from Key Vault secret "Azure--PW"
        if (string.IsNullOrEmpty(expected) || pass != expected)
        {
            req.HttpContext.Response.Headers["WWW-Authenticate"] = "Basic realm=\"B6 Secrets\"";
            return Results.Unauthorized();
        }
    }
    catch
    {
        req.HttpContext.Response.Headers["WWW-Authenticate"] = "Basic realm=\"B6 Secrets\"";
        return Results.Unauthorized();
    }

    // --- 3) List *Key Vault* secrets directly
    var client = new SecretClient(
        new Uri("https://B6Missions.vault.azure.net/"),
        new DefaultAzureCredential()
    );

    var dict = new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase);

    await foreach (var p in client.GetPropertiesOfSecretsAsync())
    {
        if (p.Enabled != true) continue;

        // optional: do not show the password secret itself
        if (p.Name.Equals("Azure--PW", StringComparison.OrdinalIgnoreCase)) continue;

        var s   = await client.GetSecretAsync(p.Name);
        var key = p.Name.Replace("--", ":");   // same normalization you use in config
        dict[key] = s.Value.Value;
    }

    return Results.Ok(dict);
});

// Expose VAPID public key to the frontend
app.MapGet("/push/vapid-public-key",          (IConfiguration cfg) =>
    Results.Text(cfg["WebPush:PublicKey"] ?? "", "text/plain")
);

// Save/refresh a browser's push subscription
app.MapPost("/push/subscribe",          async (PushSubscribeDto dto, PushFanout fanout) => {
    await fanout.UpsertSubscriptionAsync(dto);
    return Results.Ok(new { saved = true });
});

/***********************
 *  MISSIONS – READ API
 ***********************/
app.MapGet("/missions/not-ended",       async (IConfiguration cfg) => {
    var cs = cfg.GetConnectionString("Db");
    await using var conn = new NpgsqlConnection(cs);
    await conn.OpenAsync();

    // status < 5  → Pending/Enrolling/Arming/Active/Paused (not ended)
    var sql = @"
      select
        mission_address,
        name,
        mission_type,
        status,
        enrollment_start,
        enrollment_end,
        enrollment_amount_wei::text  as enrollment_amount_wei,
        enrollment_min_players,
        enrollment_max_players,
        mission_start,
        mission_end,
        mission_rounds_total,
        round_count,
        cro_start_wei::text          as cro_start_wei,
        cro_current_wei::text        as cro_current_wei,
        pause_timestamp,
        last_seen_block,
        updated_at
      from missions
      where status < 5
      order by enrollment_end asc nulls last, mission_end asc nulls last;";

    await using var cmd = new NpgsqlCommand(sql, conn);
    await using var rd  = await cmd.ExecuteReaderAsync();

    var list = new List<object>();
    while (await rd.ReadAsync())
    {
        list.Add(new {
            mission_address        = rd["mission_address"] as string,
            name                   = rd["name"] as string,
            mission_type           = (short) rd["mission_type"],
            status                 = (short) rd["status"],
            enrollment_start       = (long)  rd["enrollment_start"],
            enrollment_end         = (long)  rd["enrollment_end"],
            enrollment_amount_wei  = (string)rd["enrollment_amount_wei"],
            enrollment_min_players = (short) rd["enrollment_min_players"],
            enrollment_max_players = (short) rd["enrollment_max_players"],
            mission_start          = (long)  rd["mission_start"],
            mission_end            = (long)  rd["mission_end"],
            mission_rounds_total   = (short) rd["mission_rounds_total"],
            round_count            = (short) rd["round_count"],
            cro_start_wei          = (string)rd["cro_start_wei"],
            cro_current_wei        = (string)rd["cro_current_wei"],
            pause_timestamp        = rd["pause_timestamp"] is DBNull ? null : (long?) rd["pause_timestamp"],
            last_seen_block        = rd["last_seen_block"]  is DBNull ? null : (long?) rd["last_seen_block"],
            updated_at             = ToUnixSeconds(((DateTime) rd["updated_at"]).ToUniversalTime())
        });
    }

    return Results.Ok(list);
});

app.MapGet("/missions/joinable",        async (IConfiguration cfg) => {
    var cs = cfg.GetConnectionString("Db");
    await using var conn = new NpgsqlConnection(cs);
    await conn.OpenAsync();

    var sql = @"
      with counts as (
        select mission_address, count(*)::int as enrolled
        from mission_enrollments
        group by mission_address
      )
      select
        m.mission_address,
        m.name,
        m.mission_type,
        m.status,
        m.enrollment_start,
        m.enrollment_end,
        m.enrollment_amount_wei::text  as enrollment_amount_wei,
        m.enrollment_min_players,
        m.enrollment_max_players,
        m.mission_start,
        m.mission_end,
        m.mission_rounds_total,
        coalesce(c.enrolled,0)          as enrolled_players
      from missions m
      left join counts c using (mission_address)
      where m.status = 1
        and m.enrollment_end > (extract(epoch from now())::bigint)
      order by m.enrollment_end asc;";

    await using var cmd = new NpgsqlCommand(sql, conn);
    await using var rd  = await cmd.ExecuteReaderAsync();

    var list = new List<object>();
    while (await rd.ReadAsync())
    {
        list.Add(new {
            mission_address        = rd["mission_address"] as string,
            name                   = rd["name"] as string,
            mission_type           = (short) rd["mission_type"],
            status                 = (short) rd["status"],
            enrollment_start       = (long)  rd["enrollment_start"],
            enrollment_end         = (long)  rd["enrollment_end"],
            enrollment_amount_wei  = (string)rd["enrollment_amount_wei"],
            enrollment_min_players = (short) rd["enrollment_min_players"],
            enrollment_max_players = (short) rd["enrollment_max_players"],
            mission_start          = (long)  rd["mission_start"],
            mission_end            = (long)  rd["mission_end"],
            mission_rounds_total   = (short) rd["mission_rounds_total"],
            enrolled_players       = (int)   rd["enrolled_players"]
        });
    }

    return Results.Ok(list);
});

app.MapGet("/missions/player/{addr}",   async (string addr, IConfiguration cfg) => {
    if (string.IsNullOrWhiteSpace(addr)) return Results.BadRequest("Missing address");
    addr = addr.ToLowerInvariant();

    var cs = cfg.GetConnectionString("Db");
    await using var conn = new NpgsqlConnection(cs);
    await conn.OpenAsync();

    var sql = @"
    with counts as (
        select mission_address, count(*)::int as enrolled
        from mission_enrollments
        group by mission_address
    )
    select
        m.mission_address,
        m.name,
        m.mission_type,
        m.status,
        m.mission_start,
        m.mission_end,
        m.mission_rounds_total,
        m.round_count,
        m.enrollment_min_players,
        coalesce(c.enrolled,0) as enrolled_players,
        case
            when m.status = 7 and m.round_count = 0 and coalesce(c.enrolled,0) <  m.enrollment_min_players then 'Not enough players'
            when m.status = 7 and m.round_count = 0 and coalesce(c.enrolled,0) >= m.enrollment_min_players then 'No rounds played'
        else null
        end as failure_reason,
        e.enrolled_at,
        e.refunded,
        e.refund_tx_hash
    from mission_enrollments e
    join missions m using (mission_address)
    left join counts c using (mission_address)
    where e.player_address = @p
    order by m.status asc, m.mission_end asc nulls last;";

    await using var cmd = new NpgsqlCommand(sql, conn);
    cmd.Parameters.AddWithValue("p", addr);
    await using var rd  = await cmd.ExecuteReaderAsync();

    var list = new List<object>();
    while (await rd.ReadAsync())
    {
        list.Add(new {
            mission_address       = rd["mission_address"] as string,
            name                  = rd["name"] as string,
            mission_type          = (short) rd["mission_type"],
            status                = (short) rd["status"],
            mission_start         = (long)  rd["mission_start"],
            mission_end           = (long)  rd["mission_end"],
            mission_rounds_total  = (short) rd["mission_rounds_total"],
            round_count           = (short) rd["round_count"],
            enrollment_min_players= (short) rd["enrollment_min_players"],   // +++
            enrolled_players      = (int)   rd["enrolled_players"],         // +++
            failure_reason        = rd["failure_reason"] as string,         // +++
            enrolled_at           = rd["enrolled_at"] is DBNull
                ? (long?)null
                : ToUnixSeconds(((DateTime) rd["enrolled_at"]).ToUniversalTime()),
            refunded              = (bool)  rd["refunded"],
            refund_tx_hash        = rd["refund_tx_hash"] as string
        });
    }

    return Results.Ok(list);
});

app.MapGet("/missions/mission/{addr}",  async (string addr, IConfiguration cfg) => {
    if (string.IsNullOrWhiteSpace(addr)) return Results.BadRequest("Missing address");
    addr = addr.ToLowerInvariant();

    var cs = cfg.GetConnectionString("Db");
    await using var conn = new NpgsqlConnection(cs);
    await conn.OpenAsync();

    // 1) mission core row
    var coreSql = @"
      with counts as (
        select mission_address, count(*)::int as enrolled
        from mission_enrollments
        group by mission_address
      )
      select
        m.mission_address,
        m.name,
        m.mission_type,
        m.status,
        m.enrollment_start,
        m.enrollment_end,
        m.enrollment_amount_wei::text  as enrollment_amount_wei,
        m.enrollment_min_players,
        m.enrollment_max_players,
        m.mission_start,
        m.mission_end,
        m.mission_rounds_total,
        m.round_count,
        m.cro_start_wei::text          as cro_start_wei,
        m.cro_current_wei::text        as cro_current_wei,
        m.pause_timestamp,
        m.last_seen_block,
        m.updated_at,
        m.mission_created,
        coalesce(c.enrolled,0)       as enrolled_players
      from missions m
      left join counts c using (mission_address)
      where mission_address = @a;";

    await using var core = new NpgsqlCommand(coreSql, conn);
    core.Parameters.AddWithValue("a", addr);
    await using var rd = await core.ExecuteReaderAsync();
    if (!await rd.ReadAsync()) return Results.NotFound("Mission not found");

    var mission = new {
        mission_address        = rd["mission_address"] as string,
        name                   = rd["name"] as string,
        mission_type           = (short) rd["mission_type"],
        status                 = (short) rd["status"],
        enrollment_start       = (long)  rd["enrollment_start"],
        enrollment_end         = (long)  rd["enrollment_end"],
        enrollment_amount_wei  = (string)rd["enrollment_amount_wei"],
        enrollment_min_players = (short) rd["enrollment_min_players"],
        enrollment_max_players = (short) rd["enrollment_max_players"],
        enrolled_players       = (int)   rd["enrolled_players"],
        mission_start          = (long)  rd["mission_start"],
        mission_end            = (long)  rd["mission_end"],
        mission_rounds_total   = (short) rd["mission_rounds_total"],
        round_count            = (short) rd["round_count"],
        cro_start_wei          = (string)rd["cro_start_wei"],
        cro_current_wei        = (string)rd["cro_current_wei"],
        pause_timestamp        = rd["pause_timestamp"] is DBNull ? null : (long?) rd["pause_timestamp"],
        last_seen_block        = rd["last_seen_block"]  is DBNull ? null : (long?) rd["last_seen_block"],
        updated_at             = ToUnixSeconds(((DateTime) rd["updated_at"]).ToUniversalTime()),
        mission_created        = (long)  rd["mission_created"]
    };
    await rd.CloseAsync();

    // 2) enrollments
    var enSql = @"
      select player_address, refunded, coalesce(refund_tx_hash,'') as refund_tx_hash, enrolled_at
      from mission_enrollments
      where mission_address = @a
      order by enrolled_at asc nulls last;";
    await using var enCmd = new NpgsqlCommand(enSql, conn);
    enCmd.Parameters.AddWithValue("a", addr);
    await using var enRd = await enCmd.ExecuteReaderAsync();

    var enrollments = new List<object>();
    while (await enRd.ReadAsync())
    {
        enrollments.Add(new {
            player_address = enRd["player_address"] as string,
            refunded       = (bool) enRd["refunded"],
            refund_tx_hash = enRd["refund_tx_hash"] as string,
            enrolled_at = enRd["enrolled_at"] is DBNull
                ? (long?)null
                : ToUnixSeconds(((DateTime)enRd["enrolled_at"]).ToUniversalTime())
        });
    }
    await enRd.CloseAsync();

    // 3) rounds
    var rSql = @"
      select round_number, winner_address, payout_wei::text as payout_wei, block_number, tx_hash, created_at
      from mission_rounds
      where mission_address = @a
      order by round_number asc;";
    await using var rCmd = new NpgsqlCommand(rSql, conn);
    rCmd.Parameters.AddWithValue("a", addr);
    await using var rRd = await rCmd.ExecuteReaderAsync();

    var rounds = new List<object>();
    while (await rRd.ReadAsync())
    {
        rounds.Add(new {
            round_number  = (short) rRd["round_number"],
            winner_address= rRd["winner_address"] as string,
            payout_wei    = (string) rRd["payout_wei"],
            block_number  = rRd["block_number"] is DBNull ? null : (long?) rRd["block_number"],
            tx_hash       = rRd["tx_hash"] as string,
            created_at    = ToUnixSeconds(((DateTime) rRd["created_at"]).ToUniversalTime())
        });
    }

    return Results.Ok(new { mission, enrollments, rounds });
});

/* ---------- HEALTH ---------- */
app.MapGet("/health",                         () => 
    Results.Ok("OK")
);

app.MapGet("/health/db",                async (IConfiguration cfg) => {
    var cs = cfg.GetConnectionString("Db");
    await using var conn = new NpgsqlConnection(cs);
    await conn.OpenAsync();
    await using var cmd = new NpgsqlCommand("select 1", conn);
    var val = (int)(await cmd.ExecuteScalarAsync() ?? 0);
    return Results.Ok(val == 1 ? "DB OK" : "DB FAIL");
});

/* ---------- DEBUG: CHAIN INFO ---------- */
app.MapGet("/debug/chain",              async (IConfiguration cfg) => {
    var rpc = GetRequired(cfg, "Cronos:Rpc");
    var web3 = new Web3(rpc);
    var chainId = (long)(await web3.Eth.ChainId.SendRequestAsync()).Value;
    var latest  = (long)(await web3.Eth.Blocks.GetBlockNumber.SendRequestAsync()).Value;
    return Results.Ok(new { rpc, chainId, latest });
});

/* ---------- DEBUG: FACTORY COUNTS ---------- */
app.MapGet("/debug/factory",            async (IConfiguration cfg) => {
    var rpc     = GetRequired(cfg, "Cronos:Rpc");
    var factory = GetRequired(cfg, "Contracts:Factory");

    var web3 = new Web3(rpc);

    var notEndedH = web3.Eth.GetContractQueryHandler<GetMissionsNotEndedFunction>();
    var allH      = web3.Eth.GetContractQueryHandler<GetAllMissionsFunction>();

    var notEnded = await notEndedH.QueryDeserializingToObjectAsync<GetMissionsOutput>(
        new GetMissionsNotEndedFunction(), factory, null);

    var all = await allH.QueryDeserializingToObjectAsync<GetMissionsOutput>(
        new GetAllMissionsFunction(), factory, null);

    return Results.Ok(new {
        factory,
        notEnded = notEnded?.Missions?.Count ?? 0,
        all = all?.Missions?.Count ?? 0,
        sampleAll = all?.Missions?.Count > 0
          ? all.Missions.GetRange(0, Math.Min(5, all.Missions.Count))
          : new List<string>()
    });
});

/* ---------- DEBUG: SINGLE MISSION PROBE ---------- */
app.MapGet("/debug/mission/{addr}",     async (string addr, IConfiguration cfg) => {
    var rpc = cfg["Cronos:Rpc"] ?? "https://evm.cronos.org";
    var web3 = new Web3(rpc);

    var wrap = await web3.Eth.GetContractQueryHandler<GetMissionDataFunction>()
        .QueryDeserializingToObjectAsync<MissionDataWrapper>(
            new GetMissionDataFunction(), addr, null);
    var md = wrap.Data; // tuple payload

    return Results.Ok(new {
        addr,
        players    = md.Players?.Count ?? 0,
        missionType= (int)md.MissionType,     // byte → int for readability
        roundsTotal= (int)md.MissionRounds,   // byte → int
        roundCount = (int)md.RoundCount,      // byte → int
        croStartWei= md.EthStart.ToString(),
        croCurrWei = md.EthCurrent.ToString()
    });
});

// Debug: ping a group to verify client subscription
app.MapGet("/debug/push/{addr}",        async (string addr, IHubContext<GameHub> hub) => {
    if (string.IsNullOrWhiteSpace(addr)) return Results.BadRequest("addr required");
    var g = addr.ToLowerInvariant();
    await hub.Clients.Group(g).SendAsync("ServerPing", $"Hello group: {g}");
    return Results.Ok(new { pushed = g });
});

app.MapGet("/debug/push-subs/{wallet}", async (string wallet, PushFanout fan) => {
    // intentionally minimal output so we can see if the server has anything to send to
    // uses the same DB read PushFanout uses
    var method = typeof(B6.Backend.Services.PushFanout).GetMethod("GetType"); // no-op to keep compiler happy with DI
    // We don't expose internals; just trigger a verbose send with no send:
    var (count, _) = await fan.DebugPushVerboseAsync(wallet);
    return Results.Ok(new { wallet = wallet.ToLowerInvariant(), subscriptionCount = count });
});

// Debug: prune push subscriptions for a wallet.
// If ?keep=<endpoint> is provided, keep only that endpoint; otherwise delete all.
app.MapDelete("/debug/push-subs/{wallet}", async (string wallet, HttpRequest req, PushFanout fan) =>{
    var keep = req.Query["keep"].ToString();
    var removed = await fan.PruneSubsAsync(wallet, string.IsNullOrWhiteSpace(keep) ? null : keep);
    return Results.Ok(new { wallet = wallet.ToLowerInvariant(), kept = string.IsNullOrWhiteSpace(keep) ? null : keep, removed });
});

app.MapMethods("/debug/push-web/{wallet}", new[] { "GET", "POST" }, async (string wallet, PushFanout fan) => {
    var (count, attempts) = await fan.DebugPushVerboseAsync(wallet);
    return Results.Ok(new { wallet = wallet.ToLowerInvariant(), subscriptionCount = count, attempts });
});

app.MapMethods("/debug/push-web-empty/{wallet}", new[] { "GET", "POST" }, async (string wallet, PushFanout fan) => {
    var (count, attempts) = await fan.DebugPushVerboseAsync(wallet, noPayload: true);
    return Results.Ok(new { wallet = wallet.ToLowerInvariant(), subscriptionCount = count, attempts });
});

// Inspect environment paths and process identity
app.MapGet("/debug/env",                      (IHostEnvironment env) => {
    return Results.Ok(new {
        env.ApplicationName,
        env.EnvironmentName,
        ContentRootPath = env.ContentRootPath,
        BaseDirectory   = AppContext.BaseDirectory,
        CurrentDir      = Environment.CurrentDirectory,
        User            = System.Security.Principal.WindowsIdentity.GetCurrent().Name
    });
});

/* ---------- HUB ---------- */
app.MapHub<GameHub>("/hub/game");

app.MapPost("/push/mission",            async (HttpRequest req, IConfiguration cfg, IHubContext<GameHub> hub, PushMissionDto body,  PushFanout fan) => {
    if (req.Headers["X-Push-Key"] != (cfg["Push:Key"] ?? "")) return Results.Unauthorized();

    var g = (body.Mission ?? "").ToLowerInvariant();
    await hub.Clients.Group(g).SendAsync("MissionUpdated", g);
    await fan.OnMissionUpdatedAsync(g); // schedule -1m warning, etc.
    return Results.Ok(new { pushed = true });
});

app.MapPost("/push/status",             async (HttpRequest req, IConfiguration cfg, IHubContext<GameHub> hub, PushStatusDto body,   PushFanout fan) => {
    if (req.Headers["X-Push-Key"] != (cfg["Push:Key"] ?? "")) return Results.Unauthorized();

    var g = (body.Mission ?? "").ToLowerInvariant();
    await hub.Clients.Group(g).SendAsync("StatusChanged", g, body.NewStatus);
    await fan.OnStatusChangedAsync(g, body.NewStatus); // schedule -5m prestart
    return Results.Ok(new { pushed = true });
});

app.MapPost("/push/round",              async (HttpRequest req, IConfiguration cfg, IHubContext<GameHub> hub, PushRoundDto body,    PushFanout fan) => {
    if (req.Headers["X-Push-Key"] != (cfg["Push:Key"] ?? "")) return Results.Unauthorized();

    var g = (body.Mission ?? "").ToLowerInvariant();
    await hub.Clients.Group(g).SendAsync("RoundResult", g, body.Round, body.Winner, body.AmountWei);
    await hub.Clients.Group(g).SendAsync("MissionUpdated", g);
    await fan.OnRoundAsync(g, body.Round, body.Winner, body.AmountWei); // inactive-only banked + schedule -10s cooldown
    return Results.Ok(new { pushed = true });
});

app.Run();



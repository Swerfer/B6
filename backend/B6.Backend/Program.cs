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
using System.Numerics;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using WebPush;               // Web Push VAPID support (new)

// The base uri is https://b6missions.com/api It is set to 'api' in ISS server.

// V4

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

// ===== Eligibility function DTOs (MissionFactory) =====
[Function("canEnroll", "bool")]
public class CanEnrollFunction : FunctionMessage{
    [Parameter("address", "player", 1)]
    public string Player { get; set; } = "";
}

[Function("secondsTillWeeklySlot", "uint256")]
public class SecondsTillWeeklySlotFunction : FunctionMessage{
    [Parameter("address", "player", 1)]
    public string Player { get; set; } = "";
}

[Function("secondsTillMonthlySlot", "uint256")]
public class SecondsTillMonthlySlotFunction : FunctionMessage{
    [Parameter("address", "player", 1)]
    public string Player { get; set; } = "";
}

[Function("getPlayerLimits", typeof(PlayerLimitsOutput))]
public class GetPlayerLimitsFunction : FunctionMessage{
    [Parameter("address", "player", 1)]
    public string Player { get; set; } = "";
}

[FunctionOutput]
public class PlayerLimitsOutput : IFunctionOutputDTO{
    // Matches core.js ABI: getPlayerLimits(address) returns(uint8,uint8,uint8,uint8,uint256,uint256)
    // (weeklyCount, monthlyCount, weeklyLimit, monthlyLimit, weeklyResetAt, monthlyResetAt)
    [Parameter("uint8",   "weeklyCount",    1)] public byte     WeeklyCount     { get; set; }
    [Parameter("uint8",   "monthlyCount",   2)] public byte     MonthlyCount    { get; set; }
    [Parameter("uint8",   "weeklyLimit",    3)] public byte     WeeklyLimit     { get; set; }
    [Parameter("uint8",   "monthlyLimit",   4)] public byte     MonthlyLimit    { get; set; }
    [Parameter("uint256", "weeklyResetAt",  5)] public BigInteger WeeklyResetAt { get; set; }
    [Parameter("uint256", "monthlyResetAt", 6)] public BigInteger MonthlyResetAt{ get; set; }
}

/* --------------------- Helpers ---------------------*/
static long         ToUnixSeconds(DateTime dtUtc){
    if (dtUtc.Kind != DateTimeKind.Utc)
        dtUtc = DateTime.SpecifyKind(dtUtc, DateTimeKind.Utc);
    return new DateTimeOffset(dtUtc).ToUnixTimeSeconds();
}

static string       GetRequired(IConfiguration cfg, string key){
    var v = cfg[key];
    if (string.IsNullOrWhiteSpace(v))
        throw new InvalidOperationException($"Missing configuration key: {key}");
    return v;
}

static async Task   KickMissionAsync(string mission, string? txHash, IConfiguration cfg, IHubContext<GameHub> hub){
    try
    {
        var cs = cfg.GetConnectionString("Db");
        await using var conn = new Npgsql.NpgsqlConnection(cs);
        await conn.OpenAsync();

        await using (var cmd = new Npgsql.NpgsqlCommand(@"
            insert into indexer_kicks (mission_address, tx_hash)
            values (@m, @h);", conn))
        {
            cmd.Parameters.AddWithValue("m", mission);
            cmd.Parameters.AddWithValue("h", (object?)txHash ?? DBNull.Value);
            await cmd.ExecuteNonQueryAsync();
        }

        await using (var cmd2 = new Npgsql.NpgsqlCommand("NOTIFY b6_indexer_kick, @payload;", conn))
        {
            cmd2.Parameters.AddWithValue("payload", mission);
            await cmd2.ExecuteNonQueryAsync();
        }

        await hub.Clients.Group(mission).SendAsync("MissionUpdated", mission);
    }
    catch (Exception ex)
    {
        // non-fatal: kick/notify failure shouldn't break response
        Console.WriteLine($"indexer kick failed for {mission}: {ex.Message}");
    }
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

    var rpc     = GetRequired(cfg, "Cronos:Rpc");

    var client = f.CreateClient();
    using var msg = new HttpRequestMessage(HttpMethod.Post, rpc);
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
app.MapGet("/secrets",                  async (HttpRequest req, IConfiguration cfg) =>{
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
        m.cro_initial_wei::text        as cro_initial_wei,
        m.cro_start_wei::text          as cro_start_wei,
        m.cro_current_wei::text        as cro_current_wei,
        m.pause_timestamp,
        m.updated_at,
        m.mission_created,
        m.round_pause_secs,
        m.last_round_pause_secs,
        m.creator_address,
        m.all_refunded,
        coalesce(c.enrolled,0)       as enrolled_players
    from missions m
    left join counts c using (mission_address)
    where m.status < 5
    order by m.enrollment_end asc nulls last, m.mission_end asc nulls last;";

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
            cro_initial_wei        = (string)rd["cro_initial_wei"],
            pause_timestamp        = rd["pause_timestamp"] is DBNull ? null : (long?) rd["pause_timestamp"],
            updated_at             = ToUnixSeconds(((DateTime) rd["updated_at"]).ToUniversalTime()),
            mission_created        = (long)  rd["mission_created"],
            round_pause_secs       = rd["round_pause_secs"] is DBNull ? null : (int?) rd["round_pause_secs"],
            last_round_pause_secs  = rd["last_round_pause_secs"] is DBNull ? null : (int?) rd["last_round_pause_secs"],
            creator_address        = rd["creator_address"] as string,
            all_refunded           = rd["all_refunded"] is DBNull ? false : (bool) rd["all_refunded"],
            enrolled_players       = (int)   rd["enrolled_players"]
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
        m.round_count,                               
        m.cro_initial_wei::text      as cro_initial_wei, 
        m.cro_start_wei::text        as cro_start_wei,  
        m.cro_current_wei::text      as cro_current_wei, 
        m.pause_timestamp,                            
        m.updated_at,                               
        m.mission_created,  
        m.round_pause_secs,
        m.last_round_pause_secs,                          
        m.creator_address,                           
        m.all_refunded,                               
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
            round_count            = (short) rd["round_count"],            
            cro_start_wei          = (string)rd["cro_start_wei"],          
            cro_current_wei        = (string)rd["cro_current_wei"],          
            cro_initial_wei        = (string)rd["cro_initial_wei"],          
            pause_timestamp        = rd["pause_timestamp"] is DBNull ? null : (long?) rd["pause_timestamp"],
            updated_at             = ToUnixSeconds(((DateTime) rd["updated_at"]).ToUniversalTime()),        
            mission_created        = (long)  rd["mission_created"],
            round_pause_secs       = rd["round_pause_secs"] is DBNull ? null : (int?) rd["round_pause_secs"],
            last_round_pause_secs  = rd["last_round_pause_secs"] is DBNull ? null : (int?) rd["last_round_pause_secs"],       
            creator_address        = rd["creator_address"] as string,      
            all_refunded           = rd["all_refunded"] is DBNull ? false : (bool) rd["all_refunded"],
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
        m.enrollment_start,
        m.enrollment_end,
        m.enrollment_amount_wei::text  as enrollment_amount_wei,
        m.enrollment_min_players,
        m.enrollment_max_players,
        m.mission_start,
        m.mission_end,
        m.mission_rounds_total,
        m.round_count,                               
        m.cro_initial_wei::text      as cro_initial_wei, 
        m.cro_start_wei::text        as cro_start_wei,  
        m.cro_current_wei::text      as cro_current_wei, 
        m.pause_timestamp,                            
        m.updated_at,                               
        m.mission_created,  
        m.round_pause_secs,
        m.last_round_pause_secs,                          
        m.creator_address,                           
        m.all_refunded,                          
        coalesce(c.enrolled,0) as enrolled_players,
        case 
          when m.status >= 5 then
            case
              when m.all_refunded is true then 'all_refunded'
              when coalesce(c.enrolled,0) < m.enrollment_min_players then 'not_enough_players'
              when m.round_count = 0 then 'no_rounds_played'
              else 'ended'
            end
          else null
        end as failure_reason,
        e.enrolled_at,
        e.refunded,
        e.refund_tx_hash
    from mission_enrollments e
    join missions m using (mission_address)
    left join counts c using (mission_address)
    where lower(e.player_address) = @p
    order by m.status asc, m.mission_end asc nulls last;
    ";

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
            enrollment_start      = (long)  rd["enrollment_start"],
            enrollment_end        = (long)  rd["enrollment_end"],
            enrollment_amount_wei = (string)rd["enrollment_amount_wei"],
            enrollment_max_players= (short) rd["enrollment_max_players"],
            mission_start         = (long)  rd["mission_start"],
            mission_end           = (long)  rd["mission_end"],
            mission_rounds_total  = (short) rd["mission_rounds_total"],
            round_count           = (short) rd["round_count"],
            cro_start_wei         = (string)rd["cro_start_wei"],
            cro_current_wei       = (string)rd["cro_current_wei"],
            cro_initial_wei       = (string)rd["cro_initial_wei"],
            pause_timestamp       = rd["pause_timestamp"] is DBNull ? null : (long?) rd["pause_timestamp"],
            updated_at            = ToUnixSeconds(((DateTime) rd["updated_at"]).ToUniversalTime()),
            mission_created       = (long)  rd["mission_created"],
            round_pause_secs      = rd["round_pause_secs"] is DBNull ? null : (int?) rd["round_pause_secs"],
            last_round_pause_secs = rd["last_round_pause_secs"] is DBNull ? null : (int?) rd["last_round_pause_secs"],
            creator_address       = rd["creator_address"] as string,
            all_refunded          = rd["all_refunded"] is DBNull ? false : (bool) rd["all_refunded"],
            enrolled_players      = (int)   rd["enrolled_players"],      
            failure_reason        = rd["failure_reason"] as string,     
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
        m.cro_initial_wei::text        as cro_initial_wei,
        m.pause_timestamp,
        m.updated_at,
        m.mission_created,
        m.round_pause_secs,
        m.last_round_pause_secs,
        m.creator_address,
        m.all_refunded,
        coalesce(c.enrolled,0)       as enrolled_players
      from missions m
      left join counts c using (mission_address)
      where lower(mission_address) = @a;";

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
        mission_start          = (long)  rd["mission_start"],
        mission_end            = (long)  rd["mission_end"],
        mission_rounds_total   = (short) rd["mission_rounds_total"],
        round_count            = (short) rd["round_count"],
        cro_start_wei          = (string)rd["cro_start_wei"],
        cro_current_wei        = (string)rd["cro_current_wei"],
        cro_initial_wei        = (string)rd["cro_initial_wei"],
        pause_timestamp        = rd["pause_timestamp"] is DBNull ? null : (long?) rd["pause_timestamp"],
        updated_at             = ToUnixSeconds(((DateTime) rd["updated_at"]).ToUniversalTime()),
        mission_created        = (long)  rd["mission_created"],
        round_pause_secs       = rd["round_pause_secs"] is DBNull ? null : (int?) rd["round_pause_secs"],
        last_round_pause_secs  = rd["last_round_pause_secs"] is DBNull ? null : (int?) rd["last_round_pause_secs"],
        creator_address        = rd["creator_address"] as string,
        all_refunded           = rd["all_refunded"] is DBNull ? false : (bool) rd["all_refunded"],
        enrolled_players       = (int)   rd["enrolled_players"]
    };
    await rd.CloseAsync();

    // 2) enrollments
    var enSql = @"
      select player_address, refunded, coalesce(refund_tx_hash,'') as refund_tx_hash, enrolled_at
      from mission_enrollments
      where lower(mission_address) = @a
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
      where lower(mission_address) = @a
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

/***********************
 *  PLAYERS – READ API
 *  GET /players/{addr}/eligibility
 *  -> delegates to MissionFactory.canEnroll(address) and related views
 ***********************/
app.MapGet("/players/{addr}/eligibility", async (string addr, IConfiguration cfg) =>{
    if (string.IsNullOrWhiteSpace(addr)) return Results.BadRequest("Missing address");
    addr = addr.ToLowerInvariant();
    if (!addr.StartsWith("0x") || addr.Length != 42) return Results.BadRequest("Invalid address");

    var rpc     = GetRequired(cfg, "Cronos:Rpc");
    var factory = GetRequired(cfg, "Contracts:Factory");
    if (string.IsNullOrWhiteSpace(factory)) return Results.Problem("Factory address not configured");
    factory = factory.ToLowerInvariant();

    var web3 = new Web3(rpc);

    // Query canEnroll + cooldowns + current counters/limits
    var canQ   = web3.Eth.GetContractQueryHandler<CanEnrollFunction>();
    var wsecQ  = web3.Eth.GetContractQueryHandler<SecondsTillWeeklySlotFunction>();
    var msecQ  = web3.Eth.GetContractQueryHandler<SecondsTillMonthlySlotFunction>();
    var limQ   = web3.Eth.GetContractQueryHandler<GetPlayerLimitsFunction>();

    bool        canEnroll;
    BigInteger  weeklyLeft, monthlyLeft;
    PlayerLimitsOutput? limits;

    try
    {
        canEnroll   = await canQ.QueryAsync<bool>(factory, new CanEnrollFunction { Player = addr });
        weeklyLeft  = await wsecQ.QueryAsync<BigInteger>(factory, new SecondsTillWeeklySlotFunction { Player = addr });
        monthlyLeft = await msecQ.QueryAsync<BigInteger>(factory, new SecondsTillMonthlySlotFunction { Player = addr });
        limits      = await limQ.QueryDeserializingToObjectAsync<PlayerLimitsOutput>(
                        new GetPlayerLimitsFunction { Player = addr }, factory, null);
    }
    catch (Exception ex)
    {
        // Surface as 200 with error so frontend can still render a generic join CTA.
        return Results.Ok(new {
            address = addr,
            error   = true,
            message = ex.Message
        });
    }

    // Build a human-friendly reason if NOT eligible
    string? reason = null;
    var wLeft = (long)weeklyLeft;
    var mLeft = (long)monthlyLeft;

    if (!canEnroll)
    {
        if (wLeft > 0)      reason = $"Weekly limit reached. Next slot in {wLeft} seconds";
        else if (mLeft > 0) reason = $"Monthly limit reached. Next slot in {mLeft} seconds";
        else                reason = $"Not eligible to enroll right now (contract rules)";
    }

    // Normalize output
    return Results.Ok(new {
        address             = addr,
        can_enroll          = canEnroll,
        weekly_seconds_left = wLeft,
        monthly_seconds_left= mLeft,
        weekly_count        = (int)(limits?.WeeklyCount  ?? 0),
        monthly_count       = (int)(limits?.MonthlyCount ?? 0),
        weekly_limit        = (int)(limits?.WeeklyLimit  ?? 0),
        monthly_limit       = (int)(limits?.MonthlyLimit ?? 0),
        weekly_reset_at     = limits?.WeeklyResetAt  is null ? (long?)null : (long)limits.WeeklyResetAt,
        monthly_reset_at    = limits?.MonthlyResetAt is null ? (long?)null : (long)limits.MonthlyResetAt,
        reason
    });
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
    // basic address validation to avoid noisy calls
    if (string.IsNullOrWhiteSpace(addr) || !addr.StartsWith("0x") || addr.Length != 42)
        return Results.Json(new { error = true, message = "Invalid address format" });

    var rpc     = GetRequired(cfg, "Cronos:Rpc");
    var web3 = new Web3(rpc);

    try
    {
        var wrap = await web3.Eth.GetContractQueryHandler<GetMissionDataFunction>()
            .QueryDeserializingToObjectAsync<MissionDataWrapper>(
                new GetMissionDataFunction(), addr, null);

        if (wrap == null)
            return Results.Json(new { error = true, message = "No response from getMissionData (wrap == null)" });

        if (wrap.Data == null)
            return Results.Json(new { error = true, message = "getMissionData returned null Data (not a Mission contract?)" });

        var md = wrap.Data; // tuple payload

        return Results.Ok(new {
            addr,
            players    = md.Players?.Count ?? 0,
            missionType= (int)md.MissionType,     // byte → int for readability
            roundsTotal= (int)md.MissionRounds,   // byte → int
            roundCount = (int)md.RoundCount,      // byte → int
            croStartWei= md.CroStart.ToString(),
            croCurrWei = md.CroCurrent.ToString()
        });
    }
    catch (Exception ex)
    {
        var msg = $"{ex.GetType().FullName}: {ex.Message}";
        // Always return 200 JSON so IIS doesn't hide it
        return Results.Json(new { error = true, message = msg });
    }
});

// Debug: ping a group to verify client subscription
app.MapGet("/debug/push/{addr}",        async (string addr, IHubContext<GameHub> hub) => {
    if (string.IsNullOrWhiteSpace(addr)) return Results.BadRequest("addr required");
    var g = addr.ToLowerInvariant();
    await hub.Clients.Group(g).SendAsync("ServerPing", $"Hello group: {g}");
    return Results.Ok(new { pushed = g });
});

app.MapGet("/debug/push-subs/{wallet}",                                     async (string wallet,                  PushFanout fan) => {
    // intentionally minimal output so we can see if the server has anything to send to
    // uses the same DB read PushFanout uses
    var method = typeof(B6.Backend.Services.PushFanout).GetMethod("GetType"); // no-op to keep compiler happy with DI
    // We don't expose internals; just trigger a verbose send with no send:
    var (count, _) = await fan.DebugPushVerboseAsync(wallet);
    return Results.Ok(new { wallet = wallet.ToLowerInvariant(), subscriptionCount = count });
});

// Debug: prune push subscriptions for a wallet.
app.MapDelete("/debug/push-subs/{wallet}",                                  async (string wallet, HttpRequest req, PushFanout fan) => {
    var keep = req.Query["keep"].ToString();
    var removed = await fan.PruneSubsAsync(wallet, string.IsNullOrWhiteSpace(keep) ? null : keep);
    return Results.Ok(new { wallet = wallet.ToLowerInvariant(), kept = string.IsNullOrWhiteSpace(keep) ? null : keep, removed });
});

app.MapMethods("/debug/push-web/{wallet}",        new[] { "GET", "POST" },  async (string wallet,                  PushFanout fan) => {
    var (count, attempts) = await fan.DebugPushVerboseAsync(wallet);
    return Results.Ok(new { wallet = wallet.ToLowerInvariant(), subscriptionCount = count, attempts });
});

app.MapMethods("/debug/push-web-empty/{wallet}",  new[] { "GET", "POST" },  async (string wallet,                  PushFanout fan) => {
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

// Daily error rollup from the indexer (benign provider hiccups etc.)
app.MapGet("/debug/indexer/errors",     async (HttpRequest req, IConfiguration cfg) =>{
    // Optional ?day=YYYY-MM-DD; defaults to today (UTC)
    var dayStr = req.Query["day"].ToString();
    DateTime dayUtc;
    if (!DateTime.TryParse(dayStr, out dayUtc)) dayUtc = DateTime.UtcNow.Date;

    var cs = cfg.GetConnectionString("Db");
    await using var conn = new NpgsqlConnection(cs);
    await conn.OpenAsync();

    await using var cmd = new NpgsqlCommand(@"
        select err_key, count, updated_at
        from indexer_benign_errors
        where day = @d
        order by err_key;", conn);
    cmd.Parameters.AddWithValue("d", dayUtc.Date);

    var items = new List<object>();
    int total = 0;
    await using var rd = await cmd.ExecuteReaderAsync();
    while (await rd.ReadAsync())
    {
        var key   = rd["err_key"] as string ?? "";
        var count = (int) rd["count"];
        var upd   = (DateTime) rd["updated_at"];
        total += count;
        items.Add(new { key, count, updated_at = upd.ToUniversalTime().ToString("o") });
    }

    return Results.Ok(new {
        day = dayUtc.ToString("yyyy-MM-dd"),
        total,
        items
    });
});

// backend/B6.Backend/Program.cs  // locator: HUB & push routes section

/* ---------- HUB ---------- */
app.MapHub<GameHub>("/hub/game");

var createdPingThrottle = new System.Collections.Concurrent.ConcurrentDictionary<string, DateTime>(StringComparer.OrdinalIgnoreCase);
var enrollPingThrottle  = new System.Collections.Concurrent.ConcurrentDictionary<string, DateTime>(StringComparer.OrdinalIgnoreCase);
var bankPingThrottle    = new System.Collections.Concurrent.ConcurrentDictionary<string, DateTime>(StringComparer.OrdinalIgnoreCase);

app.MapPost("/events/created",          async (HttpRequest req, IConfiguration cfg, IHubContext<GameHub> hub                                      ) => {
    string mission = null;
    string txHash  = null; // optional – if you want to verify like /events/banked

    try
    {
        using var doc = await System.Text.Json.JsonDocument.ParseAsync(req.Body);
        var root = doc.RootElement;

        mission = root.TryGetProperty("mission", out var m) ? m.GetString() : null;
        txHash  = root.TryGetProperty("txHash",  out var h) ? h.GetString() : null;
    }
    catch
    {
        return Results.BadRequest("Invalid JSON");
    }

    if (string.IsNullOrWhiteSpace(mission))
        return Results.BadRequest("Missing mission");

    mission = mission.ToLowerInvariant();

    // Throttle: once per ~2s per mission (light abuse protection)
    var now = DateTime.UtcNow;
    if (createdPingThrottle.TryGetValue(mission, out var prev) && (now - prev) < TimeSpan.FromSeconds(2))
        return Results.Ok(new { pushed = false, reason = "throttled" });
    createdPingThrottle[mission] = now;

    // Optional: verify tx (skip if txHash not provided)
    if (!string.IsNullOrWhiteSpace(txHash))
    {
        var rpc     = GetRequired(cfg, "Cronos:Rpc");
        var web3 = new Nethereum.Web3.Web3(rpc);

        var tx = await web3.Eth.Transactions.GetTransactionByHash.SendRequestAsync(txHash);
        if (tx == null) return Results.BadRequest("Transaction not found");
        var rc = await web3.Eth.Transactions.GetTransactionReceipt.SendRequestAsync(txHash);
        if (rc == null || rc.Status == null || rc.Status.Value != 1)
            return Results.BadRequest("Transaction not successful");
    }

    await KickMissionAsync(mission, txHash, cfg, hub);

    return Results.Ok(new { pushed = true });
});

app.MapPost("/events/enrolled",         async (HttpRequest req, IConfiguration cfg, IHubContext<GameHub> hub                                      ) => {
    string mission = null;
    string player  = null;
    string txHash  = null; // optional – verify if you want

    try
    {
        using var doc = await System.Text.Json.JsonDocument.ParseAsync(req.Body);
        var root = doc.RootElement;

        mission = root.TryGetProperty("mission", out var m) ? m.GetString() : null;
        player  = root.TryGetProperty("player",  out var p) ? p.GetString() : null;
        txHash  = root.TryGetProperty("txHash",  out var h) ? h.GetString() : null;
    }
    catch
    {
        return Results.BadRequest("Invalid JSON");
    }

    if (string.IsNullOrWhiteSpace(mission))
        return Results.BadRequest("Missing mission");

    mission = mission.ToLowerInvariant();

    // Throttle per mission: once per ~2s
    var now = DateTime.UtcNow;
    if (enrollPingThrottle.TryGetValue(mission, out var prev) && (now - prev) < TimeSpan.FromSeconds(2))
        return Results.Ok(new { pushed = false, reason = "throttled" });
    enrollPingThrottle[mission] = now;

    await KickMissionAsync(mission, txHash, cfg, hub);

    return Results.Ok(new { pushed = true });
});

app.MapPost("/events/banked",           async (HttpRequest req, IConfiguration cfg, IHubContext<GameHub> hub                                      ) => {
    string mission = null;
    string txHash  = null;

    try
    {
        using var doc = await System.Text.Json.JsonDocument.ParseAsync(req.Body);
        var root = doc.RootElement;

        mission = root.TryGetProperty("mission", out var m) ? m.GetString() : null;
        txHash  = root.TryGetProperty("txHash",  out var h) ? h.GetString() : null;
    }
    catch
    {
        return Results.BadRequest("Invalid JSON");
    }

    if (string.IsNullOrWhiteSpace(mission) || string.IsNullOrWhiteSpace(txHash))
        return Results.BadRequest("Missing mission or txHash");

    mission = mission.ToLowerInvariant();

    // Throttle: once per ~2s per mission (light abuse protection)
    var now = DateTime.UtcNow;
    if (bankPingThrottle.TryGetValue(mission, out var prev) && (now - prev) < TimeSpan.FromSeconds(2))
        return Results.Ok(new { pushed = false, reason = "throttled" });
    bankPingThrottle[mission] = now;

    var rpc     = GetRequired(cfg, "Cronos:Rpc");
    var web3 = new Nethereum.Web3.Web3(rpc);

    // Verify the tx exists, is to this mission, and succeeded
    var tx = await web3.Eth.Transactions.GetTransactionByHash.SendRequestAsync(txHash);
    if (tx == null) return Results.BadRequest("Transaction not found");

    var txTo = (tx.To ?? string.Empty).ToLowerInvariant();
    if (txTo != mission) return Results.BadRequest("Transaction target mismatch");

    var rc = await web3.Eth.Transactions.GetTransactionReceipt.SendRequestAsync(txHash);
    if (rc == null || rc.Status == null || rc.Status.Value != 1)
        return Results.BadRequest("Transaction not successful");

    await KickMissionAsync(mission, txHash, cfg, hub);

    return Results.Ok(new { pushed = true });
});

app.MapPost("/push/mission",            async (HttpRequest req, IConfiguration cfg, IHubContext<GameHub> hub, PushMissionDto body,  PushFanout fan) => {
    if (req.Headers["X-Push-Key"] != (cfg["Push:Key"] ?? "")) return Results.Unauthorized();

    var g = (body.Mission ?? "").ToLowerInvariant();
    await hub.Clients.Group(g).SendAsync("MissionUpdated", g);
    await fan.OnMissionUpdatedAsync(g); // schedule -1m warning, etc.
    return Results.Ok(new { pushed = true });
});

app.MapPost("/push/status",             async (HttpRequest req, IConfiguration cfg, IHubContext<GameHub> hub, PushStatusDto  body,  PushFanout fan) => {
    if (req.Headers["X-Push-Key"] != (cfg["Push:Key"] ?? "")) return Results.Unauthorized();

    var g = (body.Mission ?? "").ToLowerInvariant();
    await hub.Clients.Group(g).SendAsync("StatusChanged", g, body.NewStatus);
    await fan.OnStatusChangedAsync(g, body.NewStatus); // schedule -5m prestart
    return Results.Ok(new { pushed = true });
});

app.MapPost("/push/round",              async (HttpRequest req, IConfiguration cfg, IHubContext<GameHub> hub, PushRoundDto   body,  PushFanout fan) => {
    if (req.Headers["X-Push-Key"] != (cfg["Push:Key"] ?? "")) return Results.Unauthorized();

    var g = (body.Mission ?? "").ToLowerInvariant();
    await hub.Clients.Group(g).SendAsync("RoundResult", g, body.Round, body.Winner, body.AmountWei);
    await hub.Clients.Group(g).SendAsync("MissionUpdated", g);
    await fan.OnRoundAsync(g, body.Round, body.Winner, body.AmountWei); // inactive-only banked + schedule -10s cooldown
    return Results.Ok(new { pushed = true });
});

app.Run();

using Azure.Identity;
using Azure.Extensions.AspNetCore.Configuration.Secrets;
using Azure.Security.KeyVault.Secrets;
using B6.Backend;
using B6.Backend.Hubs;
using B6.Contracts; 
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

var app = builder.Build();

app.UseCors("AllowFrontend");

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

static async Task   KickMissionAsync(string mission, string? txHash, string? eventType, IConfiguration cfg, IHubContext<GameHub> hub){
    try
    {
        var cs = cfg.GetConnectionString("Db");
        await using var conn = new Npgsql.NpgsqlConnection(cs);
        await conn.OpenAsync();

        await using (var cmd = new Npgsql.NpgsqlCommand(@"
            insert into indexer_kicks (mission_address, tx_hash, event_type)
            values (@m, @h, @e);", conn))
        {
            cmd.Parameters.AddWithValue("m", mission);
            cmd.Parameters.AddWithValue("h", (object?)txHash    ?? DBNull.Value);
            cmd.Parameters.AddWithValue("e", (object?)eventType ?? DBNull.Value);
            await cmd.ExecuteNonQueryAsync();

            Console.WriteLine($"[API] indexer_kicks INSERT mission={mission} event={eventType ?? "-"} tx={txHash ?? "-"} {DateTime.UtcNow:o}");
        }

        await using (var cmd2 = new Npgsql.NpgsqlCommand("NOTIFY b6_indexer_kick, @payload;", conn))
        {
            cmd2.Parameters.AddWithValue("payload", mission);
            await cmd2.ExecuteNonQueryAsync();

            Console.WriteLine($"[API] NOTIFY b6_indexer_kick mission={mission} {DateTime.UtcNow:o}");
        }

        // Let the indexer handle the kick and push MissionUpdated once the snapshot is refreshed.
    }
    catch (Exception ex)
    {
        // non-fatal: kick/notify failure shouldn't break response
        Console.WriteLine($"indexer kick failed for {mission}: {ex.Message}");
    }
}

static async Task   InsertMissionTxAsync(IConfiguration cfg, string mission, string? player, string eventType, string txHash, long? blockNumber){
    try
    {
        if (string.IsNullOrWhiteSpace(mission))   throw new ArgumentException("mission is required", nameof(mission));
        if (string.IsNullOrWhiteSpace(eventType)) throw new ArgumentException("eventType is required", nameof(eventType));
        if (string.IsNullOrWhiteSpace(txHash))    throw new ArgumentException("txHash is required", nameof(txHash));

        var cs = cfg.GetConnectionString("Db");
        await using var conn = new Npgsql.NpgsqlConnection(cs);
        await conn.OpenAsync();

        await using (var cmd = new Npgsql.NpgsqlCommand(@"
            insert into mission_tx (mission_address, player_address, event_type, tx_hash, block_number)
            values (@m, @p, @e, @h, @b);", conn))
        {
            cmd.Parameters.AddWithValue("m", mission);
            cmd.Parameters.AddWithValue("p", (object?)player      ?? DBNull.Value);
            cmd.Parameters.AddWithValue("e", eventType);
            cmd.Parameters.AddWithValue("h", txHash);
            cmd.Parameters.AddWithValue("b", (object?)blockNumber ?? DBNull.Value);

            await cmd.ExecuteNonQueryAsync();

            Console.WriteLine($"[API] mission_tx INSERT mission={mission} player={player ?? "-"} event={eventType} tx={txHash} block={blockNumber?.ToString() ?? "-"} {DateTime.UtcNow:o}");
        }
    }
    catch (Exception ex)
    {
        // logging mag best simpel blijven; dit mag de API niet breken
        Console.WriteLine($"mission_tx insert failed for {mission} {txHash}: {ex.Message}");
    }
}

/* ------------------- API endpoints ----------------- */

// /       -> health check
app.MapGet("/",                               ()                                        => // health check 
    Results.Ok("OK")
);

// /api/config -> shared runtime config for frontend
app.MapGet("/config",                         (IConfiguration cfg)                      => { // frontend config
    var rpc     = GetRequired(cfg, "Cronos:Rpc");
    var factory = GetRequired(cfg, "Contracts:Factory");
    return Results.Ok(new { rpc, factory });
});

// /api/rpc -> reverse-proxy JSON-RPC to Cronos node (to avoid CORS issues)
app.MapPost("/rpc",                     async (HttpRequest req, IHttpClientFactory f, IConfiguration cfg) => { // RPC proxy
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
app.MapGet("/secrets",                  async (HttpRequest req, IConfiguration cfg)     =>{ // list Key Vault secrets
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

/***********************
 *  MISSIONS – READ API
 ***********************/

// GET /missions/all  → full snapshot for the All Missions page (DB, no RPC)
app.MapGet("/missions/all",             async (IConfiguration cfg)                      => { // all missions
    var cs = cfg.GetConnectionString("Db");
    await using var conn = new NpgsqlConnection(cs);
    await conn.OpenAsync();

    var sql = @"
    with counts as (
      select mission_address, count(*)::int as enrolled
      from players
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
      coalesce(c.enrolled,0)         as enrolled_players
    from missions m
    left join counts c using (mission_address)
    order by m.mission_created desc, m.mission_address desc;";

    await using var cmd = new NpgsqlCommand(sql, conn);
    await using var rd  = await cmd.ExecuteReaderAsync();

    var list = new List<object>();
    while (await rd.ReadAsync())
    {
        list.Add(new {
            mission_address        = rd["mission_address"] as string,
            name                   = rd["name"] as string,
            mission_type           = Convert.ToInt16(rd["mission_type"]),
            status                 = Convert.ToInt16(rd["status"]),  // DB truth
            enrollment_start       = Convert.ToInt64(rd["enrollment_start"]),
            enrollment_end         = Convert.ToInt64(rd["enrollment_end"]),
            enrollment_amount_wei  = rd["enrollment_amount_wei"]?.ToString(),
            enrollment_min_players = Convert.ToInt16(rd["enrollment_min_players"]),
            enrollment_max_players = Convert.ToInt16(rd["enrollment_max_players"]),
            mission_start          = Convert.ToInt64(rd["mission_start"]),
            mission_end            = Convert.ToInt64(rd["mission_end"]),
            mission_rounds_total   = Convert.ToInt16(rd["mission_rounds_total"]),
            round_count            = Convert.ToInt16(rd["round_count"]),
            cro_start_wei          = rd["cro_start_wei"]?.ToString(),
            cro_current_wei        = rd["cro_current_wei"]?.ToString(),
            cro_initial_wei        = rd["cro_initial_wei"]?.ToString(),
            pause_timestamp        = rd["pause_timestamp"] is DBNull ? (long?)null : Convert.ToInt64(rd["pause_timestamp"]),
            updated_at             = ToUnixSeconds(((DateTime) rd["updated_at"]).ToUniversalTime()),
            mission_created        = Convert.ToInt64(rd["mission_created"]),
            round_pause_secs       = rd["round_pause_secs"] is DBNull ? (int?)null : Convert.ToInt32(rd["round_pause_secs"]),
            last_round_pause_secs  = rd["last_round_pause_secs"] is DBNull ? (int?)null : Convert.ToInt32(rd["last_round_pause_secs"]),
            creator_address        = rd["creator_address"] as string,
            all_refunded           = rd["all_refunded"] is DBNull ? false : (bool) rd["all_refunded"],
            enrolled_players       = Convert.ToInt32(rd["enrolled_players"])
        });
    }

    return Results.Ok(list);
});

// GET /missions/all/{n}  → latest n missions (DB, no RPC)
app.MapGet("/missions/all/{n}",         async (int n, IConfiguration cfg)               => { // latest n missions
    var cs = cfg.GetConnectionString("Db");
    await using var conn = new NpgsqlConnection(cs);
    await conn.OpenAsync();

    var sql = @"
    with counts as (
      select mission_address, count(*)::int as enrolled
      from players
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
      coalesce(c.enrolled,0)         as enrolled_players
    from missions m
    left join counts c using (mission_address)
    order by m.mission_created desc, m.mission_address desc 
    limit @n;";

    await using var cmd = new NpgsqlCommand(sql, conn);
    cmd.Parameters.AddWithValue("n", n);
    await using var rd  = await cmd.ExecuteReaderAsync();

    var list = new List<object>();
    while (await rd.ReadAsync())
    {
        list.Add(new {
            mission_address        = rd["mission_address"] as string,
            name                   = rd["name"] as string,
            mission_type           = Convert.ToInt16(rd["mission_type"]),
            status                 = Convert.ToInt16(rd["status"]), // DB truth
            enrollment_start       = Convert.ToInt64(rd["enrollment_start"]),
            enrollment_end         = Convert.ToInt64(rd["enrollment_end"]),
            enrollment_amount_wei  = rd["enrollment_amount_wei"]?.ToString(),
            enrollment_min_players = Convert.ToInt16(rd["enrollment_min_players"]),
            enrollment_max_players = Convert.ToInt16(rd["enrollment_max_players"]),
            mission_start          = Convert.ToInt64(rd["mission_start"]),
            mission_end            = Convert.ToInt64(rd["mission_end"]),
            mission_rounds_total   = Convert.ToInt16(rd["mission_rounds_total"]),
            round_count            = Convert.ToInt16(rd["round_count"]),
            cro_start_wei          = rd["cro_start_wei"]?.ToString(),
            cro_current_wei        = rd["cro_current_wei"]?.ToString(),
            cro_initial_wei        = rd["cro_initial_wei"]?.ToString(),
            pause_timestamp        = rd["pause_timestamp"] is DBNull ? (long?)null : Convert.ToInt64(rd["pause_timestamp"]),
            updated_at             = ToUnixSeconds(((DateTime) rd["updated_at"]).ToUniversalTime()),
            mission_created        = Convert.ToInt64(rd["mission_created"]),
            round_pause_secs       = rd["round_pause_secs"] is DBNull ? (int?)null : Convert.ToInt32(rd["round_pause_secs"]),
            last_round_pause_secs  = rd["last_round_pause_secs"] is DBNull ? (int?)null : Convert.ToInt32(rd["last_round_pause_secs"]),
            creator_address        = rd["creator_address"] as string,
            all_refunded           = rd["all_refunded"] is DBNull ? false : (bool) rd["all_refunded"],
            enrolled_players       = Convert.ToInt32(rd["enrolled_players"])
        });
    }

    return Results.Ok(list);
});

// GET /missions/not-ended  → missions not yet ended (DB, no RPC)
app.MapGet("/missions/not-ended",       async (IConfiguration cfg)                      => { // missions not yet ended
    var cs = cfg.GetConnectionString("Db");
    await using var conn = new NpgsqlConnection(cs);
    await conn.OpenAsync();

    // status < 5  → Pending/Enrolling/Arming/Active/Paused (not ended)
    var sql = @"
    with counts as (
        select mission_address, count(*)::int as enrolled
        from players
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
            mission_type           = Convert.ToInt16(rd["mission_type"]),
            status                 = Convert.ToInt16(rd["status"]),
            enrollment_start       = Convert.ToInt64(rd["enrollment_start"]),
            enrollment_end         = Convert.ToInt64(rd["enrollment_end"]),
            enrollment_amount_wei  = rd["enrollment_amount_wei"]?.ToString(),
            enrollment_min_players = Convert.ToInt16(rd["enrollment_min_players"]),
            enrollment_max_players = Convert.ToInt16(rd["enrollment_max_players"]),
            mission_start          = Convert.ToInt64(rd["mission_start"]),
            mission_end            = Convert.ToInt64(rd["mission_end"]),
            mission_rounds_total   = Convert.ToInt16(rd["mission_rounds_total"]),
            round_count            = Convert.ToInt16(rd["round_count"]),
            cro_start_wei          = rd["cro_start_wei"]?.ToString(),
            cro_current_wei        = rd["cro_current_wei"]?.ToString(),
            cro_initial_wei        = rd["cro_initial_wei"]?.ToString(),
            pause_timestamp        = rd["pause_timestamp"] is DBNull ? (long?)null : Convert.ToInt64(rd["pause_timestamp"]),
            updated_at             = ToUnixSeconds(((DateTime) rd["updated_at"]).ToUniversalTime()),
            mission_created        = Convert.ToInt64(rd["mission_created"]),
            round_pause_secs       = rd["round_pause_secs"] is DBNull ? (int?)null : Convert.ToInt32(rd["round_pause_secs"]),
            last_round_pause_secs  = rd["last_round_pause_secs"] is DBNull ? (int?)null : Convert.ToInt32(rd["last_round_pause_secs"]),
            creator_address        = rd["creator_address"] as string,
            all_refunded           = rd["all_refunded"] is DBNull ? false : (bool) rd["all_refunded"],
            enrolled_players       = Convert.ToInt32(rd["enrolled_players"])
        });
    }

    return Results.Ok(list);
});

// GET /missions/joinable  → missions open for enrollment (DB, no RPC)
app.MapGet("/missions/joinable",        async (IConfiguration cfg)                      => { // missions open for enrollment
    var cs = cfg.GetConnectionString("Db");
    await using var conn = new NpgsqlConnection(cs);
    await conn.OpenAsync();

    var sql = @"
      with counts as (
        select mission_address, count(*)::int as enrolled
        from players
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
            mission_type           = Convert.ToInt16(rd["mission_type"]),
            status                 = Convert.ToInt16(rd["status"]),
            enrollment_start       = Convert.ToInt64(rd["enrollment_start"]),
            enrollment_end         = Convert.ToInt64(rd["enrollment_end"]),
            enrollment_amount_wei  = rd["enrollment_amount_wei"]?.ToString(),
            enrollment_min_players = Convert.ToInt16(rd["enrollment_min_players"]),
            enrollment_max_players = Convert.ToInt16(rd["enrollment_max_players"]),
            mission_start          = Convert.ToInt64(rd["mission_start"]),
            mission_end            = Convert.ToInt64(rd["mission_end"]),
            mission_rounds_total   = Convert.ToInt16(rd["mission_rounds_total"]),
            round_count            = Convert.ToInt16(rd["round_count"]),
            cro_start_wei          = rd["cro_start_wei"]?.ToString(),
            cro_current_wei        = rd["cro_current_wei"]?.ToString(),
            cro_initial_wei        = rd["cro_initial_wei"]?.ToString(),
            pause_timestamp        = rd["pause_timestamp"] is DBNull ? (long?)null : Convert.ToInt64(rd["pause_timestamp"]),
            updated_at             = ToUnixSeconds(((DateTime) rd["updated_at"]).ToUniversalTime()),
            mission_created        = Convert.ToInt64(rd["mission_created"]),
            round_pause_secs       = rd["round_pause_secs"] is DBNull ? (int?)null : Convert.ToInt32(rd["round_pause_secs"]),
            last_round_pause_secs  = rd["last_round_pause_secs"] is DBNull ? (int?)null : Convert.ToInt32(rd["last_round_pause_secs"]),
            creator_address        = rd["creator_address"] as string,
            all_refunded           = rd["all_refunded"] is DBNull ? false : (bool) rd["all_refunded"],
            enrolled_players       = Convert.ToInt32(rd["enrolled_players"])
        });
    }

    return Results.Ok(list);
});

// GET /missions/player/{addr}  → all missions a player is enrolled in (DB, no RPC)
app.MapGet("/missions/player/{addr}",   async (string addr, IConfiguration cfg)         => { // all missions a player is enrolled in
    if (string.IsNullOrWhiteSpace(addr)) return Results.BadRequest("Missing address");
    addr = addr.ToLowerInvariant();

    var cs = cfg.GetConnectionString("Db");
    await using var conn = new NpgsqlConnection(cs);
    await conn.OpenAsync();

    var sql = @"
        WITH counts AS (
            SELECT mission_address, COUNT(*)::int AS enrolled
            FROM players
            GROUP BY mission_address
        ),
        player_missions AS (
            SELECT DISTINCT mission_address
            FROM players
            WHERE lower(player) = lower(trim(@p))
        )
        SELECT
            m.mission_address,
            m.name,
            m.mission_type,
            m.status,
            m.enrollment_start,
            m.enrollment_end,
            m.enrollment_amount_wei::text  AS enrollment_amount_wei,
            m.enrollment_min_players,
            m.enrollment_max_players,
            m.mission_start,
            m.mission_end,
            m.mission_rounds_total,
            m.round_count,
            m.cro_start_wei::text          AS cro_start_wei,
            m.cro_current_wei::text        AS cro_current_wei,
            m.cro_initial_wei::text        AS cro_initial_wei,
            m.pause_timestamp,
            m.updated_at,
            m.mission_created,
            m.round_pause_secs,
            m.last_round_pause_secs,
            m.creator_address,
            m.all_refunded,
            COALESCE(c.enrolled,0)         AS enrolled_players
        FROM missions m
        JOIN player_missions pm USING (mission_address)
        LEFT JOIN counts c USING (mission_address)
        ORDER BY m.updated_at DESC;";

    await using var cmd = new NpgsqlCommand(sql, conn);
    cmd.Parameters.AddWithValue("p", addr);
    await using var rd  = await cmd.ExecuteReaderAsync();

    var list = new List<object>();
    while (await rd.ReadAsync())
    {
        list.Add(new {
            mission_address        = rd["mission_address"] as string,
            name                   = rd["name"] as string,
            mission_type           = Convert.ToInt16(rd["mission_type"]),
            status                 = Convert.ToInt16(rd["status"]),
            enrollment_start       = Convert.ToInt64(rd["enrollment_start"]),
            enrollment_end         = Convert.ToInt64(rd["enrollment_end"]),
            enrollment_amount_wei  = rd["enrollment_amount_wei"]?.ToString(),
            enrollment_min_players = Convert.ToInt16(rd["enrollment_min_players"]),
            enrollment_max_players = Convert.ToInt16(rd["enrollment_max_players"]),
            mission_start          = Convert.ToInt64(rd["mission_start"]),
            mission_end            = Convert.ToInt64(rd["mission_end"]),
            mission_rounds_total   = Convert.ToInt16(rd["mission_rounds_total"]),
            round_count            = Convert.ToInt16(rd["round_count"]),
            cro_start_wei          = rd["cro_start_wei"]?.ToString(),
            cro_current_wei        = rd["cro_current_wei"]?.ToString(),
            cro_initial_wei        = rd["cro_initial_wei"]?.ToString(),
            pause_timestamp        = rd["pause_timestamp"] is DBNull ? (long?)null : Convert.ToInt64(rd["pause_timestamp"]),
            updated_at             = ToUnixSeconds(((DateTime) rd["updated_at"]).ToUniversalTime()),
            mission_created        = Convert.ToInt64(rd["mission_created"]),
            round_pause_secs       = rd["round_pause_secs"] is DBNull ? (int?)null : Convert.ToInt32(rd["round_pause_secs"]),
            last_round_pause_secs  = rd["last_round_pause_secs"] is DBNull ? (int?)null : Convert.ToInt32(rd["last_round_pause_secs"]),
            creator_address        = rd["creator_address"] as string,
            all_refunded           = rd["all_refunded"] is DBNull ? false : (bool) rd["all_refunded"],
            enrolled_players       = Convert.ToInt32(rd["enrolled_players"])
        });
    }

    return Results.Ok(list);
});

// GET /missions/mission/{addr}  → detailed mission view (DB, no RPC)
app.MapGet("/missions/mission/{addr}",  async (string addr, IConfiguration cfg)         => { // detailed mission view
    if (string.IsNullOrWhiteSpace(addr)) return Results.BadRequest("Missing address");
    addr = addr.ToLowerInvariant();

    var cs = cfg.GetConnectionString("Db");
    await using var conn = new NpgsqlConnection(cs);
    await conn.OpenAsync();

    // 1) mission core row
    var coreSql = @"
      with counts as (
        select mission_address, count(*)::int as enrolled
        from players
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
      where lower(m.mission_address) = lower(trim(@a));";

    await using var core = new NpgsqlCommand(coreSql, conn);
    core.Parameters.AddWithValue("a", addr);
    await using var rd = await core.ExecuteReaderAsync();
    if (!await rd.ReadAsync()) return Results.NotFound("Mission not found");

    var mission = new {
        mission_address        = rd["mission_address"] as string,
        name                   = rd["name"] as string,
        mission_type           = Convert.ToInt16(rd["mission_type"]),
        status                 = Convert.ToInt16(rd["status"]), // DB truth (zero flicker)
        enrollment_start       = Convert.ToInt64(rd["enrollment_start"]),
        enrollment_end         = Convert.ToInt64(rd["enrollment_end"]),
        enrollment_amount_wei  = rd["enrollment_amount_wei"]?.ToString(),
        enrollment_min_players = Convert.ToInt16(rd["enrollment_min_players"]),
        enrollment_max_players = Convert.ToInt16(rd["enrollment_max_players"]),
        mission_start          = Convert.ToInt64(rd["mission_start"]),
        mission_end            = Convert.ToInt64(rd["mission_end"]),
        mission_rounds_total   = Convert.ToInt16(rd["mission_rounds_total"]),
        round_count            = Convert.ToInt16(rd["round_count"]),
        cro_start_wei          = rd["cro_start_wei"]?.ToString(),
        cro_current_wei        = rd["cro_current_wei"]?.ToString(),
        cro_initial_wei        = rd["cro_initial_wei"]?.ToString(),            
        pause_timestamp        = rd["pause_timestamp"] is DBNull ? (long?)null : Convert.ToInt64(rd["pause_timestamp"]),
        updated_at             = ToUnixSeconds(((DateTime) rd["updated_at"]).ToUniversalTime()),
        mission_created        = Convert.ToInt64(rd["mission_created"]),
        round_pause_secs       = rd["round_pause_secs"] is DBNull ? (int?)null : Convert.ToInt32(rd["round_pause_secs"]),
        last_round_pause_secs  = rd["last_round_pause_secs"] is DBNull ? (int?)null : Convert.ToInt32(rd["last_round_pause_secs"]),
        creator_address        = rd["creator_address"] as string,
        all_refunded           = rd["all_refunded"] is DBNull ? false : (bool) rd["all_refunded"],
        enrolled_players       = Convert.ToInt32(rd["enrolled_players"])
    };

    await rd.CloseAsync();

    // 2) enrollments
    var enSql = @"
        select
            player,
            ""enrolledTS"",
            ""amountWon""::text as ""amountWon"",  -- large number -> text (consistent with *_wei fields)
            ""wonTS"",
            refunded,
            ""refundFailed"" as ""refundedFailed"",
            ""refundTS""
        from players
        where lower(mission_address) = @a
        order by ""enrolledTS"" asc nulls last, player asc;";

    await using var enCmd = new NpgsqlCommand(enSql, conn);
    enCmd.Parameters.AddWithValue("a", addr);
    await using var enRd = await enCmd.ExecuteReaderAsync();

    var enrollments = new List<object>();
    while (await enRd.ReadAsync())
    {
        enrollments.Add(new {
        player          = enRd["player"] as string,
        enrolled_ts     = enRd["enrolledTS"] is DBNull ? (long?)null : Convert.ToInt64(enRd["enrolledTS"]),
        amount_won_wei  = enRd["amountWon"]?.ToString(),
        won_ts          = enRd["wonTS"] is DBNull ? (long?)null : Convert.ToInt64(enRd["wonTS"]),
        refunded        = enRd["refunded"] is DBNull ? false : (bool) enRd["refunded"],
        refunded_failed = enRd["refundedFailed"] is DBNull ? false : (bool) enRd["refundedFailed"],
        refund_ts       = enRd["refundTS"] is DBNull ? (long?)null : Convert.ToInt64(enRd["refundTS"])
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
            round_number   = Convert.ToInt16(rRd["round_number"]),
            winner_address = rRd["winner_address"] as string,
            payout_wei     = rRd["payout_wei"]?.ToString(),
            block_number   = rRd["block_number"] is DBNull ? (long?)null : Convert.ToInt64(rRd["block_number"]),
            tx_hash        = rRd["tx_hash"] as string,
            created_at     = ToUnixSeconds(((DateTime) rRd["created_at"]).ToUniversalTime())
        });
    }

    return Results.Ok(new { mission, enrollments, rounds });
});

// Simple API to read the transaction log (mission_tx) per mission
app.MapGet("/missions/{missionAddress}/tx", async (string missionAddress, HttpRequest req, IConfiguration cfg) => {
    if (string.IsNullOrWhiteSpace(missionAddress))
        return Results.BadRequest("Missing missionAddress");

    var mission = missionAddress.ToLowerInvariant();

    var playerFilter    = req.Query["player"].ToString();
    var eventTypeFilter = req.Query["eventType"].ToString();

    var cs = cfg.GetConnectionString("Db");
    await using var conn = new Npgsql.NpgsqlConnection(cs);
    await conn.OpenAsync();

    var sql = @"
        select mission_address, player_address, event_type, tx_hash, block_number
        from mission_tx
        where mission_address = @m";

    if (!string.IsNullOrWhiteSpace(playerFilter))
        sql += " and player_address = @p";

    if (!string.IsNullOrWhiteSpace(eventTypeFilter))
        sql += " and event_type = @e";

    sql += " order by coalesce(block_number, 9223372036854775807), tx_hash;";

    await using var cmd = new Npgsql.NpgsqlCommand(sql, conn);
    cmd.Parameters.AddWithValue("m", mission);

    if (!string.IsNullOrWhiteSpace(playerFilter))
        cmd.Parameters.AddWithValue("p", playerFilter.ToLowerInvariant());

    if (!string.IsNullOrWhiteSpace(eventTypeFilter))
        cmd.Parameters.AddWithValue("e", eventTypeFilter);

    var items = new List<object>();
    await using var rd = await cmd.ExecuteReaderAsync();
    while (await rd.ReadAsync())
    {
        var mAddr  = (string)rd["mission_address"];
        var pAddr  = rd["player_address"] == DBNull.Value ? null : (string)rd["player_address"];
        var ev     = (string)rd["event_type"];
        var txHash = (string)rd["tx_hash"];

        var blockVal    = rd["block_number"];
        long? blockNr   = blockVal == DBNull.Value ? (long?)null : (long)blockVal;

        items.Add(new {
            mission    = mAddr,
            player     = pAddr,
            eventType  = ev,
            txHash,
            blockNumber = blockNr
        });
    }

    return Results.Ok(items);
});

/***********************
 *  PLAYERS – READ API
 *  GET /players/{addr}/tx
 *  -> reads mission_tx for a player (with optional filters)
 ***********************/
app.MapGet("/players/{addr}/tx", async (string addr, HttpRequest req, IConfiguration cfg) => {
    if (string.IsNullOrWhiteSpace(addr))
        return Results.BadRequest("Missing address");

    var player = addr.ToLowerInvariant();

    var missionFilter    = req.Query["mission"].ToString();
    var eventTypeFilter  = req.Query["eventType"].ToString();

    var cs = cfg.GetConnectionString("Db");
    await using var conn = new Npgsql.NpgsqlConnection(cs);
    await conn.OpenAsync();

    var sql = @"
        select mission_address, player_address, event_type, tx_hash, block_number
        from mission_tx
        where lower(player_address) = @p";

    if (!string.IsNullOrWhiteSpace(missionFilter))
        sql += " and mission_address = @m";

    if (!string.IsNullOrWhiteSpace(eventTypeFilter))
        sql += " and event_type = @e";

    sql += " order by coalesce(block_number, 9223372036854775807), tx_hash;";

    await using var cmd = new Npgsql.NpgsqlCommand(sql, conn);
    cmd.Parameters.AddWithValue("p", player);

    if (!string.IsNullOrWhiteSpace(missionFilter))
        cmd.Parameters.AddWithValue("m", missionFilter.ToLowerInvariant());

    if (!string.IsNullOrWhiteSpace(eventTypeFilter))
        cmd.Parameters.AddWithValue("e", eventTypeFilter);

    var items = new List<object>();
    await using var rd = await cmd.ExecuteReaderAsync();
    while (await rd.ReadAsync())
    {
        var mAddr  = (string)rd["mission_address"];
        var pAddr  = rd["player_address"] == DBNull.Value ? null : (string)rd["player_address"];
        var ev     = (string)rd["event_type"];
        var txHash = (string)rd["tx_hash"];

        var blockVal  = rd["block_number"];
        long? blockNr = blockVal == DBNull.Value ? (long?)null : (long)blockVal;

        items.Add(new {
            mission     = mAddr,
            player      = pAddr,
            eventType   = ev,
            txHash,
            blockNumber = blockNr
        });
    }

    return Results.Ok(items);
});

/***********************
 *  PLAYERS – READ API
 *  GET /players/{addr}/eligibility
 *  -> delegates to MissionFactory.canEnroll(address) and related views
 ***********************/
app.MapGet("/players/{addr}/eligibility", async (string addr, IConfiguration cfg)       =>{ // check if a player can enroll right now
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

// GET /health       → basic liveness
app.MapGet("/health",                         ()                                        => // basic liveness
    Results.Ok("OK")
);

// GET /health/db    → DB connectivity check
app.MapGet("/health/db",                async (IConfiguration cfg)                      => { // DB connectivity
    var cs = cfg.GetConnectionString("Db");
    await using var conn = new NpgsqlConnection(cs);
    await conn.OpenAsync();
    await using var cmd = new NpgsqlCommand("select 1", conn);
    var val = (int)(await cmd.ExecuteScalarAsync() ?? 0);
    return Results.Ok(val == 1 ? "DB OK" : "DB FAIL");
});

/* ---------- DEBUG ---------- */

// GET /debug/chain  → basic chain info
app.MapGet("/debug/chain",              async (IConfiguration cfg)                      => { // basic chain info
    var rpc = GetRequired(cfg, "Cronos:Rpc");
    var web3 = new Web3(rpc);
    var chainId = (long)(await web3.Eth.ChainId.SendRequestAsync()).Value;
    var latest  = (long)(await web3.Eth.Blocks.GetBlockNumber.SendRequestAsync()).Value;
    return Results.Ok(new { rpc, chainId, latest });
});

// GET /debug/factory → basic factory mission counts
app.MapGet("/debug/factory",            async (IConfiguration cfg)                      => { // basic factory mission counts
    var cs = cfg.GetConnectionString("Db");
    await using var conn = new Npgsql.NpgsqlConnection(cs);
    await conn.OpenAsync();

    int all, notEnded;

    await using (var cmd = new Npgsql.NpgsqlCommand("select count(*) from missions;", conn))
        all = Convert.ToInt32(await cmd.ExecuteScalarAsync());

    // status < 5 → Pending/Enrolling/Arming/Active/Paused (not ended)
    await using (var cmd = new Npgsql.NpgsqlCommand("select count(*) from missions where status < 5;", conn))
        notEnded = Convert.ToInt32(await cmd.ExecuteScalarAsync());

    // optional tiny sample from DB to mimic previous shape (addresses only)
    var sampleAll = new List<string>();
    await using (var cmd = new Npgsql.NpgsqlCommand(@"
        select mission_address
        from missions
        order by mission_created desc
        limit 5;", conn))
    await using (var rd = await cmd.ExecuteReaderAsync())
    {
        while (await rd.ReadAsync())
            sampleAll.Add(rd["mission_address"] as string ?? "");
    }

    return Results.Ok(new {
        notEnded,
        all,
        sampleAll
    });
});

// GET /debug/mission/{addr} → probe a mission contract directly
app.MapGet("/debug/mission/{addr}",     async (string addr, IConfiguration cfg)         => { // probe a mission contract directly
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
app.MapGet("/debug/push/{addr}",        async (string addr, IHubContext<GameHub> hub)   => { // simple ping to a group
    if (string.IsNullOrWhiteSpace(addr)) return Results.BadRequest("addr required");
    var g = addr.ToLowerInvariant();
    await hub.Clients.Group(g).SendAsync("ServerPing", $"Hello group: {g}");
    return Results.Ok(new { pushed = g });
});

// Inspect environment paths and process identity
app.MapGet("/debug/env",                      (IHostEnvironment env)                    => { // inspect environment info
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
app.MapGet("/debug/indexer/errors",     async (HttpRequest req, IConfiguration cfg)     =>{ // get benign indexer errors for a day
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
app.MapHub<GameHub>("/hub/game"); // main game hub

var createdPingThrottle = new System.Collections.Concurrent.ConcurrentDictionary<string, DateTime>(StringComparer.OrdinalIgnoreCase);
var enrollPingThrottle  = new System.Collections.Concurrent.ConcurrentDictionary<string, DateTime>(StringComparer.OrdinalIgnoreCase);
var bankPingThrottle    = new System.Collections.Concurrent.ConcurrentDictionary<string, DateTime>(StringComparer.OrdinalIgnoreCase);
var finalizePingThrottle= new System.Collections.Concurrent.ConcurrentDictionary<string, DateTime>(StringComparer.OrdinalIgnoreCase);

// POST /events/created  → mission created event
app.MapPost("/events/created", async (HttpRequest req, IConfiguration cfg, IHubContext<GameHub> hub                                 ) =>{ // mission created event
    string? mission = null;
    string? txHash  = null; // optional

    try
    {
        using var doc = await JsonDocument.ParseAsync(req.Body);
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

    if (!string.IsNullOrWhiteSpace(txHash))
    {
        try
        {
            long? blockNumber = null;

            try
            {
                var rpc  = GetRequired(cfg, "Cronos:Rpc");
                var web3 = new Nethereum.Web3.Web3(rpc);
                var rc   = await web3.Eth.Transactions
                    .GetTransactionReceipt
                    .SendRequestAsync(txHash);

                if (rc != null && rc.Status != null && rc.Status.Value == 1 && rc.BlockNumber != null)
                    blockNumber = (long)rc.BlockNumber.Value;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[API] /events/created WARN receipt lookup failed mission={mission} tx={txHash}: {ex.Message}");
            }

            await InsertMissionTxAsync(cfg, mission, null, "Created", txHash, blockNumber);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[API] /events/created WARN mission_tx insert failed mission={mission} tx={txHash}: {ex.Message}");
        }
    }

    Console.WriteLine($"[API] /events/created ACCEPT mission={mission} tx={txHash ?? "-"} {DateTime.UtcNow:o}");

    await KickMissionAsync(mission, txHash, "Created", cfg, hub);

    Console.WriteLine($"[API] /events/created DONE KickMissionAsync mission={mission} {DateTime.UtcNow:o}");

    return Results.Ok(new { pushed = true });
});

// POST /events/enrolled  → player enrolled event
app.MapPost("/events/enrolled", async (HttpRequest req, IConfiguration cfg, IHubContext<GameHub> hub                                ) =>{ // player enrolled event
    string? mission = null;
    string? player  = null;
    string? txHash  = null; // optional

    try
    {
        using var doc = await JsonDocument.ParseAsync(req.Body);
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

    // Best-effort logging of tx → mission_tx (never break the push)
    if (!string.IsNullOrWhiteSpace(txHash))
    {
        try
        {
            long? blockNumber = null;

            // Optional: try to fetch blockNumber from the receipt
            try
            {
                var rpc  = GetRequired(cfg, "Cronos:Rpc");
                var web3 = new Nethereum.Web3.Web3(rpc);
                var rc   = await web3.Eth.Transactions
                    .GetTransactionReceipt
                    .SendRequestAsync(txHash);

                if (rc != null && rc.Status != null && rc.Status.Value == 1 && rc.BlockNumber != null)
                    blockNumber = (long)rc.BlockNumber.Value;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[API] /events/enrolled WARN receipt lookup failed mission={mission} tx={txHash}: {ex.Message}");
            }

            await InsertMissionTxAsync(cfg, mission, player, "Enrolled", txHash, blockNumber);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[API] /events/enrolled WARN mission_tx insert failed mission={mission} player={player ?? "-"} tx={txHash}: {ex.Message}");
        }
    }

    Console.WriteLine($"[API] /events/enrolled ACCEPT mission={mission} player={player ?? "-"} tx={txHash ?? "-"} {DateTime.UtcNow:o}");

    await KickMissionAsync(mission, txHash, "Enrolled", cfg, hub);

    Console.WriteLine($"[API] /events/enrolled DONE KickMissionAsync mission={mission} {DateTime.UtcNow:o}");

    return Results.Ok(new { pushed = true });
});

// POST /events/banked  → mission banked event
app.MapPost("/events/banked", async (HttpRequest req, IConfiguration cfg, IHubContext<GameHub> hub                                  ) => { // mission banked event    
    string? mission = null;
    string? txHash  = null;

    try
    {
        using var doc = await JsonDocument.ParseAsync(req.Body);
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

    try
    {
        long? blockNumber = null;

        try
        {
            var rpc  = GetRequired(cfg, "Cronos:Rpc");
            var web3 = new Nethereum.Web3.Web3(rpc);
            var rc   = await web3.Eth.Transactions
                .GetTransactionReceipt
                .SendRequestAsync(txHash);

            if (rc != null && rc.Status != null && rc.Status.Value == 1 && rc.BlockNumber != null)
                blockNumber = (long)rc.BlockNumber.Value;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[API] /events/banked WARN receipt lookup failed mission={mission} tx={txHash}: {ex.Message}");
        }

        await InsertMissionTxAsync(cfg, mission, null, "Banked", txHash, blockNumber);
    }
    catch (Exception ex)
    {
        Console.WriteLine($"[API] /events/banked WARN mission_tx insert failed mission={mission} tx={txHash}: {ex.Message}");
    }

    Console.WriteLine($"[API] /events/banked ACCEPT mission={mission} tx={txHash} {DateTime.UtcNow:o}");

    await KickMissionAsync(mission, txHash, "Banked", cfg, hub);

    Console.WriteLine($"[API] /events/banked DONE KickMissionAsync mission={mission} {DateTime.UtcNow:o}");

    return Results.Ok(new { pushed = true });
});

// POST /events/finalized  → mission finalized event
app.MapPost("/events/finalized", async (HttpRequest req, IConfiguration cfg, IHubContext<GameHub> hub                               ) => { // mission finalized event
    string? mission = null;
    string? txHash  = null; // optional

    try
    {
        using var doc = await JsonDocument.ParseAsync(req.Body);
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

    // Throttle per mission: once per ~2s
    var now = DateTime.UtcNow;
    if (finalizePingThrottle.TryGetValue(mission, out var prev) && (now - prev) < TimeSpan.FromSeconds(2))
        return Results.Ok(new { pushed = false, reason = "throttled" });

    finalizePingThrottle[mission] = now;

    if (!string.IsNullOrWhiteSpace(txHash))
    {
        try
        {
            long? blockNumber = null;

            try
            {
                var rpc  = GetRequired(cfg, "Cronos:Rpc");
                var web3 = new Nethereum.Web3.Web3(rpc);
                var rc   = await web3.Eth.Transactions
                    .GetTransactionReceipt
                    .SendRequestAsync(txHash);

                if (rc != null && rc.Status != null && rc.Status.Value == 1 && rc.BlockNumber != null)
                    blockNumber = (long)rc.BlockNumber.Value;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[API] /events/finalized WARN receipt lookup failed mission={mission} tx={txHash}: {ex.Message}");
            }

            await InsertMissionTxAsync(cfg, mission, null, "Finalized", txHash, blockNumber);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[API] /events/finalized WARN mission_tx insert failed mission={mission} tx={txHash}: {ex.Message}");
        }
    }

    Console.WriteLine($"[API] /events/finalized ACCEPT mission={mission} tx={txHash ?? "-"} {DateTime.UtcNow:o}");

    await KickMissionAsync(mission, txHash, "Finalized", cfg, hub);

    Console.WriteLine($"[API] /events/finalized DONE KickMissionAsync mission={mission} {DateTime.UtcNow:o}");

    return Results.Ok(new { pushed = true });
});

// ===== PUSH ROUTES =====

// POST /push/mission  → push a mission snapshot to clients
app.MapPost("/push/mission",     async (string mission, string reason, string? txHash, string? eventType, string key, IHubContext<GameHub> hub) =>{
    var cfg = app.Services.GetRequiredService<IConfiguration>();
    var expectedKey = cfg["Push:Key"];

    if (expectedKey == null || key != expectedKey)
        return Results.Unauthorized();

    var g = mission.ToLowerInvariant();

    Console.WriteLine($"[API] /push/mission ACCEPT mission={g} reason={reason} event={eventType ?? "-"} tx={txHash ?? "-"} {DateTime.UtcNow:o}");

    await hub.Clients.Group(g).SendAsync("MissionUpdated", g, reason, txHash, eventType);

    return Results.Ok(new { pushed = true });
});

// POST /push/status   → push status change to clients
app.MapPost("/push/status",      async (string mission, int status, string key, IHubContext<GameHub> hub                            ) => {
    var cfg = app.Services.GetRequiredService<IConfiguration>();
    var expectedKey = cfg["Push:Key"];

    if (expectedKey == null || key != expectedKey)
        return Results.Unauthorized();

    var g = mission.ToLowerInvariant();
    await hub.Clients.Group(g).SendAsync("StatusChanged", g, status);

    return Results.Ok(new { pushed = true });
});

// POST /push/round    → push round result to clients
app.MapPost("/push/round",       async (string mission, int round, string winner, string amountWei, string key, IHubContext<GameHub> hub) =>{
    var cfg = app.Services.GetRequiredService<IConfiguration>();
    var expectedKey = cfg["Push:Key"];

    if (expectedKey == null || key != expectedKey)
        return Results.Unauthorized();

    var g = mission.ToLowerInvariant();

    Console.WriteLine($"[API] /push/round ACCEPT mission={g} round={round} winner={winner} amountWei={amountWei} {DateTime.UtcNow:o}");

    await hub.Clients.Group(g).SendAsync("RoundResult", g, round, winner, amountWei);

    return Results.Ok(new { pushed = true });
});

app.Run();

// ===== Eligibility function DTOs (MissionFactory) =====
[Function("canEnroll", "bool")]
public class CanEnrollFunction              : FunctionMessage   {
    [Parameter("address", "player", 1)]
    public string Player { get; set; } = "";
}

[Function("secondsTillWeeklySlot", "uint256")]
public class SecondsTillWeeklySlotFunction  : FunctionMessage   {
    [Parameter("address", "player", 1)]
    public string Player { get; set; } = "";
}

[Function("secondsTillMonthlySlot", "uint256")]
public class SecondsTillMonthlySlotFunction : FunctionMessage   {
    [Parameter("address", "player", 1)]
    public string Player { get; set; } = "";
}

[Function("getPlayerLimits", typeof(PlayerLimitsOutput))]
public class GetPlayerLimitsFunction        : FunctionMessage   {
    [Parameter("address", "player", 1)]
    public string Player { get; set; } = "";
}

[FunctionOutput]
public class PlayerLimitsOutput             : IFunctionOutputDTO{
    // Matches core.js ABI: getPlayerLimits(address) returns(uint8,uint8,uint8,uint8,uint256,uint256)
    // (weeklyCount, monthlyCount, weeklyLimit, monthlyLimit, weeklyResetAt, monthlyResetAt)
    [Parameter("uint8",   "weeklyCount",    1)] public byte     WeeklyCount     { get; set; }
    [Parameter("uint8",   "monthlyCount",   2)] public byte     MonthlyCount    { get; set; }
    [Parameter("uint8",   "weeklyLimit",    3)] public byte     WeeklyLimit     { get; set; }
    [Parameter("uint8",   "monthlyLimit",   4)] public byte     MonthlyLimit    { get; set; }
    [Parameter("uint256", "weeklyResetAt",  5)] public BigInteger WeeklyResetAt { get; set; }
    [Parameter("uint256", "monthlyResetAt", 6)] public BigInteger MonthlyResetAt{ get; set; }
}


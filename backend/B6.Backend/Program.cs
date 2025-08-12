using Azure.Identity;
using Azure.Extensions.AspNetCore.Configuration.Secrets;
using B6.Backend.Hubs;
using B6.Contracts;                        
using Microsoft.Extensions.Logging.EventLog;
using Nethereum.Web3;
using Nethereum.Contracts;
using Nethereum.ABI.FunctionEncoding.Attributes;
using Npgsql;

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

foreach (var k in requiredKeys)
{
    if (string.IsNullOrWhiteSpace(builder.Configuration[k]))
        throw new InvalidOperationException($"Missing configuration key on startup: {k}");
}

builder.Services.AddSignalR();
builder.Logging.ClearProviders();
builder.Logging.SetMinimumLevel(LogLevel.Information);
builder.Logging.AddConsole();

var app = builder.Build();

/* --------------------- Helpers ---------------------*/
static long ToUnixSeconds(DateTime dtUtc)
{
    if (dtUtc.Kind != DateTimeKind.Utc)
        dtUtc = DateTime.SpecifyKind(dtUtc, DateTimeKind.Utc);
    return new DateTimeOffset(dtUtc).ToUnixTimeSeconds();
}

static string GetRequired(IConfiguration cfg, string key)
{
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
app.MapGet("/config",                         (IConfiguration cfg) =>
{
    var rpc     = GetRequired(cfg, "Cronos:Rpc");
    var factory = GetRequired(cfg, "Contracts:Factory");
    return Results.Ok(new { rpc, factory });
});

/***********************
 *  MISSIONS – READ API
 ***********************/
app.MapGet("/missions/not-ended",       async (IConfiguration cfg) =>
{
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

app.MapGet("/missions/joinable",        async (IConfiguration cfg) =>
{
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
            enrolled_players       = (int)   rd["enrolled_players"]
        });
    }

    return Results.Ok(list);
});

app.MapGet("/missions/player/{addr}",   async (string addr, IConfiguration cfg) =>
{
    if (string.IsNullOrWhiteSpace(addr)) return Results.BadRequest("Missing address");
    addr = addr.ToLowerInvariant();

    var cs = cfg.GetConnectionString("Db");
    await using var conn = new NpgsqlConnection(cs);
    await conn.OpenAsync();

    var sql = @"
      select
        m.mission_address,
        m.name,
        m.mission_type,
        m.status,
        m.mission_start,
        m.mission_end,
        m.mission_rounds_total,
        m.round_count,
        e.enrolled_at,
        e.refunded,
        e.refund_tx_hash
      from mission_enrollments e
      join missions m using (mission_address)
      where e.player_address = @p
      order by m.status asc, m.mission_end asc nulls last;";

    await using var cmd = new NpgsqlCommand(sql, conn);
    cmd.Parameters.AddWithValue("p", addr);
    await using var rd  = await cmd.ExecuteReaderAsync();

    var list = new List<object>();
    while (await rd.ReadAsync())
    {
        list.Add(new {
            mission_address     = rd["mission_address"] as string,
            name                = rd["name"] as string,
            mission_type        = (short) rd["mission_type"],
            status              = (short) rd["status"],
            mission_start       = (long)  rd["mission_start"],
            mission_end         = (long)  rd["mission_end"],
            mission_rounds_total= (short) rd["mission_rounds_total"],
            round_count         = (short) rd["round_count"],
            enrolled_at         = rd["enrolled_at"] is DBNull
                ? (long?)null
                : ToUnixSeconds(((DateTime) rd["enrolled_at"]).ToUniversalTime()),
            refunded            = (bool)  rd["refunded"],
            refund_tx_hash      = rd["refund_tx_hash"] as string
        });
    }

    return Results.Ok(list);
});

app.MapGet("/missions/mission/{addr}",  async (string addr, IConfiguration cfg) =>
{
    if (string.IsNullOrWhiteSpace(addr)) return Results.BadRequest("Missing address");
    addr = addr.ToLowerInvariant();

    var cs = cfg.GetConnectionString("Db");
    await using var conn = new NpgsqlConnection(cs);
    await conn.OpenAsync();

    // 1) mission core row
    var coreSql = @"
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
        mission_start          = (long)  rd["mission_start"],
        mission_end            = (long)  rd["mission_end"],
        mission_rounds_total   = (short) rd["mission_rounds_total"],
        round_count            = (short) rd["round_count"],
        cro_start_wei          = (string)rd["cro_start_wei"],
        cro_current_wei        = (string)rd["cro_current_wei"],
        pause_timestamp        = rd["pause_timestamp"] is DBNull ? null : (long?) rd["pause_timestamp"],
        last_seen_block        = rd["last_seen_block"]  is DBNull ? null : (long?) rd["last_seen_block"],
        updated_at             = ToUnixSeconds(((DateTime) rd["updated_at"]).ToUniversalTime())
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

app.MapGet("/health/db",                async (IConfiguration cfg) =>
{
    var cs = cfg.GetConnectionString("Db");
    await using var conn = new NpgsqlConnection(cs);
    await conn.OpenAsync();
    await using var cmd = new NpgsqlCommand("select 1", conn);
    var val = (int)(await cmd.ExecuteScalarAsync() ?? 0);
    return Results.Ok(val == 1 ? "DB OK" : "DB FAIL");
});

/* ---------- DEBUG: CHAIN INFO ---------- */
app.MapGet("/debug/chain",              async (IConfiguration cfg) =>
{
    var rpc = GetRequired(cfg, "Cronos:Rpc");
    var web3 = new Web3(rpc);
    var chainId = (long)(await web3.Eth.ChainId.SendRequestAsync()).Value;
    var latest  = (long)(await web3.Eth.Blocks.GetBlockNumber.SendRequestAsync()).Value;
    return Results.Ok(new { rpc, chainId, latest });
});

/* ---------- DEBUG: FACTORY COUNTS ---------- */
app.MapGet("/debug/factory",            async (IConfiguration cfg) =>
{
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
app.MapGet("/debug/mission/{addr}",     async (string addr, IConfiguration cfg) =>
{
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

/* ---------- HUB ---------- */
app.MapHub<GameHub>("/hub/game");

// Inspect environment paths and process identity
app.MapGet("/debug/env", (IHostEnvironment env) =>
{
    return Results.Ok(new {
        env.ApplicationName,
        env.EnvironmentName,
        ContentRootPath = env.ContentRootPath,
        BaseDirectory   = AppContext.BaseDirectory,
        CurrentDir      = Environment.CurrentDirectory,
        User            = System.Security.Principal.WindowsIdentity.GetCurrent().Name
    });
});

app.Run();


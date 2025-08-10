using B6.Backend.Hubs;
using B6.Contracts;                        
using Npgsql;
using Nethereum.Web3;
using Nethereum.Contracts;
using Nethereum.ABI.FunctionEncoding.Attributes;
using Microsoft.Extensions.Logging.EventLog;

using Azure.Identity;
using Azure.Extensions.AspNetCore.Configuration.Secrets;

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

static string GetRequired(IConfiguration cfg, string key)
{
    var v = cfg[key];
    if (string.IsNullOrWhiteSpace(v))
        throw new InvalidOperationException($"Missing configuration key: {key}");
    return v;
}

app.MapGet("/", () => Results.Ok("OK"));

// /api/config -> shared runtime config for frontend
app.MapGet("/config", (IConfiguration cfg) =>
{
    var rpc     = GetRequired(cfg, "Cronos:Rpc");
    var factory = GetRequired(cfg, "Contracts:Factory");
    return Results.Ok(new { rpc, factory });
});

/* ---------- HEALTH ---------- */
app.MapGet("/health", () => Results.Ok("OK"));

app.MapGet("/health/db", async (IConfiguration cfg) =>
{
    var cs = cfg.GetConnectionString("Db");
    await using var conn = new NpgsqlConnection(cs);
    await conn.OpenAsync();
    await using var cmd = new NpgsqlCommand("select 1", conn);
    var val = (int)(await cmd.ExecuteScalarAsync() ?? 0);
    return Results.Ok(val == 1 ? "DB OK" : "DB FAIL");
});

/* ---------- DEBUG: CHAIN INFO ---------- */
app.MapGet("/debug/chain", async (IConfiguration cfg) =>
{
    var rpc = GetRequired(cfg, "Cronos:Rpc");
    var web3 = new Web3(rpc);
    var chainId = (long)(await web3.Eth.ChainId.SendRequestAsync()).Value;
    var latest  = (long)(await web3.Eth.Blocks.GetBlockNumber.SendRequestAsync()).Value;
    return Results.Ok(new { rpc, chainId, latest });
});

/* ---------- DEBUG: FACTORY COUNTS ---------- */
app.MapGet("/debug/factory", async (IConfiguration cfg) =>
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
app.MapGet("/debug/mission/{addr}", async (string addr, IConfiguration cfg) =>
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


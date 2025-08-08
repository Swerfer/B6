using B6.Backend.Hubs; 
using Npgsql;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddSignalR();

var app = builder.Build();

app.MapGet("/health", () => Results.Ok("OK"));

// NEW: DB health check
app.MapGet("/health/db", async (IConfiguration cfg) =>
{
    var cs = cfg.GetConnectionString("Db");
    await using var conn = new NpgsqlConnection(cs);
    await conn.OpenAsync();
    await using var cmd = new NpgsqlCommand("select 1", conn);
    var val = (int)(await cmd.ExecuteScalarAsync() ?? 0);
    return Results.Ok(val == 1 ? "DB OK" : "DB FAIL");
});

app.MapHub<GameHub>("/hub/game");
app.Run();

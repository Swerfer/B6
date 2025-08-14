using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Logging;

namespace B6.Backend.Hubs;

public class GameHub : Hub
{
    private readonly ILogger<GameHub> _log;
    public GameHub(ILogger<GameHub> log) => _log = log;

    public override async Task OnConnectedAsync()
    {
        _log.LogInformation("Hub connected: {ConnId} from {UserAgent}",
            Context.ConnectionId,
            Context.GetHttpContext()?.Request.Headers.UserAgent.ToString());
        await base.OnConnectedAsync();
    }

    public override async Task OnDisconnectedAsync(Exception? exception)
    {
        _log.LogInformation("Hub disconnected: {ConnId} ({Reason})",
            Context.ConnectionId, exception?.Message ?? "normal");
        await base.OnDisconnectedAsync(exception);
    }

    public async Task SubscribeMission(string addr)
    {
        var g = addr?.ToLowerInvariant() ?? "";
        await Groups.AddToGroupAsync(Context.ConnectionId, g);
        _log.LogInformation("Subscribed: {ConnId} -> {Group}", Context.ConnectionId, g);
    }

    public async Task UnsubscribeMission(string addr)
    {
        var g = addr?.ToLowerInvariant() ?? "";
        await Groups.RemoveFromGroupAsync(Context.ConnectionId, g);
        _log.LogInformation("Unsubscribed: {ConnId} -/-> {Group}", Context.ConnectionId, g);
    }
}

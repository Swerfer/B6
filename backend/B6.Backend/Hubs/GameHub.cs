using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading.Tasks;

namespace B6.Backend.Hubs;

public class GameHub : Hub
{
    private readonly ILogger<GameHub> _log;

    // Optional: tiny in-memory tracker for debugging
    private static readonly ConcurrentDictionary<string, ConcurrentDictionary<string, byte>> _groups = new(StringComparer.InvariantCulture);

    public GameHub(ILogger<GameHub> log) { _log = log; }

    public override         Task OnConnectedAsync() {
        _log.LogInformation("Hub connected: {id}", Context.ConnectionId);
        return base.OnConnectedAsync();
    }

    public override async   Task OnDisconnectedAsync(Exception ex) {
        // best-effort remove from all tracked groups
        foreach (var g in _groups.Keys.ToList())
        {
            if (_groups.TryGetValue(g, out var set) && set.TryRemove(Context.ConnectionId, out _))
            {
                // ok
            }
        }
        _log.LogInformation("Hub disconnected: {id} ({msg})", Context.ConnectionId, ex?.Message);
        await base.OnDisconnectedAsync(ex);
    }

    public async            Task SubscribeMission(string addr) {
        var g = (addr ?? "").ToLowerInvariant();
        await Groups.AddToGroupAsync(Context.ConnectionId, g);

        // track (debug)
        var set = _groups.GetOrAdd(g, _ => new ConcurrentDictionary<string, byte>());
        set[Context.ConnectionId] = 1;

        _log.LogInformation("Subscribed {id} to {group}", Context.ConnectionId, g);

        // ACK to caller so you instantly see it in the UI
        await Clients.Caller.SendAsync("ServerPing", $"Subscribed to {g}");
    }

    public async            Task UnsubscribeMission(string addr) {
        var g = (addr ?? "").ToLowerInvariant();
        await Groups.RemoveFromGroupAsync(Context.ConnectionId, g);
        if (_groups.TryGetValue(g, out var set)) set.TryRemove(Context.ConnectionId, out _);
        _log.LogInformation("Unsubscribed {id} from {group}", Context.ConnectionId, g);
        await Clients.Caller.SendAsync("ServerPing", $"Unsubscribed from {g}");
    }

    // Debug helper the API can use to show current groups
    public static object         SnapshotGroups() =>
        _groups.ToDictionary(kvp => kvp.Key, kvp => kvp.Value.Count);
}

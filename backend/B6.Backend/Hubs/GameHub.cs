using Microsoft.AspNetCore.SignalR;

namespace B6.Backend.Hubs;

public class GameHub : Hub
{
    public Task SubscribeMission(string addr) =>
        Groups.AddToGroupAsync(Context.ConnectionId, addr.ToLowerInvariant());

    public Task UnsubscribeMission(string addr) =>
        Groups.RemoveFromGroupAsync(Context.ConnectionId, addr.ToLowerInvariant());
}

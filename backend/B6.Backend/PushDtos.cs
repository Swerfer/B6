namespace B6.Backend;

public sealed class PushMissionDto
{
    public string Mission { get; set; } = string.Empty;
}
public record PushStatusDto     (string Mission, short NewStatus);
public record PushRoundDto      (string Mission, short Round, string Winner, string AmountWei);
public record PushSubscribeDto  (string Address, string Endpoint, string P256dh, string Auth, string? UserAgent, string? Locale, string? Timezone);

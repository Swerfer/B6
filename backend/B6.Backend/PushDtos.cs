namespace B6.Backend;

public sealed class PushMissionDto
{
    public string Mission { get; set; } = string.Empty;

    /// <summary>
    /// Optional textual reason describing why this mission push was sent
    /// (e.g. "Mission ended time based Failed").
    /// </summary>
    public string? Reason { get; set; }

    /// <summary>
    /// Optional transaction hash associated with this update
    /// (e.g. the join/bank/finalize/refund transaction that triggered it).
    /// </summary>
    public string? TxHash { get; set; }
}

public record PushStatusDto     (string Mission, short NewStatus);
public record PushRoundDto      (string Mission, short Round, string Winner, string AmountWei);

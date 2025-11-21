namespace B6.Backend;

public sealed class PushMissionDto
{
    public string  Mission  { get; set; } = string.Empty;
    public string? Reason   { get; set; }
    public string? TxHash   { get; set; }
}

public sealed class PushStatusDto
{
    public string Mission   { get; set; } = string.Empty;
    public short  NewStatus { get; set; }
}

public sealed class PushRoundDto
{
    public string Mission   { get; set; } = string.Empty;
    public short  Round     { get; set; }
    public string Winner    { get; set; } = string.Empty;
    public string AmountWei { get; set; } = string.Empty;
}

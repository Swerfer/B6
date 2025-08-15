namespace B6.Backend;

public record PushStatusDto (string Mission, short NewStatus);
public record PushRoundDto  (string Mission, short Round, string Winner, string AmountWei);
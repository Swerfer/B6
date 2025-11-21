using System.Numerics;
using System.Collections.Generic;
using Nethereum.ABI.FunctionEncoding.Attributes;
using Nethereum.Contracts;

namespace B6.Contracts
{
    [Function("getMissionData", typeof(MissionDataWrapper))]
    public class GetMissionDataFunction         : FunctionMessage { }

    [Function("refundPlayers")]
    public class RefundPlayersFunction          : FunctionMessage { }

    [FunctionOutput]
    public class MissionDataWrapper             : IFunctionOutputDTO {
        // getMissionData returns a single tuple
        [Parameter("tuple", "", 1)]
        public MissionDataTuple Data { get; set; } = new();
    }

    [FunctionOutput]
    public class PlayerWin                      : IFunctionOutputDTO {
        [Parameter("address", "player",    1)] public string     Player { get; set; } = string.Empty;
        [Parameter("uint256", "amountWon", 2)] public BigInteger Amount { get; set; }
    }

    // Player tuple matching the Solidity struct Players { address player; uint256 enrolledTS; uint256 amountWon; uint256 wonTS; bool refunded; bool refundFailed; uint256 refundTS; }
    [FunctionOutput]
    public class PlayerTuple                    : IFunctionOutputDTO {
        [Parameter("address","player",       1)] public string     Player        { get; set; } = string.Empty;
        [Parameter("uint256","enrolledTS",   2)] public BigInteger EnrolledTS    { get; set; }
        [Parameter("uint256","amountWon",    3)] public BigInteger AmountWon     { get; set; }
        [Parameter("uint256","wonTS",        4)] public BigInteger WonTS         { get; set; }
        [Parameter("bool",   "refunded",     5)] public bool       Refunded      { get; set; }
        [Parameter("bool",   "refundFailed", 6)] public bool       RefundFailed  { get; set; }
        [Parameter("uint256","refundTS",     7)] public BigInteger RefundTS      { get; set; }
    }

    [FunctionOutput]
    public class MissionDataTuple               : IFunctionOutputDTO {
        [Parameter("uint8",   "status",                 1)] public byte       Status                 { get; set; }
        [Parameter("uint256", "missionCreated",         2)] public BigInteger MissionCreated         { get; set; }
        [Parameter("string",  "name",                   3)] public string     Name                   { get; set; } = string.Empty;

        [Parameter("uint8",   "missionType",            4)] public byte       MissionType            { get; set; }
        [Parameter("uint8",   "missionRounds",          5)] public byte       MissionRounds          { get; set; }
        [Parameter("uint8",   "roundPauseDuration",     6)] public byte       RoundPauseDuration     { get; set; }
        [Parameter("uint8",   "lastRoundPauseDuration", 7)] public byte       LastRoundPauseDuration { get; set; }

        [Parameter("uint256", "croInitial",             8)] public BigInteger CroInitial             { get; set; }
        [Parameter("uint256", "croStart",               9)] public BigInteger CroStart               { get; set; }
        [Parameter("uint256", "croCurrent",            10)] public BigInteger CroCurrent             { get; set; }
        [Parameter("uint256", "enrollmentAmount",      11)] public BigInteger EnrollmentAmount       { get; set; }

        [Parameter("uint8",   "enrollmentMinPlayers",  12)] public byte       EnrollmentMinPlayers   { get; set; }
        [Parameter("uint8",   "enrollmentMaxPlayers",  13)] public byte       EnrollmentMaxPlayers   { get; set; }

        [Parameter("uint256", "enrollmentStart",        14)] public BigInteger EnrollmentStart       { get; set; }
        [Parameter("uint256", "enrollmentEnd",          15)] public BigInteger EnrollmentEnd         { get; set; }
        [Parameter("uint256", "missionStart",           16)] public BigInteger MissionStart          { get; set; }
        [Parameter("uint256", "missionEnd",             17)] public BigInteger MissionEnd            { get; set; }

        // --- dynamic + trailing scalars in on-chain order ---
        [Parameter("tuple[]","players",                 18)] public List<PlayerTuple> Players        { get; set; } = new();

        // trailing scalars (shift back because arrays are gone)
        [Parameter("uint8",   "enrollmentCount",     19)] public byte                  EnrollmentCount  { get; set; }
        [Parameter("uint8",   "roundCount",          20)] public byte                  RoundCount       { get; set; }
        [Parameter("uint256", "pauseTimestamp",      21)] public BigInteger            PauseTimestamp   { get; set; }
        [Parameter("bool",    "allRefunded",         22)] public bool                  AllRefunded      { get; set; }
        [Parameter("address", "creator",             23)] public string                Creator          { get; set; } = string.Empty;

    }

    // ───────── Events: Factory ─────────
    [Event("MissionStatusUpdated")]
    public class MissionStatusUpdatedEventDTO   : IEventDTO {
        [Parameter("address", "mission",     1, true)]  public string     Mission     { get; set; } = string.Empty;
        [Parameter("uint8",   "fromStatus",  2, true)]  public byte       FromStatus  { get; set; }
        [Parameter("uint8",   "toStatus",    3, true)]  public byte       ToStatus    { get; set; }
        [Parameter("uint256", "timestamp",   4, false)] public BigInteger Timestamp   { get; set; }
    }

    [Event("MissionFinalized")]
    public class MissionFinalizedEventDTO       : IEventDTO {
        [Parameter("address", "mission",     1, true)]  public string     Mission     { get; set; } = string.Empty;
        [Parameter("uint8",   "finalStatus", 2, true)]  public byte       FinalStatus { get; set; }
        [Parameter("uint256", "timestamp",   3, false)] public BigInteger Timestamp   { get; set; }
    }

    // ───────── Events: Mission ─────────
    [Event("MissionStatusChanged")]
    public class MissionStatusChangedEventDTO   : IEventDTO {
        [Parameter("uint8",   "previousStatus", 1, true)]  public byte       PreviousStatus { get; set; }
        [Parameter("uint8",   "newStatus",      2, true)]  public byte       NewStatus      { get; set; }
        [Parameter("uint256", "timestamp",      3, false)] public BigInteger Timestamp      { get; set; }
    }

    [Event("PlayerEnrolled")]
    public class PlayerEnrolledEventDTO         : IEventDTO {
        [Parameter("address", "player",       1, true)]  public string     Player       { get; set; } = string.Empty;
        [Parameter("uint256", "amount",       2, false)] public BigInteger Amount       { get; set; }
        [Parameter("uint256", "totalPlayers", 3, false)] public BigInteger TotalPlayers { get; set; }
    }

    [Event("RoundCalled")]
    public class RoundCalledEventDTO            : IEventDTO {
        // event RoundCalled(address indexed player, uint8 indexed roundNumber, uint256 payout, uint256 croRemaining);
        [Parameter("address", "player",       1, true)]   public string     Player        { get; set; } = string.Empty;
        [Parameter("uint8",   "roundNumber",  2, true)]   public byte       RoundNumber   { get; set; }
        [Parameter("uint256", "payout",       3, false)]  public BigInteger Payout        { get; set; }
        [Parameter("uint256", "croRemaining", 4, false)]  public BigInteger CroRemaining  { get; set; }
    }

    [Event("PlayerRefunded")]
    public class PlayerRefundedEventDTO         : IEventDTO {
        [Parameter("address", "player", 1, true)]         public string     Player { get; set; } = string.Empty;
        [Parameter("uint256", "amount", 2, false)]        public BigInteger Amount { get; set; }
    }

    [Event("MissionRefunded")]
    public class MissionRefundedEventDTO        : IEventDTO {
        // fixed: address[] is NOT indexed
        [Parameter("uint256",  "nrOfPlayers", 1, true)]     public BigInteger   NrOfPlayers { get; set; }
        [Parameter("uint256",  "amount",      2, true)]     public BigInteger   Amount      { get; set; }
        [Parameter("address[]","players",     3, false)]    public List<string> Players     { get; set; } = new();
        [Parameter("uint256",  "timestamp",   4, false)]    public BigInteger   Timestamp   { get; set; }
    }

    [Event("MissionCreated")]
    public class MissionCreatedEventDTO         : IEventDTO {
        [Parameter("address", "mission",               1, true )] public string     Mission                 { get; set; } = string.Empty;
        [Parameter("string",  "name",                  2, false)] public string     Name                    { get; set; } = string.Empty;
        [Parameter("uint8",   "missionType",           3, false)] public byte       MissionType             { get; set; }
        [Parameter("uint256", "enrollmentStart",       4, false)] public BigInteger EnrollmentStart         { get; set; }
        [Parameter("uint256", "enrollmentEnd",         5, false)] public BigInteger EnrollmentEnd           { get; set; }
        [Parameter("uint8",   "minPlayers",            6, false)] public byte       MinPlayers              { get; set; }
        [Parameter("uint8",   "maxPlayers",            7, false)] public byte       MaxPlayers              { get; set; }
        [Parameter("uint8",   "roundPauseDuration",    8, false)] public byte       RoundPauseDuration      { get; set; }
        [Parameter("uint8",   "lastRoundPauseDuration",9, false)] public byte       LastRoundPauseDuration  { get; set; }
        [Parameter("uint256", "enrollmentAmount",     10, false)] public BigInteger EnrollmentAmount        { get; set; }
        [Parameter("uint256", "missionStart",         11, false)] public BigInteger MissionStart            { get; set; }
        [Parameter("uint256", "missionEnd",           12, false)] public BigInteger MissionEnd              { get; set; }
        [Parameter("uint8",   "missionRounds",        13, false)] public byte       MissionRounds           { get; set; }
    }

}

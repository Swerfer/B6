using System.Numerics;
using System.Collections.Generic;
using Nethereum.ABI.FunctionEncoding.Attributes;
using Nethereum.Contracts;

namespace B6.Contracts
{
    // ---- Mission.getMissionData (wrapped single tuple) ----
    [Function("getMissionData", typeof(MissionDataWrapper))]
    public class GetMissionDataFunction : FunctionMessage { }

    [FunctionOutput]
    public class PlayerWin : IFunctionOutputDTO {
        [Parameter("address",  "player",    1)] public string     Player { get; set; } = string.Empty;
        [Parameter("uint256",  "amountWon", 2)] public BigInteger Amount { get; set; }
    }

    // function returns ( tuple(...) ) → wrap it:
    [FunctionOutput]
    public class MissionDataWrapper : IFunctionOutputDTO {
        [Parameter("tuple", "", 1)] public MissionDataTuple Data { get; set; } = new();
    }

    [FunctionOutput]
    public class MissionDataTuple : IFunctionOutputDTO {
        // keep players at the top (as you requested)
        [Parameter("address[]", "players",                1)] public List<string>    Players                 { get; set; } = new();

        // mission_type, schedule, enrollment, and pauses (order aligned with your API projections)
        [Parameter("uint8",     "missionType",            2)] public byte            MissionType             { get; set; }
        [Parameter("uint256",   "enrollmentStart",        3)] public BigInteger      EnrollmentStart         { get; set; }
        [Parameter("uint256",   "enrollmentEnd",          4)] public BigInteger      EnrollmentEnd           { get; set; }
        [Parameter("uint256",   "enrollmentAmount",       5)] public BigInteger      EnrollmentAmount        { get; set; }
        [Parameter("uint8",     "enrollmentMinPlayers",   6)] public byte            EnrollmentMinPlayers    { get; set; }
        [Parameter("uint8",     "enrollmentMaxPlayers",   7)] public byte            EnrollmentMaxPlayers    { get; set; }

        [Parameter("uint256",   "missionStart",           8)] public BigInteger      MissionStart            { get; set; }
        [Parameter("uint256",   "missionEnd",             9)] public BigInteger      MissionEnd              { get; set; }
        [Parameter("uint8",     "missionRounds",         10)] public byte            MissionRounds           { get; set; }
        [Parameter("uint8",     "roundCount",            11)] public byte            RoundCount              { get; set; }

        // CRO values (add croInitial, then croStart/croCurrent)
        [Parameter("uint256",   "croInitial",            12)] public BigInteger      CroInitial              { get; set; }   
        [Parameter("uint256",   "croStart",              13)] public BigInteger      CroStart                { get; set; }
        [Parameter("uint256",   "croCurrent",            14)] public BigInteger      CroCurrent              { get; set; }

        // pauses (seconds) + pause timestamp
        [Parameter("uint32",    "roundPauseDuration",    15)] public uint            RoundPauseDuration      { get; set; }
        [Parameter("uint32",    "lastRoundPauseDuration",16)] public uint            LastRoundPauseDuration  { get; set; }
        [Parameter("uint256",   "pauseTimestamp",        17)] public BigInteger      PauseTimestamp          { get; set; }

        // win/refund lists, name, created
        [Parameter("tuple[]",   "playersWon",            18)] public List<PlayerWin> PlayersWon              { get; set; } = new();
        [Parameter("address[]", "refundedPlayers",       19)] public List<string>    RefundedPlayers         { get; set; } = new();
        [Parameter("string",    "name",                  20)] public string          Name                    { get; set; } = string.Empty;
        [Parameter("uint256",   "missionCreated",        21)] public BigInteger      MissionCreated          { get; set; }

        // creator + allRefunded (added in your contract update)
        [Parameter("address",   "creator",               22)] public string          Creator                 { get; set; } = string.Empty;  // NEW
        [Parameter("bool",      "allRefunded",           23)] public bool            AllRefunded             { get; set; }                  // NEW
    }

    // ───────── Events: Factory ─────────
    [Event("MissionStatusUpdated")]
    public class MissionStatusUpdatedEventDTO : IEventDTO {
        [Parameter("address", "mission",     1, true)]  public string     Mission     { get; set; } = string.Empty;
        [Parameter("uint8",   "fromStatus",  2, true)]  public byte       FromStatus  { get; set; }
        [Parameter("uint8",   "toStatus",    3, true)]  public byte       ToStatus    { get; set; }
        [Parameter("uint256", "timestamp",   4, false)] public BigInteger Timestamp   { get; set; }
    }

    [Event("MissionFinalized")]
    public class MissionFinalizedEventDTO : IEventDTO {
        [Parameter("address", "mission",     1, true)]  public string     Mission     { get; set; } = string.Empty;
        [Parameter("uint8",   "finalStatus", 2, true)]  public byte       FinalStatus { get; set; }
        [Parameter("uint256", "timestamp",   3, false)] public BigInteger Timestamp   { get; set; }
    }

    // ───────── Events: Mission ─────────
    [Event("MissionStatusChanged")]
    public class MissionStatusChangedEventDTO : IEventDTO {
        [Parameter("uint8",   "previousStatus", 1, true)]  public byte       PreviousStatus { get; set; }
        [Parameter("uint8",   "newStatus",      2, true)]  public byte       NewStatus      { get; set; }
        [Parameter("uint256", "timestamp",      3, false)] public BigInteger Timestamp      { get; set; }
    }

    [Event("PlayerEnrolled")]
    public class PlayerEnrolledEventDTO : IEventDTO {
        [Parameter("address", "player",       1, true)]  public string     Player       { get; set; } = string.Empty;
        [Parameter("uint256", "amount",       2, false)] public BigInteger Amount       { get; set; }
        [Parameter("uint256", "totalPlayers", 3, false)] public BigInteger TotalPlayers { get; set; }
    }

    [Event("RoundCalled")]
    public class RoundCalledEventDTO : IEventDTO {
        // event RoundCalled(address indexed player, uint8 indexed roundNumber, uint256 payout, uint256 croRemaining);
        [Parameter("address", "player",       1, true)]   public string     Player        { get; set; } = string.Empty;
        [Parameter("uint8",   "roundNumber",  2, true)]   public byte       RoundNumber   { get; set; }
        [Parameter("uint256", "payout",       3, false)]  public BigInteger Payout        { get; set; }
        [Parameter("uint256", "croRemaining", 4, false)]  public BigInteger CroRemaining  { get; set; }
    }

    [Event("PlayerRefunded")]
    public class PlayerRefundedEventDTO : IEventDTO {
        [Parameter("address", "player", 1, true)]         public string     Player { get; set; } = string.Empty;
        [Parameter("uint256", "amount", 2, false)]        public BigInteger Amount { get; set; }
    }

    [Event("MissionRefunded")]
    public class MissionRefundedEventDTO : IEventDTO {
        // fixed: address[] is NOT indexed
        [Parameter("uint256",  "nrOfPlayers", 1, true)]     public BigInteger   NrOfPlayers { get; set; }
        [Parameter("uint256",  "amount",      2, true)]     public BigInteger   Amount      { get; set; }
        [Parameter("address[]","players",     3, false)]    public List<string> Players     { get; set; } = new();
        [Parameter("uint256",  "timestamp",   4, false)]    public BigInteger   Timestamp   { get; set; }
    }

    [Event("MissionCreated")]
    public class MissionCreatedEventDTO : IEventDTO {
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

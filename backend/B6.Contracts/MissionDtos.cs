using System.Numerics;
using System.Collections.Generic;
using Nethereum.ABI.FunctionEncoding.Attributes;
using Nethereum.Contracts;

namespace B6.Contracts
{
    // ---- Factory views ----
    [Function("getAllMissions", typeof(GetMissionsOutput))]
    public class GetAllMissionsFunction : FunctionMessage { }

    [Function("getMissionsNotEnded", typeof(GetMissionsOutput))]
    public class GetMissionsNotEndedFunction : FunctionMessage { }

    [FunctionOutput]
    public class GetMissionsOutput : IFunctionOutputDTO
    {
        [Parameter("address[]", "missions",     1)] public List<string>     Missions    { get; set; } = new();
        [Parameter("uint256[]", "statuses",     2)] public List<BigInteger> Statuses    { get; set; } = new();
        [Parameter("string[]" , "names",        3)] public List<string>     Names       { get; set; } = new();
    }

    // ---- Mission.getMissionData (wrapped single tuple) ----
    [Function("getMissionData", typeof(MissionDataWrapper))]
    public class GetMissionDataFunction : FunctionMessage { }

    [FunctionOutput]
    public class PlayerWin : IFunctionOutputDTO
    {
        [Parameter("address",  "player", 1)] public string      Player { get; set; } = string.Empty;
        [Parameter("uint256",  "amount", 2)] public BigInteger  Amount { get; set; }
    }

    // function returns ( tuple(...) ) → wrap it:
    [FunctionOutput]
    public class MissionDataWrapper : IFunctionOutputDTO
    {
        [Parameter("tuple", "", 1)] public MissionDataTuple Data { get; set; } = new();
    }

    [FunctionOutput]
    public class MissionDataTuple : IFunctionOutputDTO
    {
        [Parameter("address[]", "players",               1)] public List<string>    Players                 { get; set; } = new();
        [Parameter("uint8",     "missionType",           2)] public byte            MissionType             { get; set; }
        [Parameter("uint256",   "enrollmentStart",       3)] public BigInteger      EnrollmentStart         { get; set; }
        [Parameter("uint256",   "enrollmentEnd",         4)] public BigInteger      EnrollmentEnd           { get; set; }
        [Parameter("uint256",   "enrollmentAmount",      5)] public BigInteger      EnrollmentAmount        { get; set; }
        [Parameter("uint8",     "enrollmentMinPlayers",  6)] public byte            EnrollmentMinPlayers    { get; set; }
        [Parameter("uint8",     "enrollmentMaxPlayers",  7)] public byte            EnrollmentMaxPlayers    { get; set; }
        [Parameter("uint256",   "missionStart",          8)] public BigInteger      MissionStart            { get; set; }
        [Parameter("uint256",   "missionEnd",            9)] public BigInteger      MissionEnd              { get; set; }
        [Parameter("uint8",     "missionRounds",        10)] public byte            MissionRounds           { get; set; }
        [Parameter("uint8",     "roundCount",           11)] public byte            RoundCount              { get; set; }
        [Parameter("uint256",   "ethStart",             12)] public BigInteger      EthStart                { get; set; }
        [Parameter("uint256",   "ethCurrent",           13)] public BigInteger      EthCurrent              { get; set; }
        [Parameter("tuple[]",   "playersWon",           14)] public List<PlayerWin> PlayersWon              { get; set; } = new();
        [Parameter("uint256",   "pauseTimestamp",       15)] public BigInteger      PauseTimestamp          { get; set; }
        [Parameter("address[]", "refundedPlayers",      16)] public List<string>    RefundedPlayers         { get; set; } = new();
    }

    // ───────── Events: Factory ─────────
    [Event("MissionStatusUpdated")]
    public class MissionStatusUpdatedEventDTO : IEventDTO
    {
        [Parameter("address", "mission",     1, true)]  public string     Mission     { get; set; } = string.Empty;
        [Parameter("uint8",   "fromStatus",  2, true)]  public byte       FromStatus  { get; set; }
        [Parameter("uint8",   "toStatus",    3, true)]  public byte       ToStatus    { get; set; }
        [Parameter("uint256", "timestamp",   4, false)] public BigInteger Timestamp   { get; set; }
    }

    [Event("MissionFinalized")]
    public class MissionFinalizedEventDTO : IEventDTO
    {
        [Parameter("address", "mission",     1, true)]  public string     Mission     { get; set; } = string.Empty;
        [Parameter("uint8",   "finalStatus", 2, true)]  public byte       FinalStatus { get; set; }
        [Parameter("uint256", "timestamp",   3, false)] public BigInteger Timestamp   { get; set; }
    }

    // ───────── Events: Mission ─────────
    [Event("MissionStatusChanged")]
    public class MissionStatusChangedEventDTO : IEventDTO
    {
        [Parameter("uint8",   "previousStatus", 1, true)]  public byte       PreviousStatus { get; set; }
        [Parameter("uint8",   "newStatus",      2, true)]  public byte       NewStatus      { get; set; }
        [Parameter("uint256", "timestamp",      3, false)] public BigInteger Timestamp      { get; set; }
    }

    [Event("PlayerEnrolled")]
    public class PlayerEnrolledEventDTO : IEventDTO
    {
        [Parameter("address", "player",       1, true)]  public string     Player       { get; set; } = string.Empty;
        [Parameter("uint256", "amount",       2, false)] public BigInteger Amount       { get; set; }
        [Parameter("uint256", "totalPlayers", 3, false)] public BigInteger TotalPlayers { get; set; }
    }

    [Event("RoundCalled")]
    public class RoundCalledEventDTO : IEventDTO
    {
        // event RoundCalled(address indexed player, uint8 indexed roundNumber, uint256 payout, uint256 croRemaining);
        [Parameter("address", "player",       1, true)]   public string     Player        { get; set; } = string.Empty;
        [Parameter("uint8",   "roundNumber",  2, true)]   public byte       RoundNumber   { get; set; }
        [Parameter("uint256", "payout",       3, false)]  public BigInteger Payout        { get; set; }
        [Parameter("uint256", "croRemaining", 4, false)]  public BigInteger CroRemaining  { get; set; }
    }

    [Event("PlayerRefunded")]
    public class PlayerRefundedEventDTO : IEventDTO
    {
        [Parameter("address", "player", 1, true)]         public string     Player { get; set; } = string.Empty;
        [Parameter("uint256", "amount", 2, false)]        public BigInteger Amount { get; set; }
    }

    [Event("MissionRefunded")]
    public class MissionRefundedEventDTO : IEventDTO
    {
        // fixed: address[] is NOT indexed
        [Parameter("uint256",  "nrOfPlayers", 1, true)]     public BigInteger   NrOfPlayers { get; set; }
        [Parameter("uint256",  "amount",      2, true)]     public BigInteger   Amount      { get; set; }
        [Parameter("address[]","players",     3, false)]    public List<string> Players     { get; set; } = new();
        [Parameter("uint256",  "timestamp",   4, false)]    public BigInteger   Timestamp   { get; set; }
    }

    [Event("MissionCreated")]
    public class MissionCreatedEventDTO : IEventDTO
    {
        [Parameter("address", "mission",         1, true)]  public string     Mission          { get; set; } = string.Empty;
        [Parameter("string",  "name",            2, false)] public string     Name             { get; set; } = string.Empty;
        [Parameter("uint8",   "missionType",     3, false)] public byte       MissionType      { get; set; }
        [Parameter("uint256", "enrollmentStart", 4, false)] public BigInteger EnrollmentStart  { get; set; }
        [Parameter("uint256", "enrollmentEnd",   5, false)] public BigInteger EnrollmentEnd    { get; set; }
        [Parameter("uint8",   "minPlayers",      6, false)] public byte       MinPlayers       { get; set; }
        [Parameter("uint8",   "maxPlayers",      7, false)] public byte       MaxPlayers       { get; set; }
        [Parameter("uint256", "enrollmentAmount",8, false)] public BigInteger EnrollmentAmount { get; set; }
        [Parameter("uint256", "missionStart",    9, false)] public BigInteger MissionStart     { get; set; }
        [Parameter("uint256", "missionEnd",     10, false)] public BigInteger MissionEnd       { get; set; }
        [Parameter("uint8",   "missionRounds",  11, false)] public byte       MissionRounds    { get; set; }
    }

}

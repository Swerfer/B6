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
}

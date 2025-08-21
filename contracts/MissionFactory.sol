// SPDX-License-Identifier: MIT
/**
 * © 2025 Be Brave Be Bold Be Banked™ | B6 Labs™ – Swerfer
 * All rights reserved.
 *
 * @title   Be Brave Be Bold Be Banked™ – Mission & Factory Architecture
 * @author  B6 Labs™ – Swerfer
 * @notice
 *  ▸ **B6** – a decentralized gaming platform that runs on the Cronos blockchain.
 *  ▸ **Mission** – an on-chain, time-boxed competition where players enroll
 *    by paying a fixed CRO fee and race through multiple payout rounds.  
 *  ▸ **MissionFactory** – the manager contract that deploys Mission clones,
 *    enforces enrolment limits, routes fees, and recycles funds for future
 *    games.  Each Mission clone is created with `clone.initialize(...)`
 *    and thereafter calls back into the factory for bookkeeping.
 *
 * ## 📖 Mission Overview
 * A Mission is a competitive game with three consecutive phases:
 *
 * 1. **Enrollment** (`enrollmentStart → enrollmentEnd`)  
 *    • Players pay `enrollmentAmount` once.  
 *    • Anti-addiction limits: max `weeklyLimit` per 7 days & `monthlyLimit`
 *      per 30 days – enforced by the factory.  
 *    • Mission requires `enrollmentMinPlayers` to arm; max is
 *      `enrollmentMaxPlayers`.
 *
 * 2. **Active** (`missionStart → missionEnd`)  
 *    • Consists of `missionRounds` payout rounds.  
 *    • A cooldown: 5 min after normal rounds, 1 min before the final round.  
 *    • A player can win **once per mission**.  
 *    • Each round’s payout = time-progress since last claim × `croStart` / 100.
 *
 * 3. **End / Settle**  
 *    • Ends when all rounds are claimed **or** `missionEnd` passes.  
 *    • Owner/authorized may call `forceFinalizeMission()` if necessary.  
 *    • Remaining CRO is distributed via `_withdrawFunds()`.
 *
 * ## 🏭 MissionFactory Responsibilities
 * • **Deployment** – clones the Mission implementation (EIP-1167).  
 * • **Status Tracking** – every Mission reports its status back via
 *   `setMissionStatus`; the factory stores this for dashboards and queries.  
 * • **Enrollment Limits** – global weekly / monthly caps checked in
 *   `canEnroll()` and recorded with `recordEnrollment()`.  
 * • **Reserved Funds Pool** – collects 75 % of leftover CRO from finished
 *   missions and redistributes part of it to newly-created games.  
 * • **Authorization Layer** – owner can whitelist helpers; `onlyOwnerOrAuthorized`
 *   guards all admin actions (create, withdraw, config).  
 * • **Registry** – `isMission[addr]` and `missionStatus[addr]` let the factory
 *   authenticate mission callbacks and list active / ended missions.
 *
 * ## 💰 Fee Split
 * • 25 % of post-game pot → factory owner.  
 * • 75 %                  → factory reserve (`reservedFunds[missionType]`).
 *
 * ## 🔄 Refund & Failure Logic
 * • If a mission never arms (not enough players) all enrollments are refunded.  
 * • If CRO transfers to players fail, amounts are parked in
 *   `failedRefundAmounts`; .
 *
 * ## ⚠️ Key Constraints
 * • `missionRounds` ≥ `enrollmentMinPlayers`.  
 * • A player can win at most once per mission.
 *
 * ## 🛠 Admin Functions (Mission level)
 * • `checkMissionStartCondition`, `forceFinalizeMission`, `withdrawFunds`, `refundPlayers`.
 *
 * ## ✅ Security
 * • OpenZeppelin **Ownable** + **ReentrancyGuard**.  
 * • CRO transfers via `.call{value: …}` to forward all gas and avoid
 *   griefing.  
 * • Factory verifies that callbacks come from registered missions
 *   (`onlyMission`).
 *
 * @dev Each Mission is an EIP-1167 minimal proxy deployed by MissionFactory.
 */

pragma solidity ^0.8.30;

// #region ───────────── Imports ────────────────────────
import "@openzeppelin/contracts/access/Ownable.sol";
import "@openzeppelin/contracts/proxy/Clones.sol";
import "@openzeppelin/contracts/utils/ReentrancyGuard.sol";
import "@openzeppelin/contracts/utils/Strings.sol";
// #endregion
using Strings for uint256;

// ─────────────────── Global Enums ─────────────────────
/**
 * @dev Enum to represent the type of mission.
 * The mission can be one of several types: Custom, Hourly, QuarterDaily, BiDaily, Daily, Weekly, or Monthly.
 * The default use for each type is defined in the comments but is set in the dApp UI and can vary.
 */
enum MissionType {
    Custom,         // No default use, completely custom
    Hourly,         // Default use:  1 day  enrollment, 1 hour arming,  1 hour  rounds
    QuarterDaily,   // Default use:  1 day  enrollment, 1 hour arming,  6 hours rounds
    BiDaily,        // Default use:  1 day  enrollment, 1 hour arming, 12 hours rounds
    Daily,          // Default use:  1 day  enrollment, 1 hour arming, 24 hours rounds
    Weekly,         // Default use:  1 week enrollment, 1 hour arming,  7 days  rounds
    Monthly         // Default use:  1 week enrollment, 1 hour arming, 30 days  rounds
}

/**
 * @dev Enum to represent the status of a mission.
 * The mission can be in one of several states: Pending, Enrolling, Active, Paused, Ended, or Failed.
 */
enum Status     {
    Pending,        // Mission is created but not yet enrolling
    Enrolling,      // Mission is open for enrollment, waiting for players to join
    Arming,         // Mission is armed and ready to start
    Active,         // Mission is currently active and players can participate
    Paused,         // Mission is paused, no further actions can be taken
    PartlySuccess,  // Mission has ended with some players winning, but not all rounds were claimed
    Success,        // Mission has ended successfully, all rounds were claimed
    Failed          // Mission has failed, no players won or not enough players enrolled
}

/**
 * @dev Enum to represent the enrollment limits for a mission.
 * The limits can be None, Weekly, or Monthly.
 */
enum Limit      { 
    None,       // No limit breached
    Weekly,     // Weekly limit breached
    Monthly     // Monthly limit breached
}

// ────────────── Contract MissionFactory────────────────
contract MissionFactory is Ownable, ReentrancyGuard {
    using Clones    for address;
    
    // #region ────────── Events ───────────────────────
    /** 
     * @dev Events emitted by the MissionFactory contract.
     * These events are used to log important actions and state changes within the contract.
     */
    event MissionCreated(
        address indexed mission,
        string          name,
        MissionType     missionType,
        uint256         enrollmentStart,
        uint256         enrollmentEnd,
        uint8           minPlayers,
        uint8           maxPlayers,
        uint256         enrollmentAmount,
        uint256         missionStart,
        uint256         missionEnd,
        uint8           missionRounds
    );
    event AuthorizedAddressAdded                (address        indexed addr                                                                        );
    event AuthorizedAddressRemoved              (address        indexed addr                                                                        );
    event FundsReceived                         (address        indexed sender,         uint256             amount                                  );
    event MissionFundsRegistered                (uint256                amount,         MissionType indexed missionType,    address indexed sender  );
    event FundsWithdrawn                        (address        indexed to,             uint256             amount                                  );    
    event OwnershipTransferProposed             (address        indexed proposer,       address             newOwner,       uint256 timestamp       );
    event OwnershipTransferConfirmed            (address        indexed confirmer,      address             newOwner,       uint256 timestamp       );
    event EnrollmentLimitUpdated                (uint8                  newWeekly,      uint8               newMonthly                              );
    event EnrollmentRecorded                    (address        indexed user,           uint256             timestamp                               );
    event MissionStatusUpdated                  (address        indexed mission,        uint8       indexed fromStatus,     uint8   indexed toStatus, uint256        timestamp);
    event MissionFinalized                      (address        indexed mission,        uint8       indexed finalStatus,    uint256 timestamp       );
    // #endregion

    // ────────────────── Modifiers ─────────────────────
    /**
     * @dev Modifier that allows only the owner or an authorized address to call.
     */
    modifier onlyOwnerOrAuthorized() {
        require(
            msg.sender == owner() || authorized[msg.sender],    // Check if the caller is the owner or an authorized address
            "Not owner or MissionFactory authorized"
        );
        _;
    }
	
	/**
     * @dev Modifier that allows only a valid mission contract to call.
     * This ensures that the caller is a contract that has been registered as a mission.
     */
	modifier onlyMission() {
        require(
            isMission[msg.sender],                                      // Check if the caller is a registered mission
            "MissionFactory: caller is not a valid mission contract"
        );
        _;
    }

    // #region ─────── State Variables ───────────────────
    /**
     * @dev State variables for the MissionFactory contract.
     * These variables store the state of the contract, including authorized addresses, reserved funds, mission statuses, and the implementation address for missions.
     */
    address[]                               public  missions;                                   // Array to hold all mission addresses
    uint8                                   public  weeklyLimit = 4;                            // Maximum number of missions a player can enroll in per week
    uint8                                   public  monthlyLimit = 10;                          // Maximum number of missions a player can enroll in per month
    uint256                                 public  totalMissionFunds;                          // Total funds registered by missions
    uint256                                 public  totalOwnerEarnedFunds;                      // Total funds earned by the owner from missions
    uint256                                 public  totalMissionSuccesses;                      // Total number of successful missions
    uint256                                 public  totalMissionFailures;                       // Total number of failed missions
    address                                 public immutable missionImplementation;             // Address of the Mission implementation contract for creating new missions
    uint256                                 public constant OWNERSHIP_PROPOSAL_WINDOW = 1 days; // Duration for ownership proposal validity
    uint256                                 public proposalTimestamp;                           // The timestamp the proposal was made
    address                                 public proposedNewOwner;                            // The proposed new owner's address     
    address                                 public proposalProposer;                            // The proposer's address
    mapping(address => bool)                public  authorized;                                 // Mapping to track authorized addresses
    mapping(address => bool)                public  isMission;                                  // ↪ quick “is this address a mission?” lookup
    mapping(address => Status)              public  missionStatus;                              // Mapping to hold the status of each mission
    mapping(MissionType => uint256)         public  reservedFunds;                              // Track funds by type
    mapping(address => uint256[])           private _enrollmentHistory;                         // Store timestamps
    mapping(address => string)              public missionNames;                                // Store mission names
    mapping(MissionType => uint256)         public missionTypeCounts;                           // Store per mission type the mission type count
    // #endregion

    // ──────────────── Constructor ─────────────────────
    /**
     * @dev Struct to hold information about players who won the mission.
     * Contains the player's address and the amount they won.
     */
    constructor(address _impl) Ownable(msg.sender) {
        require(_impl != address(0), "impl zero");
        missionImplementation = _impl;
    }

    // ────────────────── Helper functions ──────────────
    /**
     * @dev Function to convert mission types to human readable names 
     */  
    function _toHumanReadableName(MissionType t) internal pure returns (string memory) {
        if (t == MissionType.Hourly)         return "Hourly";
        if (t == MissionType.QuarterDaily)   return "QuarterDaily";
        if (t == MissionType.BiDaily)        return "BiDaily";
        if (t == MissionType.Daily)          return "Daily";
        if (t == MissionType.Weekly)         return "Weekly";
        if (t == MissionType.Monthly)        return "Monthly";
        return "Custom";                      
    }

    /**
     * @dev Returns the time until the next weekly slot for a user.
     * This function calculates the time remaining until the next weekly slot based on the user's enrollment history.
     * @param user The address of the user to check.
     * @return The number of seconds until the next weekly slot.
     */
    function secondsTillWeeklySlot(address user)                            external view returns (uint256) {
        uint256 nowTs = block.timestamp;                                // Get the current timestamp
        uint256[] storage h = _enrollmentHistory[user];                 // Get the user's enrollment history
        uint256 earliest;                                               // Variable to store the earliest enrollment time within the next week
        for (uint i = 0; i < h.length; i++) {                           // Loop through the enrollment history  
            if (h[i] + 7 days > nowTs) {
                if (earliest == 0 || h[i] < earliest) earliest = h[i];  // If this is the first valid enrollment or earlier than the current earliest, update earliest
            }
        }
        return earliest == 0 ? 0 : earliest + 7 days - nowTs;    // If no valid enrollment found, return 0; otherwise, return the time until the next weekly slot
    }

    /**
     * @dev Returns the time until the next monthly slot for a user.
     * This function calculates the time remaining until the next monthly slot based on the user's enrollment history.
     * @param user The address of the user to check.
     * @return The number of seconds until the next monthly slot.
     */
    function secondsTillMonthlySlot(address user)                           external view returns (uint256) {
        uint256 nowTs = block.timestamp;                                // Get the current timestamp
        uint256[] storage h = _enrollmentHistory[user];                 // Get the user's enrollment history
        uint256 earliest;                                               // Variable to store the earliest enrollment time within the next month 
        for (uint i = 0; i < h.length; i++) {                           // Loop through the enrollment history  
            if (h[i] + 30 days > nowTs) {
                if (earliest == 0 || h[i] < earliest) earliest = h[i];  // If this is the first valid enrollment or earlier than the current earliest, update earliest
            }
        }
        return earliest == 0 ? 0 : earliest + 30 days - nowTs;   // If no valid enrollment found, return 0; otherwise, return the time until the next monthly slot
    }

    // ──────────── Anti-addiction Functions ────────────
    /**
     * @dev Sets the weekly and monthly enrollment limits.
     * This function allows the owner or an authorized address to set the limits for how many missions a user can enroll in per week and per month.
     * @param _weekly The new weekly limit for mission enrollments.
     * @param _monthly The new monthly limit for mission enrollments.
     */
    function setEnrollmentLimits(uint8 _weekly, uint8 _monthly)             external onlyOwnerOrAuthorized {
        weeklyLimit = _weekly;
        monthlyLimit = _monthly;
        emit EnrollmentLimitUpdated(_weekly, _monthly);
    }

    /**
     * @dev Checks if a user can enroll in a mission based on anti-addiction limits.
     * This function checks the user's enrollment history to determine if they have exceeded the weekly or monthly limits.
     * @param user The address of the user to check.
     * @return ok A boolean indicating if the user can enroll.
     * @return breach A Limit enum indicating which limit is breached, if any.
     */
    function canEnroll(address user)                                        public view returns (bool ok, Limit breach) {
        uint256 nowTs = block.timestamp;                                    // Get the current timestamp
        uint256 weeklyCount;                                                // Count of enrollments in the last 7 days  
        uint256 monthlyCount;                                               // Count of enrollments in the last 30 days
        uint256 earliest7d;                                                 // Earliest enrollment timestamp in the last 7 days
        uint256 earliest30d;                                                // Earliest enrollment timestamp in the last 30 days    

        uint256[] storage h = _enrollmentHistory[user];                     // Get the user's enrollment history
        for (uint256 i; i < h.length; ++i) {                                // Loop through the enrollment history
            uint256 t = h[i];
            if (t + 30 days > nowTs) {                                      // If the enrollment is within the last 30 days
                ++monthlyCount;
                if (earliest30d == 0 || t < earliest30d) earliest30d = t;   // Update the earliest enrollment timestamp in the last 30 days
                if (t + 7 days > nowTs) {
                    ++weeklyCount;
                    if (earliest7d == 0 || t < earliest7d) earliest7d = t;  // Update the earliest enrollment timestamp in the last 7 days
                }
            }
        }

        bool wk = weeklyCount  >= weeklyLimit;                              // Check if the weekly limit is breached    
        bool mo = monthlyCount >= monthlyLimit;                             // Check if the monthly limit is breached

        if (!wk && !mo) return (true,  Limit.None);                         // If neither limit is breached, return true with Limit.None

        if (wk && !mo)  return (false, Limit.Weekly);                       // If only the weekly limit is breached, return false with Limit.Weekly
        if (mo && !wk)  return (false, Limit.Monthly);                      // If only the monthly limit is breached, return false with Limit.Monthly   

        // both breached: compare remaining seconds
        uint256 leftW = earliest7d  +  7 days - nowTs;                      // Calculate the time left until the next weekly slot
        uint256 leftM = earliest30d + 30 days - nowTs;                      // Calculate the time left until the next monthly slot
        return (false, leftM > leftW ? Limit.Monthly : Limit.Weekly);       // If both limits are breached, return the one with the shorter time left
    }

    /**
     * @dev Records the enrollment of a user in a mission.
     * This function is called when a user enrolls in a mission.
     * It updates the user's enrollment history and emits an event.
     * @param user The address of the user enrolling in the mission.
     */
    function recordEnrollment(address user)                                 external {
        uint256 nowTs = block.timestamp;                                            // Get the current timestamp
        require(missionStatus[msg.sender] == Status.Enrolling, "Invalid caller");   // Ensure the caller is in the Enrolling status

        uint256 cutoff = nowTs - 30 days;                                           // Calculate the cutoff timestamp for pruning  
        uint256[] storage history = _enrollmentHistory[user];                       // Get the user's enrollment history    
        uint256 i = 0;
        while (i < history.length && history[i] < cutoff) {                         // Loop through the history to find entries older than 30 days
            i++;    
        }
        if (i > 0) {                                                                // If there are old entries, remove them
            for (uint256 j = 0; j < history.length - i; j++) {                      // Shift remaining entries to the left
                history[j] = history[j + i];
            }
            for (uint256 k = 0; k < i; k++) {                                       // Remove the last i entries   
                history.pop();
            }
        }

        history.push(nowTs);                                                        // Add the current timestamp to the enrollment history  
        emit EnrollmentRecorded(user, nowTs);                                       // Emit an event for the enrollment record
    }

    /**
     * @dev Returns the player's enrollment limits and time until next slots.
     * This function calculates the number of enrollments a player has made in the last week and month,
     * and returns the limits and time until the next slots.
     * @param player The address of the player to check.
     * @return weekUsed The number of enrollments used in the last week.
     * @return weekMax The maximum number of enrollments allowed in a week.
     * @return monthUsed The number of enrollments used in the last month.
     * @return monthMax The maximum number of enrollments allowed in a month.
     * @return secToWeek The number of seconds until the next weekly slot.
     * @return secToMonth The number of seconds until the next monthly slot.
     */
    function getPlayerLimits(address player)                                external view returns 
        (uint8 weekUsed, uint8 weekMax, uint8 monthUsed, uint8 monthMax, uint256 secToWeek, uint256 secToMonth) {
        uint256 nowTs = block.timestamp;                                        // Get the current timestamp
        uint256[] storage h = _enrollmentHistory[player];                       // Get the player's enrollment history
        uint256 weeklyCount;                                                    // Count of enrollments in the last 7 days
        uint256 monthlyCount;                                                   // Count of enrollments in the last 30 days
        uint256 earliest7d;                                                     // Earliest enrollment timestamp in the last 7 days
        uint256 earliest30d;                                                    // Earliest enrollment timestamp in the last 30 days
        for (uint256 i; i < h.length; ++i) {                                    // Loop through the enrollment history
            uint256 t = h[i];
            if (t + 30 days > nowTs) {                                          // If the enrollment is within the last 30 days
                ++monthlyCount;
                if (earliest30d == 0 || t < earliest30d) earliest30d = t;       // Update the earliest enrollment timestamp in the last 30 days
                if (t + 7 days > nowTs) {
                    ++weeklyCount;
                    if (earliest7d == 0 || t < earliest7d) earliest7d = t;      // Update the earliest enrollment timestamp in the last 7 days
                }
            }
        }
        weekUsed   = uint8(weeklyCount);                                        // Convert weekly count to uint8
        weekMax    = weeklyLimit;                                               // Get the maximum weekly limit
        monthUsed  = uint8(monthlyCount);                                       // Convert monthly count to uint8
        monthMax   = monthlyLimit;                                              // Get the maximum monthly limit
        secToWeek  = earliest7d  == 0 ? 0 : earliest7d  + 7 days - nowTs;       // Calculate seconds until next weekly slot
        secToMonth = earliest30d == 0 ? 0 : earliest30d + 30 days - nowTs;      // Calculate seconds until next monthly slot
        return (weekUsed, weekMax, monthUsed, monthMax, secToWeek, secToMonth); // Return the limits and time until next slots
    }

    // ──────────────── Admin Functions ─────────────────
    /**
     * @dev Adds an address to the list of authorized addresses.
     * @param account The address to authorize.
     */
    function addAuthorizedAddress(address account)                          external onlyOwnerOrAuthorized {
        require(account != address(0),  "Invalid address");                         // Ensure the account is valid
        require(!authorized[account],   "Already authorized");                      // Ensure the account is not already authorized
        authorized[account] = true;                                                 // Add authorization for the account  
        emit AuthorizedAddressAdded(account);                                       // Emit event for addition of authorization
    }

    /**
     * @dev Removes authorization for an address.
     * @param account The address to remove authorization from.
     */
    function removeAuthorizedAddress(address account)                       external onlyOwnerOrAuthorized {
        require(account != address(0),  "Invalid address");                         // Ensure the account is valid
        require(authorized[account],    "Not authorized");                          // Ensure the account is currently authorized
        authorized[account] = false;                                                // Remove authorization for the account
        emit AuthorizedAddressRemoved(account);                                     // Emit event for removal of authorization
    }

    /**
     * @dev Proposes a transfer of ownership to a new address.
     * @param newOwner The address of the new owner.
     * If the owner is not available anymore or lost access, this function allows an authorized address to propose a new owner.
     */
    function proposeOwnershipTransfer(address newOwner)                     external onlyOwnerOrAuthorized {
        uint256 nowTs = block.timestamp;                                // Get the current timestamp
        require(newOwner != address(0), "Invalid new owner");           // Ensure the new owner is a valid address
        proposedNewOwner = newOwner;
        proposalProposer = msg.sender;
        proposalTimestamp = block.timestamp;
        emit OwnershipTransferProposed(msg.sender, newOwner, nowTs);    // Emit event for ownership transfer proposal
    }

    /**
     * @dev Confirms the ownership transfer to a new address.
     * This function allows a 2nd authorized address to confirm the ownership transfer.
     */
    function confirmOwnershipTransfer()                                     external onlyOwnerOrAuthorized {
        uint256 nowTs = block.timestamp;                                                                // Get the current timestamp
        require(proposalProposer != msg.sender, "Cannot confirm your own proposal");                    // Ensure the confirmer is not the proposer
        require(block.timestamp <= proposalTimestamp + OWNERSHIP_PROPOSAL_WINDOW, "Proposal expired");  // Ensure the proposal is still valid within the proposal window

        // Transfer ownership
        _transferOwnership(proposedNewOwner);                                                           // Transfer ownership to the new owner   

        emit OwnershipTransferConfirmed(msg.sender, proposedNewOwner, nowTs);                           // Emit event for ownership transfer confirmation
        // Cleanup
        delete proposedNewOwner;                                                                        // Delete the new owner
        delete proposalProposer;                                                                        // Delete the proposal proposer
        delete proposalTimestamp;                                                                       // Delete the proposal timestamp

    }

    // ───────────── Core Factory Functions ─────────────
    /**
     * @dev Creates a new mission with the specified parameters.
     * @param _missionType          The type of the mission.
     * @param _enrollmentStart      The start time for enrollment.
     * @param _enrollmentEnd        The end time for enrollment.
     * @param _enrollmentAmount     The amount required for enrollment.
     * @param _enrollmentMinPlayers The minimum number of players required to start the mission.
     * @param _enrollmentMaxPlayers The maxnimum number of players required to start the mission.
     * @param _missionStart         The start time for the mission.
     * @param _missionEnd           The end time for the mission.
     * @param _missionRounds        The number of rounds in the mission.
     */
    function createMission (
        MissionType     _missionType,           // Type of the mission
        uint256         _enrollmentStart,       // Start time for enrollment
        uint256         _enrollmentEnd,         // End time for enrollment
        uint256         _enrollmentAmount,      // Amount required for enrollment
        uint8           _enrollmentMinPlayers,  // Minimum number of players required to start the mission
        uint8           _enrollmentMaxPlayers,  // Maximum number of players required to start the mission
        uint256         _missionStart,          // Start time for the mission
        uint256         _missionEnd,            // End time for the mission
        uint8           _missionRounds,         // Number of rounds in the mission
        string calldata _missionName            // The mission name (optional)
        ) external payable onlyOwnerOrAuthorized nonReentrant returns (address, string memory) {
            require(_missionRounds          >= 5,                       "Mission rounds must be greater than or equal to 5");               // Ensure mission rounds is greater than or equal to 5
            require(_enrollmentMinPlayers   >= _missionRounds,          "Minimum players must be greater than or equal to mission rounds"); // Ensure minimum players is at least equal to mission rounds
            require(_enrollmentMaxPlayers   >= _enrollmentMinPlayers,   "Maximum players must be greater than or equal to minimum players");// Ensure maximum players is at least equal to minimum players
            require(_enrollmentStart        <  _enrollmentEnd,          "Enrollment start must be before end");                             // Ensure enrollment start is before end
            require(_missionStart           >= _enrollmentEnd,          "Mission start must be on or after enrollment end");                // Ensure mission start is on or after enrollment end
            require(_missionEnd             >  _missionStart,           "Mission start must be before end");                                // Ensure mission start is before end
            require(_enrollmentAmount       >  0,                       "Enrollment amount must be greater than zero");                     // Ensure enrollment amount is greater than zero

			address clone = missionImplementation.clone(); 	    // EIP-1167 minimal proxy

            // Increment mission type counter
            missionTypeCounts[_missionType]++;                          // Increase mission counts by 1 and store for the mission type
            string memory _finalName = bytes(_missionName).length > 0   // Check if mission name is not ""
                ? _missionName                                          // Not empty --> the supplied name
                : string(abi.encodePacked(
                    _toHumanReadableName(_missionType),
                    " - ",
                    missionTypeCounts[_missionType].toString()  // "" --> calculated mission name
                ));

            isMission[clone]     = true;                        // mark as a valid mission
            missionStatus[clone] = Status.Pending;              // placeholder so first callback passes onlyMission
            missionNames[clone] = _finalName;                   // Store the supplied name or calculated name if nothing supplied

            Mission(payable(clone)).initialize{value: msg.value} (
				owner(),									    // Set the owner of the mission to the owner of MissionFactory
				address(this),								    // Set the MissionFactory address
                _missionType,                                   // Set the type of the mission
                _enrollmentStart,                               // Set the enrollment start time
                _enrollmentEnd,                                 // Set the enrollment end time
                _enrollmentAmount,                              // Set the enrollment amount
                _enrollmentMinPlayers,                          // Set the minimum players required
                _enrollmentMaxPlayers,                          // Set the maximum players allowed
                _missionStart,                                  // Set the mission start time
                _missionEnd,                                    // Set the mission end time
                _missionRounds,                                 // Set the number of rounds in the mission
                _finalName                                      // The supplied name or calculated name if nothing supplied
            );

        missions.push(clone);                                   // Add the new mission to the list of missions
        emit MissionCreated(
            clone,
            _finalName,
            _missionType,
            _enrollmentStart,
            _enrollmentEnd,
            _enrollmentMinPlayers,
            _enrollmentMaxPlayers,
            _enrollmentAmount,
            _missionStart,
            _missionEnd,
            _missionRounds
        );               // Emit event for mission creation

        // Calculate allocation based on mission type
        uint256 allocation = reservedFunds[_missionType] / 4;   // Missions get 1/4th of the reserved funds

        if (allocation > 0 && address(this).balance >= allocation) {
            reservedFunds[_missionType] -= allocation;
            Mission(payable(clone)).increasePot{value: allocation}();   // Sends CRO and updates mission accounting
        }

        return (clone, _finalName);						                            // Return the address of the newly created mission
    }

    /**
     * @dev Sets the status of a mission.
     * @param newStatus The new status to set for the mission.
     */
    function setMissionStatus(Status newStatus) external onlyMission {
        Status fromStatus = missionStatus[msg.sender];
        missionStatus[msg.sender] = newStatus;

        if (newStatus == Status.Success) {
            totalMissionSuccesses++;
        } else if (newStatus == Status.Failed) {
            totalMissionFailures++;
        }

        emit MissionStatusUpdated(
            msg.sender,
            uint8(fromStatus),
            uint8(newStatus),
            block.timestamp
        );

        // Ended statuses: PartlySuccess (=5), Success (=6), Failed (=7)
        if (
            newStatus == Status.PartlySuccess ||
            newStatus == Status.Success ||
            newStatus == Status.Failed
        ) {
            emit MissionFinalized(msg.sender, uint8(newStatus), block.timestamp);
        }
    }

    // ──────────────── Financial Functions ─────────────
    /**
     * @dev Registers mission funds for a specific mission type.
     * @param missionType The type of the mission.
     */
    function registerMissionFunds(MissionType missionType)                  external payable onlyMission nonReentrant {
        require(msg.value > 0, "Amount must be greater than zero");                                                         // Ensure the amount is greater than zero
        bool isEndedMission = missionStatus[msg.sender] == Status.Success || missionStatus[msg.sender] == Status.Failed;    // Check if the mission has ended successfully or failed
        require(isEndedMission, "Caller not a mission");                                                                    // Ensure the caller is a valid mission that has ended 
        reservedFunds[missionType] += msg.value;                                                                            // Add the amount to the reserved funds for the specified mission type
        totalMissionFunds += msg.value;                                                                                     // Update the total mission funds
        totalOwnerEarnedFunds += msg.value / 3;                                                                             // Update the total funds earned by the owner (25% of the amount)
        emit MissionFundsRegistered(msg.value, missionType, msg.sender);                                                    // Emit an event for the registered mission funds
    }

    /**
     * @dev Returns the breakdown of reserved funds for each mission type.
     * This function returns an array containing the reserved funds for each mission type.
     * @return breakdown An array containing the reserved funds for each mission type.
     */
    function reservedFundsBreakdown()                                       external view returns (uint256[7] memory) {
        uint256[7] memory breakdown;                        // Array to hold the breakdown of reserved funds for each mission type
        for (uint256 i = 0; i < 7; i++) {
            breakdown[i] = reservedFunds[MissionType(i)];   // Fill the array with the reserved funds for each mission type
        }
        return breakdown;                                   // Return the breakdown of reserved funds
    }

    /**
     * @dev Receives funds sent to the contract.
     * This function is called when the contract receives CRO without any data.
     * It allows the contract to accept CRO transfers.
     */
    receive()                                                               external payable {}

    /**
     * @dev Fallback function to receive CRO.
     * This function is called when the contract receives CRO without any data.
     * It allows the contract to accept CRO transfers.
     */
    fallback()                                                              external payable {}

    /**
     * @dev Withdraws funds from the MissionFactory contract.
     * This function allows the owner or an authorized address to withdraw funds from the contract.
     * This function shall only be called if the contract is not in use anymore and all missions have ended.
     * It transfers the specified amount of funds to the owner of the MissionFactory contract.
     * @param amount The amount of funds to withdraw. If 0, withdraws all available funds.
     */
    function withdrawFunds(uint256 amount)                                  external onlyOwner nonReentrant {
        address mgrOwner = owner();                                         // Get the owner of the MissionFactory contract
        require(mgrOwner != address(0), "Invalid manager owner");           // Ensure the manager owner is valid
        if (amount == 0) {
            amount = address(this).balance;                                 // If no amount specified, withdraw all funds
        }
        require(amount <= address(this).balance, "Insufficient balance");   // Ensure the contract has enough balance to withdraw
        (bool ok, ) = payable(mgrOwner).call{ value: amount }("");          // Attempt to transfer the specified amount to the manager owner
        require(ok, "Transfer failed");                                     // Ensure the transfer was successful
        emit FundsWithdrawn(mgrOwner, amount);                              // Emit event for funds withdrawal
    }

    // ──────────────── View Functions ────────────────

    /**
     * @dev Returns the missions a player is participating in and their statuses.
     * This function retrieves all missions the player is enrolled in and their current statuses.
     * @param player The address of the player to check.
     * @return joined An array of addresses of the missions the player is enrolled in.
     * @return statuses An array of statuses corresponding to each mission.
     */
    function getPlayerParticipation(address player)                         public view returns (address[] memory, Status[] memory, string[] memory) {
        uint256 len = missions.length;                                      // Get the total number of missions
        uint256 count;                                                      // Variable to count how many missions the player is in

        // First pass: count how many missions the player is in
        for (uint256 i = len; i > 0; i--) {                                 // Loop through the missions from newest to oldest
            if (Mission(payable(missions[i - 1])).isPlayer(player)) {       // Check if the player is enrolled in the mission
                count++;
            }
        }

        // Allocate return arrays
        address[] memory joined     = new address[](count);                 // Create an array to hold the addresses of the missions the player is in
        Status[]  memory statuses   = new Status[](count);                  // Create an array to hold the statuses of the missions the player is in    
        string[]  memory names      = new string[](count);                  // Create an array to hold the mission names
        uint256 idx;                                                        // Index for the return arrays

        // Second pass: fill both arrays
        for (uint256 i = len; i > 0; i--) {                                 // Loop through the missions from newest to oldest
            address m = missions[i - 1];                                    // Get the address of the current mission
            if (Mission(payable(m)).isPlayer(player)) {                     // Check if the player is enrolled in the mission
                joined[idx]   = m;                                          // Add the mission address to the joined array                          
                statuses[idx] = Mission(payable(m)).getRealtimeStatus();    // Get the realtime status of the mission and add it to the statuses array
                names[idx] = missionNames[m];                               // Add the mission name to the output array
                idx++;                                                      // Increment the index for the return arrays
            }
        }
        return (joined, statuses, names);                                   // Return arrays: addresses of missions not ended, their statuses and names 
    }

    /**
     * @dev Returns a summary of the factory's state.
     * This function returns various details about the factory, including owner address, implementation address, total missions, limits, funds, and mission success/failure counts.
     * @return ownerAddress The address of the owner of the factory.
     * @return factoryAddress The address of the factory contract.
     * @return implementation The address of the mission implementation contract.
     * @return totalMissions The total number of missions created.
     * @return weekly The weekly enrollment limit.
     * @return monthly The monthly enrollment limit.
     * @return missionFunds The total funds registered by missions.
     * @return ownerFunds The total funds earned by the owner from missions.
     * @return successes The total number of successful missions.
     * @return failures The total number of failed missions.
     * @return fundsPerTypeArray An array containing the reserved funds for each mission type (1–6).
     */
    function getFactorySummary()                                            public view
        returns (
            address ownerAddress,
            address factoryAddress,
            address implementation,
            uint256 totalMissions,
            uint256 weekly,
            uint256 monthly,
            uint256 missionFunds,
            uint256 ownerFunds,
            uint256 successes,
            uint256 failures,
            uint256[] memory fundsPerTypeArray
        ) {
        uint256 enumLength = uint256(type(MissionType).max) + 1;
		uint256[] memory breakdown = new uint256[](enumLength);
		for (uint256 i = 0; i < enumLength; i++) {
			breakdown[i] = reservedFunds[MissionType(i)];
		}
        return (
            owner(),                // Return the address of the owner of the factory contract
            address(this),          // Return the address of the factory contract
            missionImplementation,  // Return the address of the mission implementation contract
            missions.length,        // Return the total number of missions
            weeklyLimit,            // Return the weekly limit
            monthlyLimit,           // Return the weekly and monthly limits
            totalMissionFunds,      // Return the total funds registered by missions
            totalOwnerEarnedFunds,  // Return the total funds registered by missions and earned by the owner
            totalMissionSuccesses,  // Return the total number of successful missions
            totalMissionFailures,   // Return the total number of successful and failed missions
            breakdown               // Return the reserved funds for each mission type
        );
    }

    /**
     * @dev Returns the status of a mission.
     * @param missionAddress The address of the mission to check.
     * @return mission data of the mission.
     */
    function getMissionData(address missionAddress)                         external view returns (Mission.MissionData memory) {
        require(missionAddress != address(0), "Invalid mission address");          // Ensure mission address is valid
        return Mission(payable(missionAddress)).getMissionData();                           // Return the mission data from the Mission contract
    }

    /**
     * @dev Returns the total number of missions.
     * This function returns the length of the missions array, which contains all mission addresses.
     * @return The total number of missions.
     */
    function getTotalMissions()                                             external view returns (uint256) {
        return missions.length;             // Return the total number of missions
    }

    /**
     * @dev Returns the addresses and statuses of all missions.
     * This function retrieves all missions and their statuses, filtering out old missions.
     * @return An array of mission addresses and an array of their corresponding statuses.
     */
    function getAllMissions()                                               external view returns (address[] memory, Status[] memory, string[] memory) {
        uint256 nowTs = block.timestamp;                                            // Get the current timestamp
        uint256 len = missions.length;
        if (len == 0) {                                                             // If there are no missions, return empty arrays
            return (new address[](0), new Status[](0), new string[](0));
        }

        uint256 startCutoff = nowTs - 60 days;                                      // skip if missionStart < startCutoff
        uint256 endCutoff   = nowTs - 30 days;                                      // skip if (ended) missionEnd < endCutoff
        uint256 count;

        // FIRST PASS ── count how many to return, scanning newest → oldest
        for (uint256 i = len; i > 0;) {
            unchecked { --i; }                                                      // safe because we check i>0 first
            address m = missions[i];
            Status  s = missionStatus[m];
            Mission.MissionData memory md = Mission(payable(m)).getMissionData();

            bool tooOld =
                md.missionStart < startCutoff &&                                    // started > 60 days ago
                (s == Status.Success || s == Status.Failed)
                    ? md.missionEnd < endCutoff                                     // …and ended/failed > 30 days ago
                    : md.missionStart < startCutoff;                                // or is still running but started > 60 days ago

            if (tooOld) {
                break;                                                              // every earlier mission will be older ⇒ stop
            }
            count++;
        }

        // SECOND PASS ── copy the selected missions into fixed-size arrays
        address[] memory outAddrs  = new address[](count);                          // Create an array to hold the addresses of the missions
        Status[]  memory outStatus = new Status[](count);                           // Create an array to hold the statuses of the missions
        string[]  memory names     = new string[](count);                           // Create an array to hold the mission names
        uint256 j;

        for (uint256 i = len; i > 0 && j < count;) {                                // Loop through the missions from newest to oldest
            unchecked { --i; }
            address m = missions[i];                                                // Get the address of the current mission
            Status  s = missionStatus[m];                                           // Get the status of the current mission    
            Mission.MissionData memory md = Mission(payable(m)).getMissionData();   // Get the mission data for the current mission

            bool tooOld =
                md.missionStart < startCutoff &&
                (s == Status.Success || s == Status.Failed)                         // If the mission has ended or failed, check if it ended more than 30 days ago
                    ? md.missionEnd < endCutoff
                    : md.missionStart < startCutoff;

            if (tooOld) {                                                           // If the mission is too old, skip it
                break;
            }
            outAddrs[j]  = m;                                                       // Add the mission address to the output array  
            outStatus[j] = s;                                                       // Add the mission status to the output array
            names[j] = missionNames[m];                                             // Add the mission name to the output array
            unchecked { ++j; }                                                      // Increment the index for the output arrays
        }

        return (outAddrs, outStatus, names);                                        // Return arrays: addresses of missions not ended, their statuses and names 
    }

    /**
     * @dev Returns the addresses of missions filtered by status.
     * This function filters missions based on their status and returns an array of mission addresses that match the specified status.
     * @param s The status to filter missions by.
     * @return An array of mission addresses and an array of their corresponding statuses.
     */
    function getMissionsByStatus(Status s)                                  external view returns (address[] memory, uint8[] memory, string[] memory) {
        uint256 len = missions.length;                              // Get the total number of missions
        uint256 count;

        // First pass: count missions with the specified status
        for (uint256 i = 0; i < len; i++) {                         // Loop through all missions
            if (missionStatus[missions[i]] == s) {                  // If the mission status matches the specified status
                count++;                                            // Increment the count of matching missions
            }
        }

        // Second pass: populate result arrays
        address[] memory filteredMissions = new address[](count);   // Create an array to hold the addresses of matching missions
        uint8[]   memory statuses         = new uint8[](count);     // Create a parallel array for statuses
        string[]  memory names            = new string[](count);    // Create an array to hold the mission names
        uint256 index;
        for (uint256 i = 0; i < len; i++) {                         // Loop through all missions again
            if (missionStatus[missions[i]] == s) {                  // If the mission status matches the specified status
                filteredMissions[index] = missions[i];              // Add the mission address to the result array
                statuses[index] = uint8(s);                         // Add the known status
                names[index] = missionNames[missions[i]];           // Add the mission name to the output array
                index++;
            }
        }

        return (filteredMissions, statuses, names);                 // Return arrays: addresses of missions not ended, their statuses and names 
    }
    
    /**
     * @dev Returns the addresses of missions that have not ended.
     * This function filters out missions that are in the Ended or Failed status.
     * @return An array of mission addresses and an array of their corresponding statuses.
     */
    
    function getMissionsNotEnded()                                          external view returns (address[] memory, uint8[] memory, string[] memory) {
        uint256 len = missions.length;                          // Get the total number of missions 
        uint256 count;                                          // Variable to count how many missions are not ended    

        // First pass: count how many missions are not ended
        for (uint256 i = 0; i < len; i++) {                     // Loop through all missions    
            Status s = missionStatus[missions[i]];
            if (s != Status.Success && s != Status.Failed) {    // If the mission is not in Success or Failed status
                count++;
            }
        }

        // Second pass: populate arrays
        address[] memory result   = new address[](count);       // Create an array to hold the addresses of missions that are not ended
        uint8[]   memory statuses = new uint8[](count);         // Create a parallel array for statuses
        string[]  memory names    = new string[](count);        // Create an array to hold the mission names
        uint256 index;

        for (uint256 i = 0; i < len; i++) {                     // Loop through all missions again
            Status s = missionStatus[missions[i]];              // Get the status of the current mission
            if (s != Status.Success && s != Status.Failed) {    // If the mission is not in Success or Failed status
                result[index] = missions[i];                    // Add the mission address to the result array
                statuses[index] = uint8(s);                     // Add the status to the statuses array
                names[index] = missionNames[missions[i]];       // Add the mission name to the output array
                index++;
            }
        }

        return (result, statuses, names);                       // Return arrays: addresses of missions not ended, their statuses and names  
    }

    /**
     * @dev Returns the addresses of missions that have ended.
     * This function filters out missions that are in the Ended or Failed status.
     * @return An array of mission addresses and an array of their corresponding statuses.
     */
    function getMissionsEnded()                                             external view returns (address[] memory, uint8[] memory, string[] memory) {
        uint256 len = missions.length;                          // Get the total number of missions
        uint256 count;                                          // Variable to count how many missions have ended

        // First pass: count how many missions are ended
        for (uint256 i = 0; i < len; i++) {                     // Loop through all missions
            Status s = missionStatus[missions[i]];              // Get the status of the current mission
            if (s == Status.Success || s == Status.Failed) {    // If the mission is in Success or Failed status
                count++;
            }
        }

        // Second pass: populate arrays
        address[] memory result   = new address[](count);       // Create an array to hold the addresses of missions that have ended
        uint8[]   memory statuses = new uint8[](count);         // Create a parallel array for statuses
        string[]  memory names    = new string[](count);        // Create an array to hold the mission names
        uint256 index;

        for (uint256 i = 0; i < len; i++) {                     // Loop through all missions again
            Status s = missionStatus[missions[i]];              // Get the status of the current mission
            if (s == Status.Success || s == Status.Failed) {    // If the mission is in Success or Failed status
                result[index] = missions[i];                    // Add the mission address to the result array  
                statuses[index] = uint8(s);                     // Add the status to the statuses array
                names[index] = missionNames[missions[i]];       // Add the mission name to the output array
                index++;
            }
        }

        return (result, statuses, names);                       // Return arrays: addresses of missions not ended, their statuses and names  
    }

    // NEW
    function getMissionsEndedPaged(uint256 offset, uint256 limit)           external view returns (address[] memory addrs, uint8[] memory statuses, string[] memory names) {
        uint256 len = missions.length;
        if (offset >= len) {
            // IMPORTANT: construct arrays with [] length, not bare types
            addrs    = new address[](0);
            statuses = new uint8[](0);
            names    = new string[](0);
            return (addrs, statuses, names);
        }

        uint256 to = offset + limit;
        if (to > len) to = len;

        // First pass: count ended in the window
        uint256 count;
        for (uint256 i = offset; i < to; i++) {
            Status s = missionStatus[missions[i]];
            if (s == Status.PartlySuccess || s == Status.Success || s == Status.Failed) {
                unchecked { count++; }
            }
        }

        // Allocate exact-size arrays
        addrs    = new address[](count);
        statuses = new uint8[](count);
        names    = new string[](count);

        // Second pass: fill results
        uint256 k;
        for (uint256 i = offset; i < to; i++) {
            address m = missions[i];
            Status  s = missionStatus[m];
            if (s == Status.PartlySuccess || s == Status.Success || s == Status.Failed) {
                addrs[k]    = m;
                statuses[k] = uint8(s);
                names[k]    = missionNames[m];
                unchecked { k++; }
            }
        }

        return (addrs, statuses, names);
    }

    /**
     * @dev Returns the addresses of the latest n missions.
     * This function retrieves the last n missions from the list of all missions.
     * @param n The number of latest missions to return.
     * @return An array of mission addresses and an array of their corresponding statuses.
     */
    function getLatestMissions(uint256 n)                                   external view returns (address[] memory, uint8[] memory, string[] memory) {
        uint256 total = missions.length;                    // Get the total number of missions
        if (n > total) n = total;                           // If n is greater than the total number of missions, adjust n to total

        address[] memory result   = new address[](n);       // Create an array to hold the addresses of the latest missions
        uint8[]   memory statuses = new uint8[](n);         // Create a parallel array for statuses
        string[]  memory names    = new string[](n);        // Create an array to hold the mission names

        for (uint256 i = 0; i < n; i++) {                   // Loop through the last n missions
            address m = missions[total - 1 - i];            // Get the address of the mission
            result[i] = m;                                  // Add the mission address to the result array  
            statuses[i] = uint8(missionStatus[m]);          // Add the status of the mission to the statuses array
            names[i] = missionNames[missions[i]];           // Add the mission name to the output array
       }

        return (result, statuses, names);                   // Return arrays: addresses of missions not ended, their statuses and names  
    }

    /**
     * @dev Returns the reserved funds for a specific mission type.
     * @param _type The type of the mission to check.
     * @return The amount of reserved funds for the specified mission type.
     */
    function getFundsByType(MissionType _type)                              external view returns (uint256) {
        return reservedFunds[_type];                                                // Return the reserved funds for the specified mission type
    }

    /**
     * @dev Returns the proposal data
     * @return newOwner the stored newOwner proposal
     * @return proposer the proposer
     * @return timestamp the time of the proposal
     * @return timeLeft the time left
     */   
    function getOwnershipProposal()                                         external view returns (address newOwner, address proposer, uint256 timestamp, uint256 timeLeft) {
        if (proposalTimestamp == 0) {
            return (address(0), address(0), 0, 0);                              // No active proposal
        }

        uint256 expiry = proposalTimestamp + OWNERSHIP_PROPOSAL_WINDOW;
        uint256 nowTs = block.timestamp;

        if (nowTs >= expiry) {
            return (proposedNewOwner, proposalProposer, proposalTimestamp, 0);  // Expired
        }

        return (
            proposedNewOwner,
            proposalProposer,
            proposalTimestamp,
            expiry - nowTs                                                      // Seconds remaining until expiry
        );
    }

}

// ───────────────── Contract Mission ───────────────────
contract Mission        is Ownable, ReentrancyGuard {
    
    // #region ─────────────── Events ─────────────────────
    event MissionStatusChanged  (Status     indexed previousStatus, Status      indexed newStatus,      uint256 timestamp                   );
    event PlayerEnrolled        (address    indexed player,         uint256             amount,         uint256 totalPlayers                );
    event RoundCalled           (address    indexed player,         uint8       indexed roundNumber,    uint256 payout, uint256 croRemaining);
    event PlayerRefunded        (address    indexed player,         uint256             amount                                              );
    event FundsWithdrawn        (uint256            ownerAmount,    uint256             factoryAmount                                       );
    event RefundFailed          (address    indexed player,         uint256             amount                                              ); 
    event MissionRefunded       (uint256    indexed nrOfPlayers,    uint256     indexed amount,         address[] player,  uint256 timestamp); // Event emitted when a player is refunded
    event MissionInitialized    (address    indexed owner,          MissionType indexed missionType,    uint256 timestamp                   );
	event PotIncreased			(uint256			value,			uint256				croCurrent											);
    // #endregion

    // #region ─── Player-facing custom errors ───────────
    error EnrollmentNotStarted(uint256 nowTs, uint256 startTs);     // Enrollment has not started yet.
    error EnrollmentClosed(uint256 nowTs, uint256 endTs);           // Enrollment is closed.
    error MaxPlayers(uint8 maxPlayers);                             // Maximum number of players has been reached.  
    error WrongEntryFee(uint256 expected, uint256 sent);            // The entry fee sent does not match the expected amount.
    error AlreadyJoined();                                          // Player has already joined the mission.
    error WeeklyLimit(uint256 secondsLeft);                         // Weekly  limit for mission enrollments has been reached.
    error MonthlyLimit(uint256 secondsLeft);                        // Monthly limit for mission enrollments has been reached.
    error Cooldown(uint256 secondsLeft);                            // Cooldown period is still active, cannot join a new mission.
    error NotActive(uint256 nowTs, uint256 missionStart);           // Mission is not active yet.
    error MissionEnded();                                           // Mission has already ended.
    error AlreadyWon();                                             // Player has already won in a previous round.
    error NotJoined();                                              // Player has not joined the mission.
    error AllRoundsDone();                                          // All rounds of the mission have been completed.
    error PayoutFailed(address winner, uint256 amount, bytes data); // Payout to a winner failed.
    error ContractsNotAllowed();                                    // Contracts are not allowed to join the mission.
    // #endregion 

    // ──────────────────── Modifiers ───────────────────
    /**
     * @dev Modifier to restrict access to the owner or an authorized address.
     * This is used for functions that can only be called by the owner or an authorized address.
     */
    modifier onlyOwnerOrAuthorized() {
        require(
            msg.sender == owner() || missionFactory.authorized(msg.sender),
            "Not owner or authorized"
        );
        _;
    }

    // ──────────────────── Structs ───────────────────── 
    /**
     * @dev Struct to hold information about players who won the mission.
     * Contains the player's address and the amount they won.
     */
    struct PlayersWon {
        address player;                     // Address of the player who won
        uint256 amountWon;                  // Amount won by the player
    }

    /**
     * @dev Struct to hold all mission data.
     * Contains information about players, mission status, enrollment details, and financials.
     */
    struct MissionData {
        address[]       players;                        // Array to hold addresses of players enrolled in the mission
        MissionType     missionType;                    // Type of the mission
        uint256         enrollmentStart;                // Start and end times for enrollment
        uint256         enrollmentEnd;                  // Start and end times for enrollment
        uint256         enrollmentAmount;               // Amount required for enrollment
        uint8           enrollmentMinPlayers;           // Minimum number of players required to start the mission
        uint8           enrollmentMaxPlayers;           // Maximum number of players allowed in the mission
        uint256         missionStart;                   // Start time for the mission
        uint256         missionEnd;                     // End time for the mission
        uint8           missionRounds;                  // Total number of rounds in the mission
        uint8           roundCount;                     // Current round count  
        uint256         croStart;                       // Initial CRO amount at the start of the mission
        uint256         croCurrent;                     // Current CRO amount in the mission
        PlayersWon[]    playersWon;                     // Array to hold players who won in the mission     
        uint256         pauseTimestamp;                 // Time when the mission was paused
        address[]       refundedPlayers;                // Track players who have been refunded
        string          name;                           // Name of the mission
        uint256         missionCreated;                 // Timestamp of when the mission was created, used for 'Pending' stage in dApp
    }

    // #region ───────── State Variables ─────────────────
    /**
     * @dev Reference to the MissionFactory contract.
     * This contract manages the overall mission lifecycle and player interactions.
     */
    MissionFactory              public  missionFactory;      // Reference to the MissionFactory contract
    mapping(address => bool)    public  enrolled;            // Track if a player is enrolled in the mission
    mapping(address => bool)    public  hasWon;              // Track if a player has won in any round
    mapping(address => bool)    public  refunded;            // Track if a player has been refunded
    mapping(address => uint256) public  failedRefundAmounts; // Track failed refund amounts for players
    uint256                     public  ownerShare;          // Total share of funds for the owner
    uint256                     public  factoryShare;        // Total share of funds for the MissionFactory
    bool                        public  missionStartConditionChecked = false; // Flag to check if the mission start condition has been checked
    MissionData                 private _missionData;        // Struct to hold all mission data  
    bool                        private _initialized;        // Flag to track if the contract has been initialized
    Status                      private _previousStatus;     // Track the previous status of the mission
    // #endregion

    // ────────────────── Constructor ───────────────────
    /**
     * @dev Constructor for the Mission contract.
     * Initializes the contract with the owner set to address(0) to prevent accidental ownership.
     * The actual ownership will be set during the initialization phase.
     */
    constructor() Ownable(msg.sender) {}      

    // ────────────────── Initializer ───────────────────
    /**
     * @dev Initializes the Mission contract.
     * This function sets the initial values for the mission and registers it with the MissionFactory.
     * It can only be called once during contract deployment.
     * @param _owner                The address of the owner of the contract.
     * @param _missionFactory            The address of the MissionFactory contract.
     * @param _missionType          The type of the mission.
     * @param _enrollmentStart      The start time for enrollment.
     * @param _enrollmentEnd        The end time for enrollment.
     * @param _enrollmentAmount     The amount required for enrollment.
     * @param _enrollmentMinPlayers The minimum number of players required to start the mission.
     * @param _enrollmentMaxPlayers The maximum number of players allowed in the mission.
     * @param _missionStart         The start time for the mission.
     * @param _missionEnd           The end time for the mission.
     * @param _missionRounds        The number of rounds in the mission.
     */ 
    function initialize(
        address         _owner,
        address         _missionFactory,
        MissionType     _missionType,
        uint256         _enrollmentStart,
        uint256         _enrollmentEnd,
        uint256         _enrollmentAmount,
        uint8           _enrollmentMinPlayers,
        uint8           _enrollmentMaxPlayers,
        uint256         _missionStart,
        uint256         _missionEnd,
        uint8           _missionRounds,
        string calldata _name
    )                                       external payable nonReentrant {
        require(!_initialized, "Already initialized");                          // Ensure the contract is not already initialized

        _initialized = true;

        _transferOwnership(_owner);
        missionFactory = MissionFactory(payable(_missionFactory));              // Set the MissionFactory contract reference

        // Initialize mission data
        _missionData.missionType             = _missionType;
        _missionData.enrollmentStart         = _enrollmentStart;
        _missionData.enrollmentEnd           = _enrollmentEnd;
        _missionData.enrollmentAmount        = _enrollmentAmount;
        _missionData.enrollmentMinPlayers    = _enrollmentMinPlayers;
        _missionData.enrollmentMaxPlayers    = _enrollmentMaxPlayers;
        _missionData.missionStart            = _missionStart;
        _missionData.missionEnd              = _missionEnd;
        _missionData.missionRounds           = _missionRounds;
        _missionData.roundCount              = 0;
        _missionData.croStart                = msg.value;                       // Set initial CRO amount to the value sent during initialization
        _missionData.croCurrent              = msg.value;                       // Set current CRO amount to the value sent during initialization
        _missionData.pauseTimestamp          = 0;                               // Initialize pause time to 0
        _missionData.players                 = new address[](0);                // Initialize players array
        _missionData.playersWon              = new PlayersWon[](0);             // Initialize playersWon array
        _missionData.name                    = _name;
        _missionData.missionCreated          = block.timestamp;
        emit MissionInitialized(_owner, _missionType, block.timestamp);         // Emit event for mission initialization
    }

    // ──────────── Core Mission Functions ──────────────
    /**
     * @notice Allows a player to enroll by paying the enrollment fee.
     * @dev Player can enroll only during the enrollment window and only once.
     * @dev Reverts if:
     *      - Player is a contract
     *      - Enrollment period not open
     *      - Max players reached
     *      - Insufficient CRO sent
     *      - Player has already enrolled
     *      - Player has reached their weekly/monthly limit
     */
    function enrollPlayer()                 external payable nonReentrant {
        uint256 nowTs = block.timestamp;                                                    // Get the current timestamp
        address player = msg.sender;                                                        // Get the address of the player enrolling  

        if (player.code.length > 0) {
            revert ContractsNotAllowed();                                                   // Ensure that contracts cannot enroll in the mission
        }
        if (nowTs < _missionData.enrollmentStart) {
            revert EnrollmentNotStarted(nowTs, _missionData.enrollmentStart);               // Check if enrollment has started
        }
        if (nowTs > _missionData.enrollmentEnd) {
            revert EnrollmentClosed(nowTs, _missionData.enrollmentEnd);                     // Check if enrollment has ended
        }

        if (_missionData.players.length >= _missionData.enrollmentMaxPlayers) {
            revert MaxPlayers(_missionData.enrollmentMaxPlayers);                           // Check if maximum players limit has been reached
        }
        if (msg.value != _missionData.enrollmentAmount) {
            revert WrongEntryFee(_missionData.enrollmentAmount, msg.value);                 // Check if the sent CRO matches the required enrollment amount
        }
        if (enrolled[player]) revert AlreadyJoined();

        (bool ok, Limit breach) = missionFactory.canEnroll(player);                         // Check if the player can enroll based on anti-addiction limits    
        if (!ok) {                                                                          // If the player cannot enroll, revert with the appropriate limit breach error
            if (breach == Limit.Weekly) {                                                   
                revert WeeklyLimit(
                    missionFactory.secondsTillWeeklySlot(player)                            // Revert with the time left until the next weekly slot
                );
            } else {                     
                revert MonthlyLimit(
                    missionFactory.secondsTillMonthlySlot(player)                           // Revert with the time left until the next monthly slot    
                );
            }
        }

        _missionData.players.push(player);                                                  // Add the player to the players array
        enrolled[player] = true;                                                            // Mark the player as enrolled
        _missionData.croStart += msg.value;                                                 // Increase the initial CRO amount by the enrollment fee
        _missionData.croCurrent += msg.value;                                               // Increase the current CRO amount by the enrollment fee

        _setStatus(Status.Enrolling);                                                       // Set the mission status to Enrolling
        missionFactory.recordEnrollment(player);                                            // Record the player's enrollment in the MissionFactory contract
        emit PlayerEnrolled(player, msg.value, _missionData.players.length);                // Emit event for player enrollment
    }

    /**
     * @dev Checks if the mission's conditions are met to start.
     * Only callable by the owner or an authorized address
     * This function must be called after the enrollment period ends and before the mission starts to
     * refund players if the conditions are not met. If calling the function is obmitted, 
     * calling refundPlayers() is the last chance to refund players.
     * @dev If conditions are not met, sets status to Failed and refunds players.
     */
    function checkMissionStartCondition()   external nonReentrant onlyOwnerOrAuthorized { 
        uint256 nowTs = block.timestamp;                                                    // Get the current timestamp
        require(nowTs > _missionData.enrollmentEnd && nowTs < _missionData.missionStart, 
                 "Mission not in arming window. Call refundPlayers instead");               // Ensure mission is in the correct time window to check start conditions
        require(missionStartConditionChecked == false, "Already checked start condition");  // Ensure the start condition has not been checked yet
        missionStartConditionChecked = true;                                                // Set the flag to indicate that the mission start condition has been checked
        if (_missionData.players.length == 0) {                                             // If no players enrolled, set status to Failed and withdraw funds
            _setStatus(Status.Failed);                                                      // Set the mission status to Failed 
            _withdrawFunds(true);                                                           // Withdraw funds and refund players
            return;
        }
        if (_missionData.players.length < _missionData.enrollmentMinPlayers) {
            _setStatus(Status.Failed);                                                      // If not enough players, refund and set status to Failed
            _refundPlayers();
        }
    }

    /**
     * @notice Called by a player to claim a round reward.
     * @dev A player can only win once. The mission must be Active and not expired.
     * @dev After each round, the mission is Paused for:
     *      - 5 minutes for normal rounds
     *      - 1 minute before the final round
     * @dev Emits {RoundClaimed}.
     * @dev Reverts if:
     *      - Mission is in Paused status
     *      - Mission is not Active
     *      - Player has already won a round
     *      - Player is not enrolled in the mission
     *      - All rounds have been claimed
     *      - Payout transfer fails
     * @dev If it is the last round, sets status to Success and withdraws funds
     */
    function callRound()                    external nonReentrant {
        Status s = _getRealtimeStatus();                                                                                // Get the current real-time status of the mission
        uint256 nowTs = block.timestamp;                                                                                // Get the current timestamp

        if (s == Status.Paused) {
            uint256 cd = (_missionData.roundCount + 1 == _missionData.missionRounds)
                ? 60 : 300;                                                                                             // Cooldown duration: 1 minute before final round, 5 minutes otherwise
            uint256 secsLeft = _missionData.pauseTimestamp + cd - nowTs;                                                // Calculate seconds left in the cooldown period
                                                                    revert Cooldown(secsLeft);                          // Ensure the mission is not in a cooldown period
        }
        if (s < Status.Active)                                      revert NotActive(nowTs, _missionData.missionStart); // Ensure the mission is in Active status
        if (s > Status.Active)                                      revert MissionEnded();                              // Ensure the mission has not ended
        if (hasWon[msg.sender])                                     revert AlreadyWon();                                // Ensure the player has not already won a round
        if (!enrolled[msg.sender])                                  revert NotJoined();                                 // Ensure the player is enrolled in the mission
        if (_missionData.roundCount >= _missionData.missionRounds)  revert AllRoundsDone();                             // Ensure all rounds have not been claimed yet

        uint256 progress = (nowTs - _missionData.missionStart) * 100                                                    // Calculate the progress percentage of the mission
                        / (_missionData.missionEnd - _missionData.missionStart);
        uint256 lastAmt = _missionData.playersWon.length > 0                                                            // Get the last payout amount, or 0 if no payouts have been made
            ? _missionData.playersWon[_missionData.playersWon.length-1].amountWon
            : 0;                                                                                                        
        uint256 lastProg = (lastAmt * 100) / _missionData.croStart;                                                     // Calculate the last progress percentage based on the last payout amount
        uint256 payout   = (progress - lastProg) * _missionData.croStart / 100;                                         // Calculate the payout amount based on the progress and last payout

        _missionData.croCurrent -= payout;                                                                              // Deduct the payout from the current CRO amount
        _missionData.roundCount++;                                                                                      // Increment the round count
        hasWon[msg.sender] = true;                                                                                      // Mark the player as having won a round
        _missionData.playersWon.push(PlayersWon(msg.sender, payout));                                                   // Add the player and their payout to the playersWon array

        (bool ok, bytes memory data) = msg.sender.call{ value: payout }("");                                            // Attempt to transfer the payout to the player
        if (!ok)                                                    revert PayoutFailed(msg.sender, payout, data);      // If the transfer fails, revert with an error

        emit RoundCalled(msg.sender, _missionData.roundCount, payout, _missionData.croCurrent);                         // Emit event for round claim

        if (_missionData.roundCount == _missionData.missionRounds) {                                                    // If this is the last round, set status to Success
            _setStatus(Status.Success);
            _withdrawFunds(false);
        } else {
            _setStatus(Status.Paused);
        }
    }

    // ────────────── Financial Functions ───────────────
	/**
     * @dev Add funds to prize pool.
     */
	function increasePot()                  external payable {
		require(msg.value > 0, "No funds sent");                                            // Ensure some funds are sent
        require(
            msg.sender == address(missionFactory) || missionFactory.authorized(msg.sender) || msg.sender == owner(),
            "Only factory or authorized can fund"
        );                                                                                  // Ensure the sender is the MissionFactory or an authorized address
        require(_getRealtimeStatus() < Status.Active, "Mission passed activation");         // Ensure the mission is not already active
		_missionData.croStart 	    += msg.value;                                           // Increase the initial CRO amount by the value sent
		_missionData.croCurrent 	+= msg.value;                                           // Increase the current CRO amount by the value sent
		emit PotIncreased(msg.value, _missionData.croCurrent);                              // Emit event for pot increase
	}

    /**
     * @dev Refunds players if the mission fails.
     * This function can be called by the owner or an authorized address.
     */
    function refundPlayers()                external nonReentrant onlyOwnerOrAuthorized {
        _refundPlayers();                                                                                           // Call internal refund function
    }

    /**
     * @notice Distributes remaining CRO after mission completion or failure.
     * @dev Sends:
     *      - 25% to factory owner
     *      - 75% to MissionFactory (for future missions)
     * @dev If `force = true`, also withdraws failed refund amounts.
     */
    function withdrawFunds()                external nonReentrant onlyOwnerOrAuthorized {
        _withdrawFunds(true);                                                                                       // Call internal withdraw function
    }

    /**
     * @notice Allows owner or authorized to finalize a mission after time expiry.
     * @dev Ends mission and withdraws remaining pot.
     */   
    function forceFinalizeMission()         external onlyOwnerOrAuthorized nonReentrant {
        require(_getRealtimeStatus() == Status.PartlySuccess);  // Ensure mission is in PartlySuccess status

        _setStatus(Status.Success);                             
        _withdrawFunds(false);                                  // Withdraw funds to MissionFactory contract 
    }

    // ───────────────── View Functions ─────────────────

    /**
     * @dev Returns the current number of players enrolled in the mission.
     * This function retrieves the length of the players array in the mission data.
     */
    function getPlayerCount()               public view returns (uint256) {
        return _missionData.players.length;
    }

    /**
     * @dev Returns true if the address is a player in the mission.
     * This function checks if the address is present in the players array of the mission data.
     */
    function isPlayer(address addr)         public view returns (bool) {
        require(addr != address(0), "Invalid address");                 // Ensure the address is not zero
        return enrolled[addr];                                          // Check if the address is enrolled in the mission                                                                        
    }

    /**
     * @dev Returns the player state for a given address.
     * This function checks if the player is enrolled and if they have won in any round.
     * @param player The address of the player to check.
     * @return joined A boolean indicating if the player is enrolled in the mission.
     * @return won A boolean indicating if the player has won in any round.
     */
    function playerState(address player)    external view returns (bool joined, bool won) {
        return (enrolled[player], hasWon[player]);           // Return the enrollment and win status of the player
    }

    /**
     * @dev Returns the number of seconds until the next round starts.
     * This function checks the current real-time status of the mission and calculates the time until the next round.
     * @return The number of seconds until the next round starts, or 0 if the mission is not paused.
     */
    function secondsUntilNextRound()        external view returns (uint256) {
        Status s = _getRealtimeStatus();                                        // Get the current real-time status of the mission
        if (s != Status.Paused) {
            return 0;                                                           // If the mission is not paused, return 0 seconds until next round
        }
        uint256 nowTs = block.timestamp;                                        // Get the current timestamp
        uint256 cd = (_missionData.roundCount + 1 == _missionData.missionRounds)
            ? 60 : 300;                                                         // Cooldown duration: 1 minute before final round, 5 minutes otherwise
        return _missionData.pauseTimestamp + cd - nowTs;                        // Calculate seconds until next round based on pause timestamp and cooldown duration
    }

    /**
     * @dev Returns the current progress percentage of the mission.
     * This function calculates the progress based on the elapsed time since the mission started.
     * @return The current progress percentage of the mission.
     */
    function currentProgressPct()           external view returns (uint256){
        uint256 nowTs = block.timestamp;                                                                            // Get the current timestamp
        if (nowTs < _missionData.missionStart) {
            return 0;                                                                                               // If the mission has not started, return 0% progress
        }
        if (nowTs >= _missionData.missionEnd) {
            return 100;                                                                                             // If the mission has ended, return 100% progress
        }
        return (nowTs - _missionData.missionStart) * 100 / (_missionData.missionEnd - _missionData.missionStart);   // Calculate progress percentage based on elapsed time
    }

    /**
     * @dev Returns the pending payout for a player based on their progress in the mission.
     * This function calculates the pending payout based on the current progress percentage and the last payout amount.
     * @param player The address of the player to check for pending payout.
     * @return The pending payout amount for the player, or 0 if not applicable.
     */
    function pendingPayout(address player)  external view returns (uint256) {
        uint256 nowTs = block.timestamp;                                        // Get the current timestamp
        Status s = _getRealtimeStatus();                                        // Get the current real-time status of the mission
        if (s != Status.Active && s != Status.Paused) {
            return 0;                                                           // If the mission is not Active or Paused, return 0 pending payout
        }
        if (!enrolled[player]) {
            return 0;                                                           // If the player is not enrolled, return 0 pending payout
        }
        if (hasWon[player]) {
            return 0;                                                           // If the player has already won, return 0 pending payout
        }
        if (nowTs < _missionData.missionStart) {
            return 0;                                                           // If the mission has not started, return 0 pending payout
        }
        if (nowTs >= _missionData.missionEnd) {
            return 0;                                                           // If the mission has ended, return 0 pending payout
        }
        uint256 progress = (nowTs - _missionData.missionStart) * 100 / (_missionData.missionEnd - _missionData.missionStart);  // Calculate progress percentage based on elapsed time

        uint256 lastAmt = _missionData.playersWon.length > 0                    // Get the last payout amount, or 0 if no payouts have been made
            ? _missionData.playersWon[_missionData.playersWon.length-1].amountWon
            : 0;
        uint256 lastProg = (lastAmt * 100) / _missionData.croStart;             // Calculate the last progress percentage based on the last payout amount

        return (progress - lastProg) * _missionData.croStart / 100;             // Calculate pending payout based on progress and last payout
    }

    /**
     * @dev Returns the number of remaining rounds in the mission.
     * This function checks the current real-time status of the mission and returns the number of rounds left.
     * @return The number of remaining rounds in the mission, or 0 if the mission is not in Active or Paused status.
     */
    function remainingRounds()              external view returns (uint8) {
        Status s = _getRealtimeStatus();                                        // Get the current real-time status of the mission
        if (s == Status.Active || s == Status.Paused) {
            return _missionData.missionRounds - _missionData.roundCount;        // If the mission is Active or Paused, return remaining rounds
        }
        return 0;                                                               // If the mission is not in Active or Paused status, return 0 remaining rounds
    }

    /**
     * @dev Returns the MissionData structure.
     */
    function getMissionData()               external view returns (MissionData memory) {
        return _missionData;
    }

    /**
     * @dev Returns the current real-time status of the mission.
     * This function checks the current time and mission data to determine the status.
     * @return The current status of the mission.
     */
    function getRealtimeStatus()            external view returns (Status) {
        return _getRealtimeStatus();
    }

    /**
     * @dev Returns whether the mission is in the arming phase.
     * This function checks if the current time is between the enrollment end and mission start times.
     * @return A boolean indicating if the mission is in the arming phase.
     */
    function isArming()                     public view returns (bool) {
        uint256 nowTs = block.timestamp;
        return (nowTs > _missionData.enrollmentEnd && nowTs < _missionData.missionStart);
    }

    /**
     * @dev Returns whether the mission is finalized by realtime status, 
            not the status set in the factory which can lag behind.
     * This function checks if the mission is in Success or Failed status.
     * @return A boolean indicating if the mission is finalized.
     */ 
    function isFinalized()                  public view returns (bool) {
        Status s = _getRealtimeStatus();
        return (s == Status.Success || s == Status.Failed);
    }

    /**
     * @dev Returns the addresses of players who have failed refunds.
     * This function iterates through all players and collects those with failed refund amounts.
     * @return An array of player addresses who have failed refunds.
     */
    function getFailedRefundPlayers()       external view returns (address[] memory) {
        require(_getRealtimeStatus() == Status.Failed, "Mission is not in Failed status");  // Ensure mission is in Failed status
        uint256 count = 0;
        for (uint256 i = 0; i < _missionData.players.length; i++) {                         // Iterate through all players
            if (failedRefundAmounts[_missionData.players[i]] > 0) {                         // Check if the player has a failed refund amount   
                count++;
            }
        }

        address[] memory failedPlayers = new address[](count);                              // Create an array to hold failed refund players    
        uint256 index = 0;
        for (uint256 i = 0; i < _missionData.players.length; i++) {                         // Iterate through all players again
            if (failedRefundAmounts[_missionData.players[i]] > 0) {                         // Check if the player has a failed refund amount
                failedPlayers[index++] = _missionData.players[i];                           // Add the player to the failed refund players array    
            }
        }
        return failedPlayers;                                                               // Return the array of failed refund players
    }

    /**
     * @dev Checks if a player has been refunded.
     * This function iterates through the refundedPlayers array to check if the address is present.
     * @param addr The address of the player to check for refund status.
     * @return A boolean indicating if the player has been refunded.
     */ 
    function wasRefunded(address addr)      public view returns (bool) {
        require(_getRealtimeStatus() == Status.Failed, "Mission is not in Failed status");  // Ensure mission is in Failed status
        require(addr != address(0), "Invalid address");                                     // Ensure the address is not zero
        require(enrolled[addr], "Player not enrolled");                                     // Ensure the address is enrolled
        for (uint256 i = 0; i < _missionData.refundedPlayers.length; i++) {
            if (_missionData.refundedPlayers[i] == addr) return true;
        }
        return false;
    }

    /**
     * @dev Returns the array of players who won in the mission.
     * This function retrieves the playersWon array from the mission data.
     */ 
    function getWinners()                   external view returns (PlayersWon[] memory) {
        require(_getRealtimeStatus() == Status.Success || _getRealtimeStatus() == Status.PartlySuccess, 
                "Mission is not in Success or PartlySuccess status");                   // Ensure mission is in Success or PartlySuccess status
        return _missionData.playersWon;                                                 // Return the array of players who won in the mission
    }

    /// @notice Lightweight roll-up for indexer reconciliation (rarely used)
    function getIndexerSnapshot()           external view returns (uint8 status, uint8 roundCount, uint256 croCurrent, uint32 playersCount, uint32 winnersCount, uint32 refundedCount) {
        Status s = _getRealtimeStatus(); // forward-only except Active<->Paused
        return (
            uint8(s),
            _missionData.roundCount,
            _missionData.croCurrent,
            uint32(_missionData.players.length),
            uint32(_missionData.playersWon.length),
            uint32(_missionData.refundedPlayers.length)
        );
    }

    /// @notice Return a window of refunded players to avoid huge arrays in one call
    function getRefundedPlayersSlice(uint256 offset, uint256 limit) external view returns (address[] memory slice) {
        address[] storage arr = _missionData.refundedPlayers;
        uint256 len = arr.length;
        if (offset >= len) {
            return new address[](0);
        }
        uint256 to = offset + limit;
        if (to > len) to = len;
        uint256 n = to - offset;
        slice = new address[](n);
        for (uint256 i = 0; i < n; i++) {
            slice[i] = arr[offset + i];
        }
    }

    // ──────────────── Internal Helpers ────────────────
    /**
     * @dev Returns the current status of the mission based on the current time and mission data.
     * This function checks various conditions to determine the real-time status of the mission.
     * @return status The current status of the mission.
     */ 
    function _getRealtimeStatus()           internal view returns (Status status) {

        // 1. Absolute states never change
        if (_previousStatus == Status.Success || _previousStatus == Status.Failed) {
            return _previousStatus;                                         // mission is already in Success or Failed state, return it
        }

        uint256 nowTs = block.timestamp;

        // 2. Before enrollment even opens
        if (nowTs < _missionData.enrollmentStart) {
            return Status.Pending;                                          // mission is not yet open for enrollment
        }

        // 3. Enrollment window open
        if (nowTs <= _missionData.enrollmentEnd) {
            return Status.Enrolling;                                        // mission is open for enrollment                        
        }

        // 4. Enrollment closed – decide if we *could* arm
        if (_missionData.players.length < _missionData.enrollmentMinPlayers) {
            return Status.Failed;                                           // not enough players, mission failed   
        }

        // 5. Waiting for missionStart timestamp
        if (nowTs < _missionData.missionStart) {
            return Status.Arming;                                            // mission is ready to be armed, but not yet started
        }
        
        // 6. Mission active
        if (nowTs < _missionData.missionEnd) {
            if (_missionData.roundCount >= _missionData.missionRounds) {
                return Status.Success;                                      // all rounds completed, mission is successful
            }
            if (_missionData.pauseTimestamp == 0) {
                return Status.Active;                                       // mission is active, no pause in progress
            } else if (nowTs < _missionData.pauseTimestamp +
                ((_missionData.roundCount + 1 == _missionData.missionRounds)
                    ? 1 minutes
                    : 5 minutes))                                           // if pause time is set, check if we are still in the pause window
            {
                return Status.Paused;                                       // in pause window, waiting for next round
            } else {
                return Status.Active;                                       // mission is active, no pause in progress                   
            }           
        }
        else
        {
            if (_missionData.roundCount == 0) {
                return Status.Failed;                                       // nobody ever called a round → full refund path
            }
            if (_missionData.roundCount < _missionData.missionRounds) {
                return Status.PartlySuccess;                                // some rounds claimed; leftovers need finalization
            }
            return Status.Success;                                          // all rounds claimed
        }

    }

    /**
     * @dev Sets the status of the mission.
     * @param newStatus The new status to set for the mission.
     */
    function _setStatus(Status newStatus)   internal {
        uint256 nowTs = block.timestamp;                                // Get the current timestamp
        if (newStatus == Status.Enrolling       ||                      // If the new status is one of these, update the mission status in the MissionFactory
            newStatus == Status.Arming          || 
            newStatus == Status.Success         ||
            newStatus == Status.PartlySuccess   ||
            newStatus == Status.Failed) 
        {  
            missionFactory.setMissionStatus(newStatus);                 // Update the status in MissionFactory
        }
        if (newStatus == Status.Paused) {
            _missionData.pauseTimestamp = nowTs;                        // Record the time when the mission was paused
        }
        else if (newStatus == Status.Active) {
            _missionData.pauseTimestamp = 0;                            // Reset pause time when the mission is active
        }
        emit MissionStatusChanged(_previousStatus, newStatus, nowTs);   // Emit event for status change
        _previousStatus = newStatus;                                    // Update the previous status to the new status
    }

    /**
     * @notice Distributes remaining CRO after mission completion or failure.
     * @dev Sends:
     *      - 25% to factory owner
     *      - 75% to MissionFactory (for future missions)
     * @dev If `force = true`, also withdraws failed refund amounts.
     */
    function _withdrawFunds(bool force)     internal {
        require(_getRealtimeStatus() == Status.Success || _getRealtimeStatus() == Status.Failed);   // Ensure mission is ended
        uint256 balance = address(this).balance;
        require(balance > 0,                                "No funds to withdraw");                // Ensure there are funds to withdraw

        if (_missionData.players.length == 0) {                                                      
            _setStatus(Status.Failed);                                                              // If no players, set status to Failed
        }
        uint256 distributable;
        if (force) {
            distributable = balance;                                                                // If force is true, all funds are distributable
        } else {
            uint256 unclaimable = _getTotalFailedRefunds();                                         // Get total failed refunds for all players  
            if (unclaimable > balance) {                                                            // If unclaimable amount exceeds the balance      
                unclaimable = balance;                                                              // safety check
            }
            distributable = balance - unclaimable;                                                  // Calculate distributable amount by subtracting unclaimable amounts
        }

        require(distributable > 0,                          "No funds to withdraw");                // Ensure there are funds to withdraw after deducting unclaimable amounts

        uint256 _ownerShare = (distributable * 25) / 100;                                           // Calculate the owner's share (25% of distributable funds)     
        uint256 _factoryShare = distributable - _ownerShare;                                        // Calculate the factory's share (75% of distributable funds)     

        (bool ok, ) = payable(missionFactory.owner()).call{value: _ownerShare}("");                 // Attempt to transfer the owner's share to the MissionFactory owner
        require(ok,                                         "Owner transfer failed");               // Ensure the transfer was successful   

        missionFactory.registerMissionFunds{ value: _factoryShare }(                                // Register the factory's share in the MissionFactory contract  
            _missionData.missionType                                                                // Pass the mission type
        );

        emit FundsWithdrawn(_ownerShare, _factoryShare);                                            // Emit event for funds withdrawal
        ownerShare = _ownerShare;                                                                   // Update the owner's share
        factoryShare = _factoryShare;                                                               // Update the factory's share
        _missionData.croCurrent = address(this).balance;
    }

    /**
     * @dev Returns the total amount of failed refunds for all players.
     * This function iterates through all players and sums their failed refund amounts.
     * @return total The total amount of failed refunds for all players.
     */
    function _getTotalFailedRefunds()       internal view returns (uint256 total) {
        for (uint256 i = 0; i < _missionData.players.length; i++) {                             // Iterate through all players
            address player = _missionData.players[i];                                           // Get the player address
            total += failedRefundAmounts[player];                                               // Add the player's failed refund amount to the total
        }
    }

    /**
     * @dev Refunds players if the mission fails.
     * This function is internal and can only be called when the mission is in Failed status.
     * It ensures that the mission has ended and that the enrollment period has passed.
     * It refunds all enrolled players their enrollment amount.
     */
    function _refundPlayers()               internal {
        require(_getRealtimeStatus() == Status.Failed,                  "Mission not in Failed status");        // Ensure mission is in Failed status
        require(_missionData.players.length > 0,                         "No players to refund");               // Ensure there are players to refund
        bool _force = true;
        for (uint256 i = 0; i < _missionData.players.length; i++) {
            address player = _missionData.players[i];                                                           // Get the player address
            if (!refunded[player]) {                                                                            // Check if player has not been refunded
                (bool ok, ) = payable(player).call{ value: _missionData.enrollmentAmount }("");                 // Attempt to transfer the refund amount to the player
                if (ok) {
                    refunded[player] = true;                                                                    // If transfer successful, mark player as refunded
                    _missionData.refundedPlayers.push(player);                                                  // If transfer successful, track refunded player
                    emit PlayerRefunded(player, _missionData.enrollmentAmount);                                 // Emit PlayerRefunded event with player address and amount
                } else {
                    failedRefundAmounts[player] += _missionData.enrollmentAmount;                               // Track failed refund amounts for players
                    emit RefundFailed(player, _missionData.enrollmentAmount);                                   // Log the failure, but don’t revert
                    _force = false;                                                                             // Set force to false if any refund fails     
                }
            }
        }
        _setStatus(Status.Failed);                                                                              // Set the mission status to Failed
        if (address(this).balance > 0) {                                                                        // If there are still funds left in the contract
            _withdrawFunds(_force);                                                                             // Withdraw funds to MissionFactory contract 
        }
        emit MissionRefunded(
            _missionData.refundedPlayers.length,                                                                // Emit MissionRefunded event with number of players refunded
            _missionData.enrollmentAmount,                                                                      // Emit MissionRefunded event with amount refunded to each player
            _missionData.refundedPlayers,                                                                       // Emit MissionRefunded event with list of refunded players
            block.timestamp                                                                                     // Emit MissionRefunded event with current timestamp
        );
    }

}

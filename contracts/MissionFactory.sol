// SPDX-License-Identifier: MIT
// #region Introduction
/**
 * © 2025 Be Brave Be Bold Be Banked™ | B6 Labs™ – Swerfer
 * All rights reserved.
 *
 * @title   Be Brave Be Bold Be Banked™ – Mission & Factory Architecture
 * @author  B6 Labs™ – Swerfer
 *
 * @notice
 * ## 🧩 Components (this file)
 * - **MissionFactory** — deploys EIP-1167 clones of `Mission`, tracks status,
 *   enforces weekly/monthly enrollment limits, routes economics, and manages
 *   a per-type reserve of leftover funds.
 * - **Mission** — a time-boxed game with linear, monotonic payouts and
 *   cooldowns between rounds.
 *
 * ## 🔌 External dependencies
 * OpenZeppelin `Ownable`, `ReentrancyGuard`, `Clones`, and `Strings`.
 *
 * ## 🔁 Lifecycle & real-time status
 * The on-chain lifecycle is expressed by `enum Status` and computed on demand:
 * `Pending → Enrolling → Arming → Active ↔ Paused → (PartlySuccess | Success | Failed)`.
 * - **Pending**: clone exists, before enrollment opens.
 * - **Enrolling**: `enrollmentStart..enrollmentEnd`. Players pay `enrollmentAmount` once.
 * - **Arming**: minimum players reached, waiting for `missionStart`.
 * - **Active**: `missionStart..missionEnd`. On each successful round call, one distinct player wins.
 * - **Paused**: enforced cooldown after a round; duration is per-mission.
 * - **PartlySuccess/Success/Failed**: terminal outcomes used for settlement paths.
 *
 * ## 👥 Enrollment limits (anti-addiction)
 * The factory enforces global weekly/monthly limits per address via `canEnroll()` and
 * records enrollments with `recordEnrollment()`. Helper views:
 * `secondsTillWeeklySlot()` and `secondsTillMonthlySlot()`.
 *
 * ## 💰 Payout model (monotonic and time-based)
 * Let `progress = (now - missionStart) / (missionEnd - missionStart)` (scaled).
 * The **cumulative entitlement** is `croStart * progress`. The round payout equals
 * `entitlement - paidSoFar`, clamped to `croCurrent`. A player may win **once** per mission.
 *
 * ## ⏱ Cooldowns
 * Two per-mission parameters are provided during creation:
 * - `roundPauseDuration` — cooldown after non-final rounds (seconds).
 * - `lastRoundPauseDuration` — cooldown before the final round (seconds).
 * **Note:** there are **no enforced on-chain defaults**; any “1 minute” defaults are a UI choice.
 *
 * ## 🔒 InviteOnly secret enrollment
 * InviteOnly missions verify a **private commitment** `_enrollSecretHash`,
 * computed off-chain as `keccak256(passphrase, enrollmentStart)`. The hash is stored
 * **privately** on the mission (not in public `MissionData`) and checked in
 * `enrollPlayerWithSecret(passphrase)`.
 *
 * ## 🏦 Economics & settlement
 * After refunds are secured/reserved:
 * - **InviteOnly**: **100%** of leftover goes to the **factory owner**
 * - **UserMission**: **50%** to the factory owner; **50%** to the **creator**
 * - **Other types**: **25%** to the factory owner; **75%** to the **per-type reserve**
 *   `reservedFunds[missionType]`.
 * On creating a new mission, the factory **optionally boosts** it with
 * `allocation = reservedFunds[type] / 4`, sent to the mission via `increasePot()`.
 *
 * ## 🧾 Ownership & authorization
 * The factory owner can add/remove authorized helpers and manage a two-step ownership
 * transfer (`proposeOwnershipTransfer` / `confirmOwnershipTransfer`). Sensitive functions
 * are protected by `onlyOwner`, `onlyOwnerOrAuthorized`, and `nonReentrant` where applicable.
 *
 * ## 🧭 What to look for in the code
 * - Factory: clone deployment, reserve accounting, enrollment limit bookkeeping,
 *   rich getters for dApps/indexers.
 * - Mission: initialize, enroll (normal & secret), round calling, cooldown checks,
 *   refunds, settlement routing, and detailed views for indexers.
 */
// #endregion Introduction


pragma solidity ^0.8.30;


// #region Imports
import "@openzeppelin/contracts/access/Ownable.sol";
import "@openzeppelin/contracts/proxy/Clones.sol";
import "@openzeppelin/contracts/utils/ReentrancyGuard.sol";
// #endregion





// #region Global Enums
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
    Monthly,        // Default use:  1 week enrollment, 1 hour arming, 30 days  rounds
    InviteOnly,     // Default use:  Custom enrollment, custom arming,   custom rounds
    UserMission     // Default use:  Custom enrollment, custom arming,   custom rounds
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
// #endregion




// ############################################################
// ####                                                    ####
// ####     MissionFactory size is very close to the       ####
// ####     maximum! Do not add code or it will refert     ####
// ####                                                    ####
// ############################################################

// #region Contr. MissionFactory
contract MissionFactory is Ownable, ReentrancyGuard {
    using Clones    for address;



    // #region Events
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
        uint8           roundPauseDuration,
        uint8           lastRoundPauseDuration,
        uint256         enrollmentAmount,
        uint256         missionStart,
        uint256         missionEnd,
        uint8           missionRounds,
        address         creator
    );
    event AuthorizedAddressAdded                (address        indexed addr                                                                        );
    event AuthorizedAddressRemoved              (address        indexed addr                                                                        );
    event MissionFundsRegistered                (uint256                amount,         MissionType indexed missionType,    address indexed sender  );
    event FundsWithdrawn                        (address        indexed to,             uint256             amount                                  );    
    event OwnershipTransferProposed             (address        indexed proposer,       address             newOwner,       uint256 timestamp       );
    event OwnershipTransferConfirmed            (address        indexed confirmer,      address             newOwner,       uint256 timestamp       );
    event MissionStatusUpdated                  (address        indexed mission,        uint8       indexed fromStatus,     uint8   indexed toStatus, uint256        timestamp);
    event MissionFinalized                      (address        indexed mission,        uint8       indexed finalStatus,    uint256 timestamp       );
    // #endregion





    // #region Modifiers
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
    // #endregion





    // #region State Variables
    /**
     * @dev State variables for the MissionFactory contract.
     * These variables store the state of the contract, including authorized addresses, reserved funds, mission statuses, and the implementation address for missions.
     */
    address[]                               public  missions;                                   // Array to hold all mission addresses
    uint8                                   public  weeklyLimit = 7;                            // Maximum number of missions a player can enroll in per week
    uint8                                   public  monthlyLimit = 15;                          // Maximum number of missions a player can enroll in per month
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
    mapping(address => uint256)             public lastUserMissionCreatedAt;                    // creator => last creation timestamp

    // --- Change Tracking (predictable polling; no events) -------------------
    struct ChangeEntry {
        address mission;                                                                        // mission address
        uint40  ts;                                                                             // last touch timestamp (seconds)
        uint64  seq;                                                                            // monotonic change sequence
        uint8   status;                                                                         // factory-known status
    }

    uint64                                  private _changeSeq;                                 // global increasing sequence
    address[]                               private _changedKeys;                               // each mission appears at most once
    mapping(address => uint32)              private _changedIndexPlus1;                         // 0 = absent, else index + 1
    mapping(address => ChangeEntry)         private _changed;                                   // last change per mission
    uint32                                  private _purgeCursor;                               // rotating cursor for amortized purge
    uint32                                  private constant _PURGE_BATCH_SIZE = 8;             // purge batch size per ended transition
    // -----------------------------------------------------------------------

    // #endregion





    // #region Constructor
    /**
     * @dev Struct to hold information about players who won the mission.
     * Contains the player's address and the amount they won.
     */
    constructor(address _impl) Ownable(msg.sender) {
        require(_impl != address(0), "impl zero");
        missionImplementation = _impl;
    }
    // #endregion





    // #region Helper functions
    /**
     * @dev Function to convert mission types to human readable names 
     */  
    function _toHumanReadableName(MissionType t)                                    internal pure returns (string memory) {
        if (t == MissionType.Hourly)         return "Hourly";
        if (t == MissionType.QuarterDaily)   return "QuarterDaily";
        if (t == MissionType.BiDaily)        return "BiDaily";
        if (t == MissionType.Daily)          return "Daily";
        if (t == MissionType.Weekly)         return "Weekly";
        if (t == MissionType.Monthly)        return "Monthly";
        if (t == MissionType.InviteOnly)     return "InviteOnly";
        return "Custom";                      
    }

    /**
     * @dev Returns the time until the next weekly slot for a user.
     * This function calculates the time remaining until the next weekly slot based on the user's enrollment history.
     * @param user The address of the user to check.
     * @return The number of seconds until the next weekly slot.
     */
    function secondsTillWeeklySlot(address user)                                    external view returns (uint256) {
        uint256 nowTs = block.timestamp;                                // Get the current timestamp
        uint256[] storage h = _enrollmentHistory[user];                 // Get the user's enrollment history
        uint256 earliest;                                               // Variable to store the earliest enrollment time within the next week
        for (uint i = 0; i < h.length; i++) {                           // Loop through the enrollment history  
            if (h[i] + 7 days > nowTs) {
                if (earliest == 0 || h[i] < earliest) earliest = h[i];  // If this is the first valid enrollment or earlier than the current earliest, update earliest
            }
        }
        return earliest == 0 ? 0 : earliest + 7 days - nowTs;           // If no valid enrollment found, return 0; otherwise, return the time until the next weekly slot
    }

    /**
     * @dev Returns the time until the next monthly slot for a user.
     * This function calculates the time remaining until the next monthly slot based on the user's enrollment history.
     * @param user The address of the user to check.
     * @return The number of seconds until the next monthly slot.
     */
    function secondsTillMonthlySlot(address user)                                   external view returns (uint256) {
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
    // #endregion





    // #region Int. Change Tracking
    function _touchChanged(address mission_, uint8 status_)                         internal {
        unchecked { _changeSeq++; }
        uint32 idxPlus1 = _changedIndexPlus1[mission_];

        ChangeEntry memory ce = ChangeEntry({
            mission: mission_,
            ts: uint40(block.timestamp),
            seq: _changeSeq,
            status: status_
        });
        _changed[mission_] = ce;

        if (idxPlus1 == 0) {
            _changedKeys.push(mission_);
            _changedIndexPlus1[mission_] = uint32(_changedKeys.length);
        }
    }

    function _touchChangedKeepStatus(address mission_)                              internal {
        _touchChanged(mission_, uint8(missionStatus[mission_]));
    }

    function _isEnded(Status s)                                                     internal pure returns (bool) {
        return (s == Status.Success || s == Status.Failed); // ended = 6,7
    }

    /// @dev amortized cleanup; scans up to `maxToScan` entries starting at `_purgeCursor`.
    function _purgeEndedBatch(uint32 maxToScan)                                     internal {
        uint256 len = _changedKeys.length;
        if (len == 0) { _purgeCursor = 0; return; }

        uint32 scanned = 0;
        uint40 cutoff = uint40(block.timestamp - 7 days);

        while (scanned < maxToScan && _changedKeys.length > 0) {
            if (_purgeCursor >= _changedKeys.length) {
                _purgeCursor = 0;
            }
            address m = _changedKeys[_purgeCursor];
            ChangeEntry memory ce = _changed[m];

            bool ended = _isEnded(Status(uint8(ce.status)));
            if (ended && ce.ts <= cutoff) {
                uint256 last = _changedKeys.length - 1;
                if (_purgeCursor != last) {
                    address moved = _changedKeys[last];
                    _changedKeys[_purgeCursor] = moved;
                    _changedIndexPlus1[moved] = uint32(_purgeCursor + 1);
                }
                _changedKeys.pop();
                _changedIndexPlus1[m] = 0;
                delete _changed[m];
                // note: keep _purgeCursor at same index to inspect the swapped-in element next
            } else {
                _purgeCursor++;
                scanned++;
            }
        }
    }

    /**
     * @notice Mission notifies the factory that on-chain state changed (no status change required).
     * @dev Keeps polling predictable without events. Callable only by registered missions.
     */
    function notifyTouched()                                                        external onlyMission {
        _touchChangedKeepStatus(msg.sender);
    }

    // #endregion





    // #region Anti-addiction Func.
    /**
     * @dev Sets the weekly and monthly enrollment limits.
     * This function allows the owner or an authorized address to set the limits for how many missions a user can enroll in per week and per month.
     * @param _weekly The new weekly limit for mission enrollments.
     * @param _monthly The new monthly limit for mission enrollments.
     */
    function setEnrollmentLimits(uint8 _weekly, uint8 _monthly)                     external onlyOwnerOrAuthorized {
        weeklyLimit = _weekly;
        monthlyLimit = _monthly;
    }

    /**
     * @dev Checks if a user can enroll in a mission based on anti-addiction limits.
     * This function checks the user's enrollment history to determine if they have exceeded the weekly or monthly limits.
     * @param user The address of the user to check.
     * @return ok A boolean indicating if the user can enroll.
     * @return breach A Limit enum indicating which limit is breached, if any.
     */
    function canEnroll(address user)                                                public view returns (bool ok, Limit breach) {
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
    function recordEnrollment(address user)                                         external onlyMission() {
        uint256 nowTs = block.timestamp;                                            // Get the current timestamp
        require(missionStatus[msg.sender] == Status.Enrolling);                     // Ensure the caller is in the Enrolling status

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
        // touch changed set (status stays the same)
        _touchChangedKeepStatus(msg.sender);
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
    function getPlayerLimits(address player)                                        external view returns 
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

    /**
     * @dev Undoes a user's enrollment in a mission within a specified time window.
     * This function is called by a mission to remove a user's enrollment record if they are refunded.
     * It searches the user's enrollment history for a timestamp within the specified window and removes it.
     * @param user The address of the user whose enrollment is to be undone.
     * @param startTs The start timestamp of the enrollment window.
     * @param endTs The end timestamp of the enrollment window.
     */
    function undoEnrollmentInWindow(address user, uint256 startTs, uint256 endTs)   external onlyMission {
        uint256[] storage h = _enrollmentHistory[user];
        uint256 len = h.length;
        if (len == 0) return;

        // Remove exactly one timestamp that falls inside this mission’s enrollment window.
        for (uint256 i = 0; i < len; ++i) {
            uint256 t = h[i];
            if (t >= startTs && t <= endTs) {
                // Keep chronological order (important for pruning logic): shift left, then pop.
                for (uint256 j = i; j + 1 < len; ++j) {
                    h[j] = h[j + 1];
                }
                h.pop();
                return;
            }
        }
    }

    // #endregion





    // #region Admin Functions
    /**
     * @dev Adds an address to the list of authorized addresses.
     * @param account The address to authorize.
     */
    function addAuthorizedAddress(address account)                                  external onlyOwnerOrAuthorized {
        require(account != address(0),  "addr?");                                   // Ensure the account is valid
        require(!authorized[account],   "Already authorized");                      // Ensure the account is not already authorized
        authorized[account] = true;                                                 // Add authorization for the account  
        emit AuthorizedAddressAdded(account);                                       // Emit event for addition of authorization
    }

    /**
     * @dev Removes authorization for an address.
     * @param account The address to remove authorization from.
     */
    function removeAuthorizedAddress(address account)                               external onlyOwnerOrAuthorized {
        require(account != address(0),  "Addr?");                                   // Ensure the account is valid
        require(authorized[account],    "!authorized");                             // Ensure the account is currently authorized
        authorized[account] = false;                                                // Remove authorization for the account
        emit AuthorizedAddressRemoved(account);                                     // Emit event for removal of authorization
    }

    /**
     * @dev Proposes a transfer of ownership to a new address.
     * @param newOwner The address of the new owner.
     * If the owner is not available anymore or lost access, this function allows an authorized address to propose a new owner.
     */
    function proposeOwnershipTransfer(address newOwner)                             external onlyOwnerOrAuthorized {
        uint256 nowTs = block.timestamp;                                // Get the current timestamp
        require(newOwner != address(0), "Addr?");                       // Ensure the new owner is a valid address
        proposedNewOwner = newOwner;
        proposalProposer = msg.sender;
        proposalTimestamp = block.timestamp;
        emit OwnershipTransferProposed(msg.sender, newOwner, nowTs);    // Emit event for ownership transfer proposal
    }

    /**
     * @dev Confirms the ownership transfer to a new address.
     * This function allows a 2nd authorized address to confirm the ownership transfer.
     */
    function confirmOwnershipTransfer()                                             external onlyOwnerOrAuthorized {
        uint256 nowTs = block.timestamp;                                                                // Get the current timestamp
        require(proposalProposer != msg.sender, "!Own proposal");                                       // Ensure the confirmer is not the proposer
        require(block.timestamp <= proposalTimestamp + OWNERSHIP_PROPOSAL_WINDOW, "Proposal expired");  // Ensure the proposal is still valid within the proposal window

        // Transfer ownership
        _transferOwnership(proposedNewOwner);                                                           // Transfer ownership to the new owner   

        emit OwnershipTransferConfirmed(msg.sender, proposedNewOwner, nowTs);                           // Emit event for ownership transfer confirmation
        // Cleanup
        delete proposedNewOwner;                                                                        // Delete the new owner
        delete proposalProposer;                                                                        // Delete the proposal proposer
        delete proposalTimestamp;                                                                       // Delete the proposal timestamp

    }
    // #endregion





    // #region Core Factory Func.
    /**
     * @dev Creates a new mission with the specified parameters.
     * @param _missionType              The type of the mission.
     * @param _enrollmentStart          The start time for enrollment.
     * @param _enrollmentEnd            The end time for enrollment.
     * @param _enrollmentAmount         The amount required for enrollment.
     * @param _enrollmentMinPlayers     The minimum number of players required to start the mission.
     * @param _enrollmentMaxPlayers     The maxnimum number of players required to start the mission.
     * @param _roundPauseDuration       The duration of pause between rounds in seconds.
     * @param _lastRoundPauseDuration   The duration of pause before the last round in seconds
     * @param _missionStart             The start time for the mission.
     * @param _missionEnd               The end time for the mission.
     * @param _missionRounds            The number of rounds in the mission.
     */
    function createMission (
        MissionType     _missionType,           // Type of the mission
        uint256         _enrollmentStart,       // Start time for enrollment
        uint256         _enrollmentEnd,         // End time for enrollment
        uint256         _enrollmentAmount,      // Amount required for enrollment
        uint8           _enrollmentMinPlayers,  // Minimum number of players required to start the mission
        uint8           _enrollmentMaxPlayers,  // Maximum number of players required to start the mission
        uint8           _roundPauseDuration,    // Duration of pause between rounds in seconds
        uint8           _lastRoundPauseDuration,// Duration of pause before the last round in seconds
        uint256         _missionStart,          // Start time for the mission
        uint256         _missionEnd,            // End time for the mission
        uint8           _missionRounds,         // Number of rounds in the mission
        string calldata _missionName,           // The mission name (optional)
        bytes32         _pinHash,               // The mission pin hash (optional)
        address         _creator                // The user address for UserMission type
        ) external payable onlyOwnerOrAuthorized nonReentrant returns (address, string memory) {
            if (_missionType == MissionType.InviteOnly || _missionType == MissionType.UserMission) {
                // Specified rules for user-created missions
                require(_missionRounds >= 2,                                "Rounds>1");                                                        // Ensure mission rounds is at least 2
                require(_enrollmentMinPlayers >= 3,                         "Min players>2");                                                   // Ensure minimum players is at least 3
                require(_enrollmentMaxPlayers <= 25,                        "Max players<26");                                                  // Ensure maximum players is at most 25
                require(_missionRounds <= _enrollmentMinPlayers - 1,        "Rounds<=minPlay-1");                                               // Ensure mission rounds is at most minimum players - 1
                require(_enrollmentAmount >= 1,                             "Fee>=1");                                                          // Ensure enrollment amount is at least 1 CRO
                if (_missionType == MissionType.InviteOnly) {
                    require(_pinHash != bytes32(0),                         "pinHash?");                                                        // Ensure pin hash is provided
                } else {
                    require(_creator != address(0),                         "Creator addr?");                                                   // Ensure user address is provided
                    require(block.timestamp >= lastUserMissionCreatedAt[_creator] + 1 days, "Min 24h");                                         // Ensure at least 1 day gap between user mission creations
                }
            } else {
                require(_missionRounds          >= 1,                       "Mission rnds<1");                                                  // Ensure mission rounds is greater than or equal to 1
                require(_enrollmentMinPlayers   >= _missionRounds,          "Min players<mission rnds");                                        // Ensure minimum players is at least equal to mission rounds
                require(_enrollmentMaxPlayers   >= _enrollmentMinPlayers,   "Max players<minimum players");                                     // Ensure maximum players is at least equal to minimum players
                require(_enrollmentMaxPlayers   <= 100,                     "max players<=100");                                                // Ensure maximum players is at most 100
            }
            require(_missionStart           >= _enrollmentEnd,              "M start<enroll end");                                              // Ensure mission start is on or after enrollment end
            require(_missionEnd             >  _missionStart,               "M start>=end");                                                    // Ensure mission start is before end
            require(_roundPauseDuration     >= 60,                          "Round pause duration<60s");                                        // Ensure round pause duration is at least 1 minute
            require(_lastRoundPauseDuration >= 60,                          "Last round pause duration<60s");                                   // Ensure last round pause duration is at least 1 minute
            require(_enrollmentStart        <  _enrollmentEnd,              "Enroll start>=end");                                               // Ensure enrollment start is before end
			address clone = missionImplementation.clone(); 	    // EIP-1167 minimal proxy

            require(bytes(_missionName).length > 0,                         "Mission name?");                                                   // Ensure a mission name is provided

            isMission[clone]     = true;                        // mark as a valid mission
            missionStatus[clone] = Status.Pending;              // placeholder so first callback passes onlyMission
            missionNames[clone] = _missionName;                 // Store the supplied name

            Mission(payable(clone)).initialize{value: msg.value} (
				owner(),									    // Set the owner of the mission to the owner of MissionFactory
				address(this),								    // Set the MissionFactory address
                _missionType,                                   // Set the type of the mission
                _enrollmentStart,                               // Set the enrollment start time
                _enrollmentEnd,                                 // Set the enrollment end time
                _enrollmentAmount,                              // Set the enrollment amount
                _enrollmentMinPlayers,                          // Set the minimum players required
                _enrollmentMaxPlayers,                          // Set the maximum players allowed
                _roundPauseDuration,                            // Set the pause duration between rounds
                _lastRoundPauseDuration,                        // Set the pause duration before the last round
                _missionStart,                                  // Set the mission start time
                _missionEnd,                                    // Set the mission end time
                _missionRounds,                                 // Set the number of rounds in the mission
                _missionName,                                     // The supplied name or calculated name if nothing supplied
                _pinHash,                                       // The mission pin hash (optional)
                _creator                                        // The user address for UserMission type
            );

        missions.push(clone);                                   // Add the new mission to the list of missions
        emit MissionCreated(
            clone,
            _missionName,
            _missionType,
            _enrollmentStart,
            _enrollmentEnd,
            _enrollmentMinPlayers,
            _enrollmentMaxPlayers,
            _roundPauseDuration,
            _lastRoundPauseDuration,
            _enrollmentAmount,
            _missionStart,
            _missionEnd,
            _missionRounds,
            _creator
        );                                                              // Emit event for mission creation

        // Calculate allocation based on mission type
        uint256 allocation = reservedFunds[_missionType] / 4;           // Missions get 1/4th of the reserved funds

        if (allocation > 0 && address(this).balance >= allocation) {
            reservedFunds[_missionType] -= allocation;
            Mission(payable(clone)).increasePot{value: allocation}();   // Sends CRO and updates mission accounting
        }
        if (_missionType == MissionType.UserMission) {
            lastUserMissionCreatedAt[_creator] = block.timestamp;        // Update last creation timestamp for user-created missions
        }
        // initial touch for predictable polling
        _touchChangedKeepStatus(clone);
        return (clone, _missionName);						                // Return the address of the newly created mission
    }

    /**
     * @dev Sets the status of a mission.
     * @param newStatus The new status to set for the mission.
     */
    function setMissionStatus(Status newStatus)                                     external onlyMission {
        Status fromStatus = missionStatus[msg.sender];
        missionStatus[msg.sender] = newStatus;

        // touch changed set for predictable polling
        _touchChanged(msg.sender, uint8(newStatus));

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
            // bounded on-the-fly purge only for true ended (6 or 7)
            if (newStatus == Status.Success || newStatus == Status.Failed) {
                _purgeEndedBatch(_PURGE_BATCH_SIZE);
            }

            emit MissionFinalized(msg.sender, uint8(newStatus), block.timestamp);
        }
    }

    // #endregion





    // #region Financial Functions
    /**
     * @dev Registers mission funds for a specific mission type.
     * @param missionType The type of the mission.
     */
    function registerMissionFunds(MissionType missionType)                          external payable onlyMission nonReentrant {
        bool isEndedMission = missionStatus[msg.sender] == Status.Success || missionStatus[msg.sender] == Status.Failed;    // Accept only missions that have ended (Success or Failed)
        require(isEndedMission);                                                                                            // Ensure the caller is a valid mission that has ended 
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
    function reservedFundsBreakdown()                                               external view returns (uint256[9] memory) {
        uint256[9] memory breakdown;                        // Array to hold the breakdown of reserved funds for each mission type
        for (uint256 i = 0; i < 9; i++) {
            breakdown[i] = reservedFunds[MissionType(i)];   // Fill the array with the reserved funds for each mission type
        }
        return breakdown;                                   // Return the breakdown of reserved funds
    }

    /**
     * @dev Receives funds sent to the contract.
     * This function is called when the contract receives CRO without any data.
     * It allows the contract to accept CRO transfers.
     */
    receive()                                                                       external payable {}

    /**
     * @dev Fallback function to receive CRO.
     * This function is called when the contract receives CRO without any data.
     * It allows the contract to accept CRO transfers.
     */
    fallback()                                                                      external payable {}

    /**
     * @dev Withdraws funds from the MissionFactory contract.
     * This function allows the owner or an authorized address to withdraw funds from the contract.
     * This function shall only be called if the contract is not in use anymore and all missions have ended.
     * It transfers the specified amount of funds to the owner of the MissionFactory contract.
     * @param amount The amount of funds to withdraw. If 0, withdraws all available funds.
     */
    function withdrawFunds(uint256 amount)                                          external onlyOwner nonReentrant {
        address mgrOwner = owner();                                         // Get the owner of the MissionFactory contract
        require(mgrOwner != address(0), "Not owner");                       // Ensure the manager owner is valid
        if (amount == 0) {
            amount = address(this).balance;                                 // If no amount specified, withdraw all funds
        }
        require(amount <= address(this).balance, "> balance");              // Ensure the contract has enough balance to withdraw
        (bool ok, ) = payable(mgrOwner).call{ value: amount }("");          // Attempt to transfer the specified amount to the manager owner
        require(ok, "TX failed");                                           // Ensure the transfer was successful
        emit FundsWithdrawn(mgrOwner, amount);                              // Emit event for funds withdrawal
    }
    // #endregion





    // #region View Functions

    /**
     * @dev Returns the missions a player is participating in and their statuses.
     * This function retrieves all missions the player is enrolled in and their current statuses.
     * @param player The address of the player to check.
     * @return joined An array of addresses of the missions the player is enrolled in.
     * @return statuses An array of statuses corresponding to each mission.
     */
    function getPlayerParticipation(address player)                                 public view returns (address[] memory, Status[] memory, string[] memory) {
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
    function getFactorySummary()                                                    public view
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
    function getMissionData(address missionAddress)                                 external view returns (Mission.MissionData memory) {
        require(missionAddress != address(0), "Invalid mission address");          // Ensure mission address is valid
        return Mission(payable(missionAddress)).getMissionData();                           // Return the mission data from the Mission contract
    }

    /**
     * @dev Returns the total number of missions.
     * This function returns the length of the missions array, which contains all mission addresses.
     * @return The total number of missions.
     */
    function getTotalMissions()                                                     external view returns (uint256) {
        return missions.length;             // Return the total number of missions
    }

    /**
     * @dev Returns the addresses and statuses of all missions.
     * This function retrieves all missions and their statuses, filtering out old missions.
     * @return An array of mission addresses and an array of their corresponding statuses.
     */
    function getAllMissions()                                                       external view returns (address[] memory, Status[] memory, string[] memory) {
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
    function getMissionsByStatus(Status s)                                          external view returns (address[] memory, uint8[] memory, string[] memory) {
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
    
    function getMissionsNotEnded()                                                  external view returns (address[] memory, uint8[] memory, string[] memory) {
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
    function getMissionsEnded()                                                     external view returns (address[] memory, uint8[] memory, string[] memory) {
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

    /**
     * @dev Returns the addresses of the latest n missions.
     * This function retrieves the last n missions from the list of all missions.
     * @param n The number of latest missions to return.
     * @return An array of mission addresses and an array of their corresponding statuses.
     */
    function getLatestMissions(uint256 n)                                           external view returns (address[] memory, uint8[] memory, string[] memory) {
        uint256 total = missions.length;                    // Get the total number of missions
        if (n > total) n = total;                           // If n is greater than the total number of missions, adjust n to total

        address[] memory result   = new address[](n);       // Create an array to hold the addresses of the latest missions
        uint8[]   memory statuses = new uint8[](n);         // Create a parallel array for statuses
        string[]  memory names    = new string[](n);        // Create an array to hold the mission names

        for (uint256 i = 0; i < n; i++) {                   // Loop through the last n missions
            address m = missions[total - 1 - i];            // Get the address of the mission
            result[i] = m;                                  // Add the mission address to the result array  
            statuses[i] = uint8(missionStatus[m]);          // Add the status of the mission to the statuses array
            names[i] = missionNames[m];                     // Add the mission name to the output array
       }

        return (result, statuses, names);                   // Return arrays: addresses of missions not ended, their statuses and names  
    }

    /**
     * @dev Returns the reserved funds for a specific mission type.
     * @param _type The type of the mission to check.
     * @return The amount of reserved funds for the specified mission type.
     */
    function getFundsByType(MissionType _type)                                      external view returns (uint256) {
        return reservedFunds[_type];                                                // Return the reserved funds for the specified mission type
    }

    /**
     * @dev Returns the proposal data
     * @return newOwner the stored newOwner proposal
     * @return proposer the proposer
     * @return timestamp the time of the proposal
     * @return timeLeft the time left
     */   
    function getOwnershipProposal()                                                 external view returns (address newOwner, address proposer, uint256 timestamp, uint256 timeLeft) {
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

    /**
     * @notice Returns missions changed after `lastSeq`.
     * @dev Pass 0 initially; indexer stores and supplies the last seen sequence.
     */
    function getChangesAfter(uint64 lastSeq)                                        external view returns (address[] memory m, uint40[] memory timestamps, uint64[] memory seqs, uint8[] memory statuses) {
        uint256 n = _changedKeys.length;

        // count
        uint256 count = 0;
        for (uint256 i = 0; i < n; i++) {
            ChangeEntry memory ce = _changed[_changedKeys[i]];
            if (ce.seq > lastSeq) { count++; }
        }

        m          = new address[](count);
        timestamps = new  uint40[](count);
        seqs       = new  uint64[](count);
        statuses   = new   uint8[](count);

        // fill
        uint256 w = 0;
        for (uint256 i = 0; i < n; i++) {
            ChangeEntry memory ce = _changed[_changedKeys[i]];
            if (ce.seq > lastSeq) {
                m[w]          = ce.mission;
                timestamps[w] = ce.ts;
                seqs[w]       = ce.seq;
                statuses[w]   = ce.status;
                w++;
            }
        }
    }

    // #endregion 

    // #endregion
}




// #region Contract Mission
contract Mission        is Ownable, ReentrancyGuard {




    // #region Events
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





    // #region Player custom errors
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





    // #region Modifiers
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
    // #endregion





    // #region Data Structures
    /**
    * @dev Unified per-player record.
    * One record per participant; enriched over the mission lifecycle.
    */
    struct Players {
            address player;         // Player wallet address
            uint256 enrolledTS;     // Timestamp the player enrolled (0 if not set)
            uint256 amountWon;      // Amount won by the player (0 if never won)
            uint256 wonTS;          // Timestamp of the round win (0 if never won)
            bool    refunded;       // True if refund succeeded
            bool    refundFailed;   // True if a refund attempt failed
            uint256 refundTS;       // Timestamp of refund attempt (0 if none)
    }

    /**
    * @dev Struct to hold all mission data.
    * Contains information about players, mission status, enrollment details, and financials.
    */
    struct MissionData {
            Status          status;                         // Real-time status computed at call time
                    
            uint256         missionCreated;                 // Timestamp of when the mission was created, used for 'Pending' stage in dApp

            string          name;                           // Name of the mission
            MissionType     missionType;                    // Type of the mission
            uint8           missionRounds;                  // Total number of rounds in the mission
            uint8			roundPauseDuration;			    // Cooldown duration: rounds before the penultimate round
            uint8			lastRoundPauseDuration;		    // Cooldown duration: before final round
            uint256         croInitial;                     // Initial CRO amount at the creation of the mission + added by increasePot function
            uint256         croStart;                       // Initial CRO amount at the start of the mission. croInitial + enrollment fee * players
            uint256         croCurrent;                     // Current CRO amount in the mission
            uint256         enrollmentAmount;               // Amount required for enrollment
            uint8           enrollmentMinPlayers;           // Minimum number of players required to start the mission
            uint8           enrollmentMaxPlayers;           // Maximum number of players allowed in the mission

            uint256         enrollmentStart;                // Start and end times for enrollment
            uint256         enrollmentEnd;                  // Start and end times for enrollment
            uint256         missionStart;                   // Start time for the mission
            uint256         missionEnd;                     // End time for the mission

            Players[]       players;                        // Unified per-player records (address, timestamps, win/refund info)
            uint8           enrollmentCount;                // Number of players enrolled (non-decreasing)
            uint8           roundCount;                     // Current round count 
            uint256         pauseTimestamp;                 // Time when the mission was paused
            bool            allRefunded;                    // True when all enrolled players are refunded and none failed
            address         creator;                        // Address of the mission creator
    }
    // #endregion





    // #region State Variables
    MissionFactory              public  missionFactory;                             // Reference to the MissionFactory contract
    uint256                     public  ownerShare;                                 // Total share of funds for the owner
    uint256                     public  factoryShare;                               // Total share of funds for the MissionFactory
    bool                        public  missionStartConditionChecked;               // Flag to check if the mission start condition has been checked
    MissionData                 private _missionData;                               // Struct to hold all mission data  
    bytes32                     private _enrollSecretHash;                          // Private hash for InviteOnly enrollment verification
    bool                        private _initialized;                               // Flag to track if the contract has been initialized
    Status                      private _previousStatus;                            // Track the previous status of the mission

    /**
    * @dev Unified per-player records + O(1) index map.
    * `NOT_ENROLLED` sentinel prevents accidental zero-index lookups.
    */
    Players[]                   private _players;                                   // Unified per-player storage (address, timestamps, win/refund info)
    mapping(address => uint8)   private _pIndexPlus1;                               // Address -> (index in _players) + 1 ; 0 means "not enrolled"
    uint8                       private constant NOT_ENROLLED = type(uint8).max;    // 255 sentinel for "not enrolled"
    // #endregion





    // #region Constructor-Initializer
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
        uint8           _roundPauseDuration,
        uint8           _lastRoundPauseDuration,
        uint256         _missionStart,
        uint256         _missionEnd,
        uint8           _missionRounds,
        string calldata _name,
        bytes32         _pinHash,
        address         _creator
    )                                                           external payable nonReentrant {
        require(!_initialized, "Already initialized");

        _initialized = true;

        _transferOwnership(_owner);
        missionFactory = MissionFactory(payable(_missionFactory));

        // Initialize mission data (unified layout)
        _missionData.missionType             = _missionType;
        _missionData.missionCreated          = block.timestamp;
        _missionData.name                    = _name;

        _missionData.missionRounds           = _missionRounds;
        _missionData.roundPauseDuration      = _roundPauseDuration;
        _missionData.lastRoundPauseDuration  = _lastRoundPauseDuration;

        _missionData.croInitial              = msg.value;
        _missionData.croStart                = msg.value;
        _missionData.croCurrent              = msg.value;

        _missionData.enrollmentAmount        = _enrollmentAmount;
        _missionData.enrollmentMinPlayers    = _enrollmentMinPlayers;
        _missionData.enrollmentMaxPlayers    = _enrollmentMaxPlayers;

        _missionData.enrollmentStart         = _enrollmentStart;
        _missionData.enrollmentEnd           = _enrollmentEnd;
        _missionData.missionStart            = _missionStart;
        _missionData.missionEnd              = _missionEnd;

        // Dynamics
        delete _players;                    // clear storage array (fresh clone anyway)
        delete _missionData.players;        // clear storage array (fresh clone anyway)  
        _missionData.enrollmentCount        = 0;
        _missionData.roundCount             = 0;
        _missionData.pauseTimestamp         = 0;
        _missionData.allRefunded            = false;
        _enrollSecretHash                   = _pinHash;
        _missionData.creator                = _creator;

        emit MissionInitialized(_owner, _missionType, block.timestamp);
    }

    // #endregion





    // #region Core Mission Functions
    /**
    * @notice Allows a player to enroll by paying the enrollment fee.
    * @dev For normal missions. InviteOnly missions must call enrollPlayerWithSecret().
    */
    function enrollPlayer()                                     external payable nonReentrant {
        require(_missionData.missionType != MissionType.InviteOnly, "InviteOnly: use enrollPlayerWithSecret");

        uint256 nowTs = block.timestamp;
        address player = msg.sender;

        if (player.code.length > 0)                 revert ContractsNotAllowed();
        if (nowTs < _missionData.enrollmentStart)   revert EnrollmentNotStarted(nowTs, _missionData.enrollmentStart);
        if (nowTs > _missionData.enrollmentEnd)     revert EnrollmentClosed(nowTs, _missionData.enrollmentEnd);

        _enroll(player, nowTs); // shared internal logic
    }

    /**
    * @dev Enroll for InviteOnly missions using a 4-digit PIN.
    * `salt` is optional but must match what the creator used to build the on-chain hash.
    */
    function enrollPlayerWithSecret(string calldata passphrase) external payable nonReentrant {
        require(_missionData.missionType == MissionType.InviteOnly, "Not an InviteOnly mission");

        // verify hash commitment
        bytes32 h = keccak256(abi.encodePacked(passphrase, _missionData.enrollmentStart));
        require(h == _enrollSecretHash, "Wrong Secret Passphrase");

        uint256 nowTs = block.timestamp;
        address player = msg.sender;

        if (player.code.length > 0)                 revert ContractsNotAllowed();
        if (nowTs < _missionData.enrollmentStart)   revert EnrollmentNotStarted(nowTs, _missionData.enrollmentStart);
        if (nowTs > _missionData.enrollmentEnd)     revert EnrollmentClosed(nowTs, _missionData.enrollmentEnd);

        _enroll(player, nowTs); // shared internal logic
    }

    /**
    * @dev Shared internal enrollment logic used by both enrollPlayer() and enrollPlayerWithSecret().
    */
    function _enroll(address player, uint256 nowTs)             private {
        if (_missionData.enrollmentCount >= _missionData.enrollmentMaxPlayers) {
            revert MaxPlayers(_missionData.enrollmentMaxPlayers);
        }
        if (msg.value != _missionData.enrollmentAmount) {
            revert WrongEntryFee(_missionData.enrollmentAmount, msg.value);
        }
        if (_pIndexPlus1[player] != 0) revert AlreadyJoined();

        (bool ok, Limit breach) = missionFactory.canEnroll(player);
        if (!ok) {
            if (breach == Limit.Weekly) {
                revert WeeklyLimit(missionFactory.secondsTillWeeklySlot(player));
            } else {
                revert MonthlyLimit(missionFactory.secondsTillMonthlySlot(player));
            }
        }

        // create per-player record
        _players.push(Players({
            player:         player,
            enrolledTS:     nowTs,
            amountWon:      0,
            wonTS:          0,
            refunded:       false,
            refundFailed:   false,
            refundTS:       0
        }));
        _pIndexPlus1[player] = uint8(_players.length); // store index+1

        // mirror into mission data for the single-call getter
        Players memory snap = _players[_players.length - 1];
        _missionData.players.push(snap);
        _missionData.enrollmentCount += 1;

        // funds
        _missionData.croStart   += msg.value;
        _missionData.croCurrent += msg.value;

        if (_previousStatus != Status.Enrolling) {
            _setStatus(Status.Enrolling);
        }
        missionFactory.recordEnrollment(player);
        emit PlayerEnrolled(player, msg.value, _missionData.enrollmentCount);
    }

    /**
     * @dev Checks if the mission's conditions are met to start.
     * Only callable by the owner or an authorized address
     * This function must be called after the enrollment period ends and before the mission starts to
     * refund players if the conditions are not met. If calling the function is obmitted, 
     * calling refundPlayers() is the last chance to refund players.
     * @dev If conditions are not met, sets status to Failed and refunds players.
     */
    function checkMissionStartCondition()                       external nonReentrant onlyOwnerOrAuthorized { 
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
     *      - Mission is in Cooldown
     *      - Mission is not Active
     *      - Mission has ended
     *      - Player has already won
     *      - Player has not joined
     *      - All rounds have been completed
     *      - Payout to the player fails
     * @dev If it is the last round, sets status to Success and withdraws funds
     */
    function callRound()                                        external nonReentrant {
        Status s = _getRealtimeStatus();
        uint256 nowTs = block.timestamp;

        if (s == Status.Paused) {
            uint256 cd = (_missionData.roundCount + 1 == _missionData.missionRounds)
                ? _missionData.lastRoundPauseDuration
                : _missionData.roundPauseDuration;
            uint256 secsLeft = _missionData.pauseTimestamp + cd - nowTs;
                                                                    revert Cooldown(secsLeft);
        }
        if (s < Status.Active)                                      revert NotActive(nowTs, _missionData.missionStart);
        if (s > Status.Active)                                      revert MissionEnded();

        uint8 p1 = _pIndexPlus1[msg.sender];
        if (p1 == 0)                                                revert NotJoined();
        Players storage P = _players[p1 - 1];
        if (P.amountWon > 0 || P.wonTS > 0)                         revert AlreadyWon();

        if (_missionData.roundCount >= _missionData.missionRounds)  revert AllRoundsDone();

        uint256 progress = (nowTs - _missionData.missionStart) * 1e10
                        / (_missionData.missionEnd - _missionData.missionStart);

        uint256 paidSoFar    = _missionData.croStart - _missionData.croCurrent;
        uint256 expectedPaid = (_missionData.croStart * progress) / 1e10;
        require(expectedPaid >= paidSoFar, "Progress regression");

        uint256 payout = expectedPaid - paidSoFar;
        if (payout > _missionData.croCurrent) {
            payout = _missionData.croCurrent;
        }
        require(payout > 0, "No incremental payout");

        // update funds and round
        _missionData.croCurrent -= payout;
        _missionData.roundCount++;

        // winner info (storage + mirror in getter array)
        P.amountWon = payout;
        P.wonTS     = nowTs;
        _missionData.players[p1 - 1].amountWon = payout;
        _missionData.players[p1 - 1].wonTS     = nowTs;

        (bool ok, bytes memory data) = msg.sender.call{ value: payout }("");
        if (!ok)                                                    revert PayoutFailed(msg.sender, payout, data);

        emit RoundCalled(msg.sender, _missionData.roundCount, payout, _missionData.croCurrent);

        if (_missionData.roundCount == _missionData.missionRounds) {
            _setStatus(Status.Success);
            _withdrawFunds(false);
        } else {
            _setStatus(Status.Paused);
        }
    }
    // #endregion





    // #region Financial Functions
	/**
     * @dev Add funds to prize pool.
     */
	function increasePot()                                      external payable {
		require(msg.value > 0, "No funds sent");                                            // Ensure some funds are sent
        require(
            msg.sender == address(missionFactory) || missionFactory.authorized(msg.sender) || msg.sender == owner(),
            "Only factory or authorized can fund"
        );                                                                                  // Ensure the sender is the MissionFactory or an authorized address
        require(_getRealtimeStatus() < Status.Active, "Mission passed activation");         // Ensure the mission is not already active
        _missionData.croInitial     += msg.value;                                           // Increase the initial CRO amount by the value sent
		_missionData.croStart 	    += msg.value;                                           // Increase the start   CRO amount by the value sent
		_missionData.croCurrent 	+= msg.value;                                           // Increase the current CRO amount by the value sent
		emit PotIncreased(msg.value, _missionData.croCurrent);                              // Emit event for pot increase
        // notify factory for predictable polling
        missionFactory.notifyTouched();
	}

    /**
     * @dev Refunds players if the mission fails.
     * This function can be called by the owner or an authorized address.
     */
    function refundPlayers()                                    external nonReentrant onlyOwnerOrAuthorized {
        _refundPlayers();                                                                                           // Call internal refund function
    }

    /**
     * @notice Distributes remaining CRO after mission completion or failure.
     * @dev Sends:
     *      - 25% to factory owner
     *      - 75% to MissionFactory (for future missions)
     * @dev If `force = true`, also withdraws failed refund amounts.
     */
    function withdrawFunds()                                    external nonReentrant onlyOwnerOrAuthorized {
        _withdrawFunds(true);                                                                                     // Call internal withdraw function
        // notify factory for predictable polling
        missionFactory.notifyTouched();
    }

    /**
     * @notice Allows owner or authorized to finalize a mission after time expiry.
     * @dev Ends mission and withdraws remaining pot.
     */   
    function forceFinalizeMission()                             external onlyOwnerOrAuthorized nonReentrant {
        require(_getRealtimeStatus() == Status.PartlySuccess);  // Ensure mission is in PartlySuccess status

        _setStatus(Status.Success);                             
        _withdrawFunds(false);                                  // Withdraw funds to MissionFactory contract 
    }
    // #endregion





    // #region View Functions

    /**
     * @dev Returns the current number of players enrolled in the mission.
     */
    function getPlayerCount()                                   public view returns (uint256) {
        return _missionData.enrollmentCount;
    }

    /**
     * @dev Returns true if the address is a player in the mission.
     * @param addr The address to check.
     * @return A boolean indicating if the address is a player.
     */
    function isPlayer(address addr)                             public view returns (bool) {
        require(addr != address(0), "Invalid address");
        return _pIndexPlus1[addr] != 0;
    }

    /**
     * @dev Returns the player state for a given address.
     * This function checks if the player is enrolled and if they have won in any round.
     * @param player The address of the player to check.
     * @return joined A boolean indicating if the player is enrolled in the mission.
     * @return won A boolean indicating if the player has won in any round.
     */
    function playerState(address player)                        external view returns (bool joined, bool won) {
        uint8 p1 = _pIndexPlus1[player];
        if (p1 == 0) return (false, false);
        Players storage p = _players[p1 - 1];
        return (true, (p.amountWon > 0 || p.wonTS > 0));
    }

    /**
     * @dev Returns the number of seconds until the next round starts.
     * This function checks the current real-time status of the mission and calculates the time until the next round.
     * @return The number of seconds until the next round starts, or 0 if the mission is not paused.
     */
    function secondsUntilNextRound()                            external view returns (uint256) {
        if (_getRealtimeStatus() != Status.Paused) return 0;                        // If the mission is not paused, return 0
        uint256 cd = (_missionData.roundCount + 1 == _missionData.missionRounds)    // Cooldown duration
            ? _missionData.lastRoundPauseDuration                                   
            : _missionData.roundPauseDuration;
        uint256 nowTs = block.timestamp;                                            // Get the current timestamp
        return _missionData.pauseTimestamp + cd - nowTs;                            // Calculate and return the seconds until the next round starts
    }

    /**
     * @dev Returns the current progress percentage of the mission.
     * This function calculates the progress based on the elapsed time since the mission started.
     * @return The current progress percentage of the mission.
     */
    function currentProgressPct()                               external view returns (uint256){
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
    function pendingPayout(address player)                      external view returns (uint256) {
        uint256 nowTs = block.timestamp;
        Status s = _getRealtimeStatus();
        if (s != Status.Active && s != Status.Paused) return 0;

        uint8 p1 = _pIndexPlus1[player];
        if (p1 == 0) return 0;
        Players storage P = _players[p1 - 1];
        if (P.amountWon > 0 || P.wonTS > 0) return 0;

        if (nowTs <= _missionData.missionStart || nowTs >= _missionData.missionEnd) return 0;

        // Expected paid minus paid so far, identical to callRound
        uint256 progress = (nowTs - _missionData.missionStart) * 1e10
                        / (_missionData.missionEnd - _missionData.missionStart);
        uint256 paidSoFar    = _missionData.croStart - _missionData.croCurrent;
        uint256 expectedPaid = (_missionData.croStart * progress) / 1e10;
        if (expectedPaid <= paidSoFar) return 0;
        uint256 payout = expectedPaid - paidSoFar;
        if (payout > _missionData.croCurrent) payout = _missionData.croCurrent;
        return payout;
    }

    /**
     * @dev Returns the number of remaining rounds in the mission.
     * This function checks the current real-time status of the mission and returns the number of rounds left.
     * @return The number of remaining rounds in the mission, or 0 if the mission is not in Active or Paused status.
     */
    function remainingRounds()                                  external view returns (uint8) {
        Status s = _getRealtimeStatus();                                        // Get the current real-time status of the mission
        if (s == Status.Active || s == Status.Paused) {
            return _missionData.missionRounds - _missionData.roundCount;        // If the mission is Active or Paused, return remaining rounds
        }
        return 0;                                                               // If the mission is not in Active or Paused status, return 0 remaining rounds
    }

    /**
     * @dev Returns the MissionData structure.
     */
    function getMissionData()                                   external view returns (MissionData memory) {
            MissionData memory m = _missionData;    // Copy full struct from storage to memory (cheap and compact)

            m.status = _getRealtimeStatus();        // Patch in real-time status on the memory copy (no storage write)

            return m;
    }

    /**
     * @dev Returns the current real-time status of the mission.
     * This function checks the current time and mission data to determine the status.
     * @return The current status of the mission.
     */
    function getRealtimeStatus()                                external view returns (Status) {
        return _getRealtimeStatus();
    }

    /**
     * @dev Returns whether the mission is in the arming phase.
     * This function checks if the current time is between the enrollment end and mission start times.
     * @return A boolean indicating if the mission is in the arming phase.
     */
    function isArming()                                         public view returns (bool) {
        uint256 nowTs = block.timestamp;
        return (nowTs > _missionData.enrollmentEnd && nowTs < _missionData.missionStart);
    }

    /**
     * @dev Returns whether the mission is finalized by realtime status, 
            not the status set in the factory which can lag behind.
     * This function checks if the mission is in Success or Failed status.
     * @return A boolean indicating if the mission is finalized.
     */ 
    function isFinalized()                                      public view returns (bool) {
        Status s = _getRealtimeStatus();
        return (s == Status.Success || s == Status.Failed);
    }

    /**
     * @dev Returns the addresses of players who have failed refunds.
     * This function iterates through all players and collects those with failed refund amounts.
     * @return An array of player addresses who have failed refunds.
     */
    function getFailedRefundPlayers()                           external view returns (address[] memory) {
        require(_getRealtimeStatus() == Status.Failed, "Mission is not in Failed status");
        uint256 count;
        for (uint256 i = 0; i < _missionData.players.length; i++) {
            if (_missionData.players[i].refundFailed) count++;
        }
        address[] memory failed = new address[](count);
        uint256 k;
        for (uint256 i = 0; i < _missionData.players.length; i++) {
            if (_missionData.players[i].refundFailed) {
                failed[k++] = _missionData.players[i].player;
            }
        }
        return failed;
    }

    /**
     * @dev Checks if a player has been refunded.
     * This function iterates through the refundedPlayers array to check if the address is present.
     * @param addr The address of the player to check for refund status.
     * @return A boolean indicating if the player has been refunded.
     */ 
    function wasRefunded(address addr)                          public view returns (bool) {
        require(_getRealtimeStatus() == Status.Failed, "Mission is not in Failed status");
        require(addr != address(0), "Invalid address");
        uint8 p1 = _pIndexPlus1[addr];
        require(p1 != 0, "Player not enrolled");
        return _players[p1 - 1].refunded;
    }

    /**
    * @dev Returns the unified player records for winners (those who have wonTS > 0 or amountWon > 0).
    *      Uses the on-chain unified Players[] model.
    */
    function getWinners()                                       external view returns (Players[] memory) {
        require(
            _getRealtimeStatus() == Status.Success || _getRealtimeStatus() == Status.PartlySuccess,
            "Mission is not in Success or PartlySuccess status"
        );

        // Count winners
        uint256 count;
        for (uint256 i = 0; i < _missionData.players.length; i++) {
            Players memory p = _missionData.players[i];
            if (p.wonTS > 0 || p.amountWon > 0) {
                count++;
            }
        }

        // Collect winners
        Players[] memory winners = new Players[](count);
        uint256 k;
        for (uint256 i = 0; i < _missionData.players.length; i++) {
            Players memory p = _missionData.players[i];
            if (p.wonTS > 0 || p.amountWon > 0) {
                winners[k++] = p;
            }
        }

        return winners;
    }

    /// @notice Lightweight roll-up for indexer reconciliation (rarely used)
    function getIndexerSnapshot()                               external view returns (uint8 status, uint8 roundCount, uint256 croCurrent, uint32 playersCount, uint32 winnersCount, uint32 refundedCount) {
        Status s = _getRealtimeStatus();

        uint32 winners;
        uint32 refundedN;
        uint256 len = _missionData.players.length;
        for (uint256 i = 0; i < len; i++) {
            Players memory p = _missionData.players[i];
            if (p.amountWon > 0 || p.wonTS > 0) winners++;
            if (p.refunded) refundedN++;
        }

        return (
            uint8(s),
            _missionData.roundCount,
            _missionData.croCurrent,
            uint32(_missionData.enrollmentCount),
            winners,
            refundedN
        );
    }

    /// @notice Return a window of refunded players to avoid huge arrays in one call
    function getRefundedPlayersSlice(uint256 offset, uint256 limit) external view returns (address[] memory slice) {
        // Build a compact list of refunded addresses, then slice
        uint256 len = _missionData.players.length;
        if (len == 0 || limit == 0 || offset >= len) return new address[](0);

        // Count refunded
        uint256 count;
        for (uint256 i = 0; i < len; i++) {
            if (_missionData.players[i].refunded) count++;
        }
        if (count == 0) return new address[](0);

        address[] memory all = new address[](count);
        uint256 k;
        for (uint256 i = 0; i < len; i++) {
            if (_missionData.players[i].refunded) {
                all[k++] = _missionData.players[i].player;
            }
        }

        if (offset >= all.length) return new address[](0);
        uint256 to = offset + limit;
        if (to > all.length) to = all.length;
        uint256 n = to - offset;

        slice = new address[](n);
        for (uint256 i = 0; i < n; i++) {
            slice[i] = all[offset + i];
        }
    }

    // #endregion




    // #region Internal Helpers
    /**
     * @dev Returns the current status of the mission based on the current time and mission data.
     * This function checks various conditions to determine the real-time status of the mission.
     * @return status The current status of the mission.
     */ 
    function _getRealtimeStatus()                               internal view returns (Status status) {

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
                    ? _missionData.lastRoundPauseDuration
                    : _missionData.roundPauseDuration))
            {
                return Status.Paused;
            }
            else {
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
    function _setStatus(Status newStatus)                       internal {
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
    * @dev If `force = true`, distributes full balance. If `force = false`, keeps aside
    *      the amount needed to cover players whose refunds failed.
    */
    function _withdrawFunds(bool force)                         internal {
        require(_getRealtimeStatus() == Status.Success || _getRealtimeStatus() == Status.Failed);   // Ensure mission is ended
        uint256 balance = address(this).balance;
        require(balance > 0,                                "No funds to withdraw");                // Ensure there are funds to withdraw

        if (_missionData.enrollmentCount == 0) {                                                     
            _setStatus(Status.Failed);                                                              // If no players, set status to Failed
        }

        uint256 distributable;
        if (force) {
            distributable = balance;                                                                // Force: distribute everything
        } else {
            // Compute unclaimable based on unified player model: count refundFailed
            uint256 failedCount;
            for (uint256 i = 0; i < _missionData.players.length; i++) {
                if (_missionData.players[i].refundFailed) {
                    unchecked { failedCount++; }
                }
            }
            uint256 unclaimable = failedCount * _missionData.enrollmentAmount;                      // Reserve for retriable refunds
            if (unclaimable > balance) unclaimable = balance;                                       // Safety clamp
            distributable = balance - unclaimable;
        }

        require(distributable > 0,                          "No funds to withdraw");          // Ensure there are funds to withdraw after deductions

        uint256 _ownerShare;
        uint256 _factoryShare;

        // For InviteOnly missions, send 100% of leftover to the owner; nothing flows back to the factory.
        if (_missionData.missionType == MissionType.InviteOnly) {
            _ownerShare   = distributable;
            _factoryShare = 0;

            (bool okInvite, ) = payable(missionFactory.owner()).call{value: _ownerShare}("");
            require(okInvite,                                "Owner payout failed");

            emit FundsWithdrawn(_ownerShare, _factoryShare);
            ownerShare   = _ownerShare;
            factoryShare = _factoryShare;
            return;
        } else if (_missionData.missionType == MissionType.UserMission) {
            // For PublicWithCreator missions, send 50% to the mission creator and 50% to the factory owner;
            _ownerShare   = (distributable * 50) / 100;
            uint256 creatorShare = distributable - _ownerShare;
            _factoryShare = 0;

            (bool okOwner, ) = payable(missionFactory.owner()).call{value: _ownerShare}("");
            require(okOwner,                                 "Owner payout failed");

            (bool okCreator, ) = payable(_missionData.creator).call{value: creatorShare}("");
            require(okCreator,                               "Creator payout failed");

            emit FundsWithdrawn(_ownerShare + creatorShare, _factoryShare);
            ownerShare   = _ownerShare + creatorShare;
            factoryShare = _factoryShare;
            return;
        }

        // Default behavior for other mission types: 25% owner / 75% factory
        _ownerShare   = (distributable * 25) / 100;
        _factoryShare = distributable - _ownerShare;

        (bool ok, ) = payable(missionFactory.owner()).call{value: _ownerShare}("");
        require(ok,                                         "Owner payout failed");

        missionFactory.registerMissionFunds{ value: _factoryShare }(
            _missionData.missionType
        );

        emit FundsWithdrawn(_ownerShare, _factoryShare);
        ownerShare   = _ownerShare;
        factoryShare = _factoryShare;

        _missionData.croCurrent = address(this).balance;                                            // Current balance after distribution
    }

    /**
     * @dev Refunds players if the mission fails.
     * This function is internal and can only be called when the mission is in Failed status.
     * It ensures that the mission has ended and that the enrollment period has passed.
     * It refunds all enrolled players their enrollment amount.
     */
    function _refundPlayers()                                   internal {
        require(_getRealtimeStatus() == Status.Failed, "Mission not in Failed status");
        require(_missionData.enrollmentCount > 0,      "No players to refund");

        bool forceAll = true;
        uint256 nowTs = block.timestamp;

        for (uint256 i = 0; i < _players.length; i++) {
            Players storage P = _players[i];

            // Retry any player who is not yet refunded (even if a previous attempt failed)
            if (!P.refunded) {
                (bool ok, ) = payable(P.player).call{ value: _missionData.enrollmentAmount }("");
                if (ok) {
                    // Success: mark refunded and CLEAR refundFailed (in case a prior attempt failed)
                    P.refunded     = true;
                    P.refundFailed = false;
                    P.refundTS     = nowTs;

                    // Mirror to MissionData.players (same index)
                    _missionData.players[i].refunded     = true;
                    _missionData.players[i].refundFailed = false;
                    _missionData.players[i].refundTS     = nowTs;

                    emit PlayerRefunded(P.player, _missionData.enrollmentAmount);

                    missionFactory.undoEnrollmentInWindow(
                        P.player,
                        _missionData.enrollmentStart,
                        _missionData.enrollmentEnd
                    );
                } else {
                    // Failure: set/keep failed flag
                    P.refundFailed = true;
                    P.refundTS     = nowTs;
                    _missionData.players[i].refundFailed = true;
                    _missionData.players[i].refundTS     = nowTs;

                    emit RefundFailed(P.player, _missionData.enrollmentAmount);
                    forceAll = false;
                }
            }
        }

        // Update allRefunded flag (true only if every enrolled player is refunded and none failed)
        bool allOk = true;
        for (uint256 i = 0; i < _players.length; i++) {
            if (!_players[i].refunded)        { allOk = false; break; }
            if (_players[i].refundFailed)     { allOk = false; break; }
        }
        _missionData.allRefunded = allOk;

        _setStatus(Status.Failed);

        if (address(this).balance > 0) {
            _withdrawFunds(forceAll);
        }

        // Build refunded list snapshot (addresses) for the event payload
        uint256 count;
        for (uint256 i = 0; i < _players.length; i++) if (_players[i].refunded) count++;
        address[] memory refundedAddrs = new address[](count);
        uint256 k;
        for (uint256 i = 0; i < _players.length; i++) if (_players[i].refunded) refundedAddrs[k++] = _players[i].player;

        emit MissionRefunded(
            count,
            _missionData.enrollmentAmount,
            refundedAddrs,
            block.timestamp
        );
    }

    // #endregion

    // #endregion
}

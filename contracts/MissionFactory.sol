// SPDX-License-Identifier: MIT
pragma solidity ^0.8.30;

/**
 * @title Be Brave Be Bold Be Banked (B6) Mission Game Smart Contract
 * @notice This contract represents a single "Mission" in the game system deployed via MissionFactory.
 *
 * ## 📖 Overview
 * A Mission is a time-based competitive game where players enroll by paying a fixed ETH amount
 * during the enrollment period. Once the mission starts, players compete in multiple rounds
 * to win portions of the prize pool. The game ends when all rounds are claimed or when
 * the mission duration expires.
 *
 * ## 🎮 Game Rules
 *
 * 1. **Enrollment Phase**
 *    - Starts at `enrollmentStart` and ends at `enrollmentEnd`.
 *    - Players pay `enrollmentAmount` to enroll.
 *    - Each address can enroll only once.
 *    - Each player is checked if they can enroll based on anti-addiction limits:
 *        - Weekly limit:  `weeklyLimit`  missions.
 *        - Monthly limit: `monthlyLimit` missions.
 *    - Enrollment succeeds only if the enrollment window is open and max players not reached.
 *
 * 2. **Start Conditions**
 *    - After `enrollmentEnd`, mission can only start if `enrollmentMinPlayers` is met.
 *    - If conditions fail, mission is marked as `Failed` and refunds are processed.
 *
 * 3. **Mission Phases**
 *    - **Active Phase**: Mission starts at `missionStart` and runs until `missionEnd`.
 *    - Mission consists of `missionRounds` rounds.
 *    - Each round can only be called after a cooldown:
 *        - **Normal rounds**: 5-minute pause after each round.
 *        - **Final round**: 1-minute pause before last round.
 *    - A player can only win **once per mission**.
 *
 * 4. **Round Payouts**
 *    - Prize pool starts as `ethStart` (initial ETH funding).
 *    - At each round, payout = (progress since last round) * `ethStart` / 100.
 *    - Progress = % of total mission duration elapsed since last claim.
 *    - Example:
 *        - If 10% of time passed since last claim, 10% of `ethStart` is paid out.
 *    - Remaining ETH after final round is swept during withdrawal.
 *
 * 5. **Mission End**
 *    - Mission ends when:
 *        - All rounds claimed OR
 *        - `missionEnd` timestamp reached.
 *    - If time expires with incomplete rounds:
 *        - Owner/authorized can call `forceFinalizeMission()`:
 *            - If some rounds called → Remaining ETH is swept during withdrawal.
 *            - If no rounds called → Mission is marked as `Failed`, all players refunded.
 *
 * 6. **Fees**
 *    - After mission completion (or failure):
 *        - 25% of remaining ETH → factory owner.
 *        - 75% → MissionFactory for future missions (reservedFunds).
 *    - Payout is processed in `_withdrawFunds()`.
 *
 * 7. **Refund Logic**
 *    - If mission fails (not enough players), all enrolled players get refunded.
 *    - If a refund fails (e.g., non-payable address), the amount is tracked in `failedRefundAmounts`.
 *    - Failed refunds are excluded from normal withdrawals unless `force = true` in `_withdrawFunds()`.
 *
 * ## ⚠️ Key Constraints
 * - `missionRounds` must be >= enrollmentMinPlayers.
 * - A player can only win once per mission.
 *
 * ## 🛠 Admin Functions
 * - Owner or authorized can:
 *    - Force finalize the mission after time expiry.
 *    - Withdraw leftover funds after mission end/failure.
 *
 * ## ✅ Security
 * - Uses OpenZeppelin ReentrancyGuard for state-changing functions.
 * - ETH transfers use `.call{value: ...}` to prevent gas griefing.
 * - Refund failures are logged and tracked for later withdrawal.
 *
 * @dev This contract is deployed as a clone (minimal proxy) by MissionFactory.
 */

// ───────────────────── Imports ────────────────────────
import "@openzeppelin/contracts/access/Ownable.sol";
import "@openzeppelin/contracts/proxy/Clones.sol";
import "@openzeppelin/contracts/utils/ReentrancyGuard.sol";

// ─────────────────── Global Enums ─────────────────────
/**
 * @dev Enum to represent the type of mission.
 * The mission can be one of several types: Custom, Hourly, QuarterDaily, BiDaily, Daily, Weekly, or Monthly.
 */
enum MissionType {
    Custom,
    Hourly,
    QuarterDaily,
    BiDaily,
    Daily,
    Weekly,
    Monthly
}

/**
 * @dev Enum to represent the status of a mission.
 * The mission can be in one of several states: Pending, Enrolling, Active, Paused, Ended, or Failed.
 */
enum Status {
    Pending,
    Enrolling,
    Active,
    Paused,
    Ended,
    Failed
}

// ────────────── Contract MissionFactory────────────────
/** 
 * @title   MissionFactory
 * @author  Dennis Bakker
 * @notice  Factory contract for creating and managing missions.
 *          It allows authorized addresses to create missions, manage funds, and track mission statuses.
 * @dev     Uses OpenZeppelin's Ownable and ReentrancyGuard for security and ownership management.
 */
contract MissionFactory is Ownable, ReentrancyGuard {
    using Clones    for address;
    
    // ────────────────── Events ───────────────────────
    /** 
     * @dev Events emitted by the MissionFactory contract.
     * These events are used to log important actions and state changes within the contract.
     */
    event AuthorizedAddressAdded                (address        indexed addr                                                                        );
    event AuthorizedAddressRemoved              (address        indexed addr                                                                        );
    event MissionCreated                        (address        indexed missionAddress, MissionType indexed missionType                             );
    event FundsReceived                         (address        indexed sender,         uint256             amount                                  );
    event MissionFundsRegistered                (uint256                amount,         MissionType indexed missionType,    address indexed sender  );
    event FundsWithdrawn                        (address        indexed to,             uint256             amount                                  );    
    event OwnershipTransferProposed             (address        indexed proposer,       address             newOwner,       uint256 timestamp       );
    event OwnershipTransferConfirmed            (address        indexed confirmer,      address             newOwner,       uint256 timestamp       );
    event EnrollmentLimitUpdated                (uint8                  newWeekly,      uint8               newMonthly                              );
    event EnrollmentRecorded                    (address        indexed user,           uint256             timestamp                               );

    // ────────────────── Modifiers ─────────────────────
    /**
     * @dev Modifier that allows only the owner or an authorized address to call.
     */
    modifier onlyOwnerOrAuthorized() {
        require(
            msg.sender == owner() || authorized[msg.sender],
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
			missionStatus[msg.sender] != Status(0), 
			"MissionFactory: caller is not a valid mission contract");
		_;
	}

    // ────────────── State Variables ───────────────────
    /**
     * @dev State variables for the MissionFactory contract.
     * These variables store the state of the contract, including authorized addresses, reserved funds, mission statuses, and the implementation address for missions.
     */
    mapping(address => bool)                public  authorized;             // Mapping to track authorized addresses
    mapping(MissionType => uint256)         public  reservedFunds;          // Track funds by type
    mapping(address => Status)              public  missionStatus;          // Mapping to hold the status of each mission
    address[]                               public  missions;               // Array to hold all mission addresses
    address                                 public  missionImplementation;  // Address of the Mission implementation contract for creating new missions
    mapping(address => OwnershipProposal)   public  ownershipProposals;     // Mapping to hold ownership proposals
    uint256                                 public constant OWNERSHIP_PROPOSAL_WINDOW = 1 days; // Duration for ownership proposal validity
    mapping(address => uint256[])           private enrollmentHistory;      // store timestamps
    uint8                                   public  weeklyLimit = 4;        // Maximum number of missions a player can enroll in per week
    uint8                                   public  monthlyLimit = 10;      // Maximum number of missions a player can enroll in per month

    // ─────────────────── Structs ──────────────────────
    /**
     * @dev Struct to hold information about ownership proposals.
     * Contains the address of the proposer and the timestamp of the proposal.
     */
    struct OwnershipProposal {
        address proposer;
        uint256 timestamp;
    }

    // ──────────────── Constructor ─────────────────────
    /**
     * @dev Struct to hold information about players who won the mission.
     * Contains the player's address and the amount they won.
     */
    constructor() Ownable(msg.sender) {
		Mission impl = new Mission();
        missionImplementation = address(impl);
	}

    // ──────────── Anti-addiction Functions ────────────
    /**
     * @dev Sets the weekly and monthly enrollment limits.
     * This function allows the owner or an authorized address to set the limits for how many missions a user can enroll in per week and per month.
     * @param _weekly The new weekly limit for mission enrollments.
     * @param _monthly The new monthly limit for mission enrollments.
     */
    function setEnrollmentLimits(uint8 _weekly, uint8 _monthly) external onlyOwnerOrAuthorized {
        weeklyLimit = _weekly;
        monthlyLimit = _monthly;
        emit EnrollmentLimitUpdated(_weekly, _monthly);
    }

    /**
     * @dev Records the enrollment of a user in a mission.
     * This function is called when a user enrolls in a mission.
     * It updates the user's enrollment history and emits an event.
     * @param user The address of the user enrolling in the mission.
     */
    function canEnroll(address user) public view returns (bool allowed, string memory reason) {
        uint256 nowTs = block.timestamp;
        uint256 weeklyCount;
        uint256 monthlyCount;

        uint256[] memory history = enrollmentHistory[user];
        for (uint256 i = 0; i < history.length; i++) {
            if (history[i] + 30 days > nowTs) {
                monthlyCount++;
                if (history[i] + 7 days > nowTs) {
                    weeklyCount++;
                }
            }
        }

        if (weeklyCount >= weeklyLimit) {
            return (false, "AntiAddiction: Weekly limit reached");
        }
        if (monthlyCount >= monthlyLimit) {
            return (false, "AntiAddiction: Monthly limit reached");
        }
        return (true, "");
    }

    /**
     * @dev Records the enrollment of a user in a mission.
     * This function is called when a user enrolls in a mission.
     * It updates the user's enrollment history and emits an event.
     * @param user The address of the user enrolling in the mission.
     */
    function recordEnrollment(address user) external {
        require(missionStatus[msg.sender] == Status.Enrolling || missionStatus[msg.sender] == Status.Active, "Invalid caller");
        (bool allowed, string memory reason) = canEnroll(user);
        require(allowed, reason);

        // Prune old entries (>30 days)
        uint256 cutoff = block.timestamp - 30 days;
        uint256[] storage history = enrollmentHistory[user];
        uint256 i = 0;
        while (i < history.length && history[i] < cutoff) {
            i++;
        }
        if (i > 0) {
            for (uint256 j = 0; j < history.length - i; j++) {
                history[j] = history[j + i];
            }
            for (uint256 k = 0; k < i; k++) {
                history.pop();
            }
        }

        history.push(block.timestamp);
        emit EnrollmentRecorded(user, block.timestamp);
    }

    // ──────────────── Admin Functions ─────────────────
    /**
     * @dev Adds an address to the list of authorized addresses.
     * @param account The address to authorize.
     */
    function addAuthorizedAddress(address account) external onlyOwnerOrAuthorized {
        require(account != address(0),  "Invalid address");                         // Ensure the account is valid
        require(!authorized[account],   "Already authorized");                      // Ensure the account is not already authorized
        authorized[account] = true;                                                 // Add authorization for the account  
        emit AuthorizedAddressAdded(account);                                       // Emit event for addition of authorization
    }

    /**
     * @dev Removes authorization for an address.
     * @param account The address to remove authorization from.
     */
    function removeAuthorizedAddress(address account) external onlyOwnerOrAuthorized {
        require(account != address(0),  "Invalid address");                         // Ensure the account is valid
        require(authorized[account],    "Not authorized");                          // Ensure the account is currently authorized
        authorized[account] = false;                                                // Remove authorization for the account
        emit AuthorizedAddressRemoved(account);                                     // Emit event for removal of authorization
    }

    /**
     * @dev Proposes a transfer of ownership to a new address.
     * @param newOwner The address of the new owner.
     */
    function proposeOwnershipTransfer(address newOwner) external onlyOwnerOrAuthorized {
        require(newOwner != address(0), "Invalid new owner");
        ownershipProposals[newOwner] = OwnershipProposal({
            proposer: msg.sender,
            timestamp: block.timestamp
        });
        emit OwnershipTransferProposed(msg.sender, newOwner, block.timestamp);
    }

    /**
     * @dev Confirms the ownership transfer to a new address.
     * @param newOwner The address of the new owner.
     */
    function confirmOwnershipTransfer(address newOwner) external onlyOwnerOrAuthorized {
        OwnershipProposal memory proposal = ownershipProposals[newOwner];
        require(proposal.proposer != address(0), "No proposal for this owner");
        require(proposal.proposer != msg.sender, "Cannot confirm your own proposal");
        require(block.timestamp <= proposal.timestamp + OWNERSHIP_PROPOSAL_WINDOW, "Proposal expired");

        // Transfer ownership
        super.transferOwnership(newOwner);

        // Cleanup
        delete ownershipProposals[newOwner];

        emit OwnershipTransferConfirmed(msg.sender, newOwner, block.timestamp);
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
    function createMission(
        MissionType     _missionType,           // Type of the mission
        uint256         _enrollmentStart,       // Start time for enrollment
        uint256         _enrollmentEnd,         // End time for enrollment
        uint256         _enrollmentAmount,      // Amount required for enrollment
        uint8           _enrollmentMinPlayers,  // Minimum number of players required to start the mission
        uint8           _enrollmentMaxPlayers,  // Maximum number of players required to start the mission
        uint256         _missionStart,          // Start time for the mission
        uint256         _missionEnd,            // End time for the mission
        uint8           _missionRounds          // Number of rounds in the mission
        ) external payable onlyOwnerOrAuthorized returns (address) {
            require(_missionRounds          >= 5,               "Mission rounds must be greater than or equal to 5");               // Ensure mission rounds is greater than or equal to 5
            require(_enrollmentMinPlayers   >= _missionRounds,  "Minimum players must be greater than or equal to mission rounds"); // Ensure minimum players is at least equal to mission rounds
            require(_enrollmentStart        <  _enrollmentEnd,  "Enrollment start must be before end");                             // Ensure enrollment start is before end
            require(_missionStart           >= _enrollmentEnd,  "Mission start must be on or after enrollment end");                // Ensure mission start is on or after enrollment end
            require(_missionEnd             >  _missionStart,   "Mission start must be before end");                                // Ensure mission start is before end
            require(_enrollmentAmount       >  0,               "Enrollment amount must be greater than zero");                     // Ensure enrollment amount is greater than zero

			address clone = missionImplementation.clone(); 	    // EIP-1167 minimal proxy

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
                _missionRounds                                  // Set the number of rounds in the mission
            );

        missions.push(clone);                                   // Add the new mission to the list of missions
        emit MissionCreated(clone, _missionType);               // Emit event for mission creation

        // Calculate allocation based on mission type
        uint256 allocation = reservedFunds[_missionType] / 4;   // Missions get 1/4th of the reserved funds

        if (allocation > 0 && address(this).balance >= allocation) {
            reservedFunds[_missionType] -= allocation;
            Mission(payable(clone)).increasePot{value: allocation}();   // Sends ETH and updates mission accounting
        }

        return clone;						                            // Return the address of the newly created mission
    }

    /**
     * @dev Sets the status of a mission.
     * @param newStatus The new status to set for the mission.
     */
    function setMissionStatus(Status newStatus) external onlyMission {
		missionStatus[msg.sender] = newStatus;                                                    	// Update the mission status
    } 

    // ──────────────── Financial Functions ─────────────
    /**
     * @dev Registers mission funds for a specific mission type.
     * @param amount The amount of funds to register.
     * @param missionType The type of the mission.
     */
    function registerMissionFunds(uint256 amount, MissionType missionType) external {
        require(amount > 0,                                                                                 "Amount must be greater than zero");    // Ensure the amount is greater than zero
        require(missionStatus[msg.sender] == Status.Ended || missionStatus[msg.sender] == Status.Failed,    "Caller is not a valid mission");       // Ensure the caller is a valid mission
        reservedFunds[missionType] += amount;
        emit MissionFundsRegistered(amount, missionType, msg.sender);
    }

    /**
     * @dev Receives funds sent to the contract.
     * This function is called when the contract receives ETH without any data.
     * It allows the contract to accept ETH transfers.
     */
    receive() external payable {}

    /**
     * @dev Fallback function to receive ETH.
     * This function is called when the contract receives ETH without any data.
     * It allows the contract to accept ETH transfers.
     */
    fallback() external payable {}

    /**
     * @dev Withdraws funds from the MissionFactory contract.
     * This function allows the owner or an authorized address to withdraw funds from the contract.
     * If no amount is specified, it withdraws all available funds.
     * @param amount The amount of funds to withdraw. If 0, withdraws all available funds.
     */
    function withdrawFunds(uint256 amount) external onlyOwnerOrAuthorized nonReentrant {
        address mgrOwner = owner();                                                             // Get the owner of the MissionFactory contract
        require(mgrOwner != address(0), "Invalid manager owner");                               // Ensure the manager owner is valid
        if (amount == 0) {
            amount = address(this).balance;                                                     // If no amount specified, withdraw all funds
        }
        require(amount <= address(this).balance, "Insufficient balance");                       // Ensure the contract has enough balance to withdraw
        (bool ok, ) = payable(mgrOwner).call{ value: amount }("");                              // Attempt to transfer the specified amount to the manager owner
        require(ok, "Transfer failed");                                                         // Ensure the transfer was successful
        emit FundsWithdrawn(mgrOwner, amount);                                                  // Emit event for funds withdrawal
    }

    // ──────────────── View Functions ────────────────
    /**
     * @dev Returns the status of a mission.
     * @param missionAddress The address of the mission to check.
     * @return mission data of the mission.
     */
    function getMissionData(address missionAddress) external view returns (Mission.MissionData memory) {
        require(missionAddress != address(0), "Invalid mission address");          // Ensure mission address is valid
        return Mission(payable(missionAddress)).getMissionData();                           // Return the mission data from the Mission contract
    }

    /**
     * @dev Returns the addresses and statuses of all missions.
     * This function filters out missions that have ended or failed more than 30 days ago.
     * @return An array of mission addresses and their corresponding statuses.
     */
    function getAllMissions() external view returns (address[] memory, Status[] memory) {
        uint256 len    = missions.length;
        uint256 cutoff = block.timestamp - 30 days;
        uint256 count;

        // First pass: count how many missions to include
        for (uint256 i = 0; i < len; i++) {
            address m = missions[i];
            Status  s = missionStatus[m];

            if (s == Status.Ended || s == Status.Failed) {
                Mission.MissionData memory md = Mission(payable(m)).getMissionData();
                if (md.missionEnd < cutoff) {
                    continue;  // drop if it ended/failed more than 30 days ago
                }
            }
            count++;
        }

        // Second pass: collect the addresses & statuses
        address[] memory outAddrs   = new address[](count);
        Status[]  memory outStatus  = new Status[](count);
        uint256 idx;

        for (uint256 i = 0; i < len; i++) {
            address m = missions[i];
            Status  s = missionStatus[m];

            if (s == Status.Ended || s == Status.Failed) {
                Mission.MissionData memory md = Mission(payable(m)).getMissionData();
                if (md.missionEnd < cutoff) {
                    continue;
                }
            }

            outAddrs[idx]  = m;
            outStatus[idx] = s;
            idx++;
        }

        return (outAddrs, outStatus);
    }

    /**
     * @dev Returns the addresses of missions filtered by status.
     * @param filter The status to filter missions by.
     * @return An array of mission addresses that match the specified status.
     */
    function getMissionsByStatus(Status filter) external view returns (address[] memory) {
        uint256 len = missions.length;
        uint256 count;
 
        // First pass: count matches
        for (uint256 i = 0; i < len; i++) {
            if ((filter == Status.Active && missionStatus[missions[i]] == Status.Paused) || missionStatus[missions[i]] == filter) {
                count++;
            }
        }

        // Second pass: populate result array
        address[] memory filtered = new address[](count);
        uint256 index;
        for (uint256 i = 0; i < len; i++) {
            if ((filter == Status.Active && missionStatus[missions[i]] == Status.Paused) || missionStatus[missions[i]] == filter) {
                filtered[index++] = missions[i];
            }
        }

        return filtered;
    }

    /**
     * @dev Returns the addresses of missions that have not ended.
     * This function filters out missions that are in the Ended or Failed status.
     * @return An array of mission addresses that have not ended.
     */
    function getMissionsNotEnded() external view returns (address[] memory) {
        uint256 len = missions.length;
        uint256 count;

        // First pass: count not ended missions
        for (uint256 i = 0; i < len; i++) {
            if (missionStatus[missions[i]] != Status.Ended && missionStatus[missions[i]] != Status.Failed) {
                count++;
            }
        }

        // Second pass: populate result array
        address[] memory notEndedMissions = new address[](count);
        uint256 index;
        for (uint256 i = 0; i < len; i++) {
            if (missionStatus[missions[i]] != Status.Ended && missionStatus[missions[i]] != Status.Failed) {
                notEndedMissions[index++] = missions[i];
            }
        }

        return notEndedMissions;
    }

    /**
     * @dev Returns the addresses of missions that have ended.
     * This function filters out missions that are in the Ended or Failed status.
     * @return An array of mission addresses that have ended.
     */
    function getMissionsEnded() external view returns (address[] memory) {
        uint256 len = missions.length;
        uint256 count;

        // First pass: count ended missions
        for (uint256 i = 0; i < len; i++) {
            if (missionStatus[missions[i]] == Status.Ended || missionStatus[missions[i]] == Status.Failed) {
                count++;
            }
        }

        // Second pass: populate result array
        address[] memory endedMissions = new address[](count);
        uint256 index;
        for (uint256 i = 0; i < len; i++) {
            if (missionStatus[missions[i]] == Status.Ended || missionStatus[missions[i]] == Status.Failed) {
                endedMissions[index++] = missions[i];
            }
        }

        return endedMissions;
    }

    /**
     * @dev Returns the reserved funds for a specific mission type.
     * @param _type The type of the mission to check.
     * @return The amount of reserved funds for the specified mission type.
     */
    function getFundsByType(MissionType _type) external view returns (uint256) {
        return reservedFunds[_type];                                                // Return the reserved funds for the specified mission type
    }

}

// ───────────────── Contract Mission ───────────────────
/**
 * @title Be Brave Be Bold Be Banked (B6) Mission Game Smart Contract
 * @notice This contract represents a single "Mission" in the game system deployed via MissionFactory.
 *
 * ## 📖 Overview
 * A Mission is a time-based competitive game where players enroll by paying a fixed ETH amount
 * during the enrollment period. Once the mission starts, players compete in multiple rounds
 * to win portions of the prize pool. The game ends when all rounds are claimed or when
 * the mission duration expires.
 *
 * ## 🎮 Game Rules
 *
 * 1. **Enrollment Phase**
 *    - Starts at `enrollmentStart` and ends at `enrollmentEnd`.
 *    - Players pay `enrollmentAmount` to enroll.
 *    - Each address can enroll only once.
 *    - Each player is checked if they can enroll based on anti-addiction limits:
 *        - Weekly limit:  `weeklyLimit`  missions.
 *        - Monthly limit: `monthlyLimit` missions.
 *    - Enrollment succeeds only if the enrollment window is open and max players not reached.
 *
 * 2. **Start Conditions**
 *    - After `enrollmentEnd`, mission can only start if `enrollmentMinPlayers` is met.
 *    - If conditions fail, mission is marked as `Failed` and refunds are processed.
 *
 * 3. **Mission Phases**
 *    - **Active Phase**: Mission starts at `missionStart` and runs until `missionEnd`.
 *    - Mission consists of `missionRounds` rounds.
 *    - Each round can only be called after a cooldown:
 *        - **Normal rounds**: 5-minute pause after each round.
 *        - **Final round**: 1-minute pause before last round.
 *    - A player can only win **once per mission**.
 *
 * 4. **Round Payouts**
 *    - Prize pool starts as `ethStart` (initial ETH funding).
 *    - At each round, payout = (progress since last round) * `ethStart` / 100.
 *    - Progress = % of total mission duration elapsed since last claim.
 *    - Example:
 *        - If 10% of time passed since last claim, 10% of `ethStart` is paid out.
 *    - Remaining ETH after final round is swept during withdrawal.
 *
 * 5. **Mission End**
 *    - Mission ends when:
 *        - All rounds claimed OR
 *        - `missionEnd` timestamp reached.
 *    - If time expires with incomplete rounds:
 *        - Owner/authorized can call `forceFinalizeMission()`:
 *            - If some rounds called → Remaining ETH is swept during withdrawal.
 *            - If no rounds called → Mission is marked as `Failed`, all players refunded.
 *
 * 6. **Fees**
 *    - After mission completion (or failure):
 *        - 25% of remaining ETH → factory owner.
 *        - 75% → MissionFactory for future missions (reservedFunds).
 *    - Payout is processed in `_withdrawFunds()`.
 *
 * 7. **Refund Logic**
 *    - If mission fails (not enough players), all enrolled players get refunded.
 *    - If a refund fails (e.g., non-payable address), the amount is tracked in `failedRefundAmounts`.
 *    - Failed refunds are excluded from normal withdrawals unless `force = true` in `_withdrawFunds()`.
 *
 * ## ⚠️ Key Constraints
 * - `missionRounds` must be >= enrollmentMinPlayers.
 * - A player can only win once per mission.
 *
 * ## 🛠 Admin Functions
 * - Owner or authorized can:
 *    - Force finalize the mission after time expiry.
 *    - Withdraw leftover funds after mission end/failure.
 *
 * ## ✅ Security
 * - Uses OpenZeppelin ReentrancyGuard for state-changing functions.
 * - ETH transfers use `.call{value: ...}` to prevent gas griefing.
 * - Refund failures are logged and tracked for later withdrawal.
 *
 * @dev This contract is deployed as a clone (minimal proxy) by MissionFactory.
 */

contract Mission is Ownable, ReentrancyGuard {
    
    // ───────────────────── Events ─────────────────────
    event MissionStatusChanged  (Status     indexed previousStatus, Status      indexed newStatus,      uint256 timestamp                   );
    event PlayerEnrolled        (address    indexed player,         uint256             amount,         uint256 totalPlayers                );
    event MissionArmed          (uint256            timestamp,      uint256             totalPlayers                                        );
    event RoundCalled           (address    indexed player,         uint8       indexed roundNumber,    uint256 payout, uint256 ethRemaining);
    event PlayerRefunded        (address    indexed player,         uint256             amount                                              );
    event FundsWithdrawn        (uint256            ownerAmount,    uint256             factoryAmount                                       );
    event MissionPaused         (uint256            timestamp                                                                               );
    event MissionResumed        (uint256            timestamp                                                                               );
    event RefundFailed          (address    indexed player,         uint256             amount                                              ); 
    event MissionInitialized    (address    indexed owner,          MissionType indexed missionType,    uint256 timestamp                   );
	event PotIncreased			(uint256			value,			uint256				ethCurrent											);

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

    // ──────────────── State Variables ─────────────────
    /**
     * @dev Reference to the MissionFactory contract.
     * This contract manages the overall mission lifecycle and player interactions.
     */
    MissionFactory              public missionFactory;                  // Reference to the MissionFactory contract
    mapping(address => bool)    public enrolled;                        // Track if a player is enrolled in the mission
    mapping(address => bool)    public hasWon;                          // Track if a player has won in any round
    mapping(address => bool)    public refunded;                        // Track if a player has been refunded
    MissionData                 public missionData;                     // Struct to hold all mission data  
    bool                        public _initialized;                    // Flag to track if the contract has been initialized
    mapping(address => uint256) public failedRefundAmounts;             // Track failed refund amounts for players


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
        Status          missionStatus;                  // Current status of the mission
        uint256         enrollmentStart;                // Start and end times for enrollment
        uint256         enrollmentEnd;                  // Start and end times for enrollment
        uint256         enrollmentAmount;               // Amount required for enrollment
        uint8           enrollmentMinPlayers;           // Minimum number of players required to start the mission
        uint8           enrollmentMaxPlayers;           // Maximum number of players allowed in the mission
        uint256         missionStart;                   // Start time for the mission
        uint256         missionEnd;                     // End time for the mission
        uint8           missionRounds;                  // Total number of rounds in the mission
        uint8           roundCount;                     // Current round count  
        uint256         ethStart;                       // Initial ETH amount at the start of the mission
        uint256         ethCurrent;                     // Current ETH amount in the mission
        PlayersWon[]    playersWon;                     // Array to hold players who won in the mission     
        uint256         pauseTime;                      // Time when the mission was paused
        address[]       refundedPlayers;                // Track players who have been refunded
    }

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
        uint8           _missionRounds
    ) external payable nonReentrant {
        require(!_initialized, "Already initialized");                                                      // Ensure the contract is not already initialized
        require(msg.value > 0, "No funds sent");                                                            // Ensure some ETH is sent

        _initialized = true;

        _transferOwnership(_owner);
        missionFactory = MissionFactory(payable(_missionFactory));                                                     // Set the MissionFactory contract reference

        // Initialize mission data
        missionData.missionType             = _missionType;
        missionData.enrollmentStart         = _enrollmentStart;
        missionData.enrollmentEnd           = _enrollmentEnd;
        missionData.enrollmentAmount        = _enrollmentAmount;
        missionData.enrollmentMinPlayers    = _enrollmentMinPlayers;
        missionData.enrollmentMaxPlayers    = _enrollmentMaxPlayers;
        missionData.missionStart            = _missionStart;
        missionData.missionEnd              = _missionEnd;
        missionData.missionRounds           = _missionRounds;
        missionData.roundCount              = 0;
        missionData.ethStart                = msg.value;                        // Set initial ETH amount to the value sent during initialization
        missionData.ethCurrent              = msg.value;                        // Set current ETH amount to the value sent during initialization
        missionData.pauseTime               = 0;                                // Initialize pause time to 0
        missionData.players                 = new address[](0);                 // Initialize players array
        missionData.playersWon              = new PlayersWon[](0);              // Initialize playersWon array
        _setStatus(Status.Pending);                                             // Set initial status to Pending
        emit MissionInitialized(_owner, _missionType, block.timestamp);         // Emit event for mission initialization
    }

    // ──────────── Core Mission Functions ──────────────
    /**
     * @notice Allows a player to enroll by paying the enrollment fee.
     * @dev Player can enroll only during the enrollment window and only once.
     * @dev Reverts if:
     *      - Enrollment period not open
     *      - Max players reached
     *      - Insufficient ETH sent
     */
    function enrollPlayer() external payable nonReentrant {
        // Logic to enroll a player in the mission
        // This function should check if the player can be enrolled based on the mission's rules
		address player = msg.sender;
        require(missionData.enrollmentStart <= block.timestamp,                 "Enrollment has not started yet");  // Ensure enrollment has started
        require(missionData.enrollmentEnd >= block.timestamp,                   "Enrollment has ended");            // Ensure enrollment has not ended  
        require(missionData.players.length < missionData.enrollmentMaxPlayers,  "Max players reached");             // Ensure max players limit is not exceeded
        require(msg.value == missionData.enrollmentAmount,                      "Incorrect enrollment amount");     // Ensure the correct amount is sent for enrollment
        require(!enrolled[player],                                              "Player already enrolled");         // Ensure player is not already enrolled

        (bool allowed, string memory reason) = missionFactory.canEnroll(player);
        require(allowed, reason);                                                                                   // Check if the player is allowed to enroll

        missionData.players.push(player);                                                                           // Add player to the players array
        enrolled[player] = true;                                                                                    // Mark player as enrolled
        missionData.ethStart    += msg.value;                                                                       // Update the starting ETH amount for the mission
        missionData.ethCurrent  += msg.value;                                                                       // Update the current ETH amount for the mission

        missionFactory.recordEnrollment(player);                                                                    // Record the enrollment in the MissionFactory

        emit PlayerEnrolled(player, msg.value, uint256(missionData.players.length));                                // Emit PlayerEnrolled event with player address, amount, and total players count
        _setStatus(Status.Enrolling);                                                                               // Set mission status to Enrolling
    }

    /**
     * @dev Starts the mission.
     * Only callable by the owner or an authorized address when the mission is ready.
     */
    function armMission() external nonReentrant onlyOwnerOrAuthorized {
        require(block.timestamp >= missionData.enrollmentEnd,                   "Enrollment still ongoing");        // Ensure enrollment has ended
        if (missionData.players.length < missionData.enrollmentMinPlayers) {
            _setStatus(Status.Failed);                                                                               // If not enough players, refund and set status to Failed
            _refundPlayers();
        }
        else
        {
            _setStatus(Status.Active);
            emit MissionArmed(block.timestamp, uint256(missionData.players.length));                                                                               // Set mission status to Active
        }
    }

    /**
     * @notice Called by a player to claim a round reward.
     * @dev A player can only win once. The mission must be Active and not expired.
     * @dev After each round, the mission is Paused for:
     *      - 5 minutes for normal rounds
     *      - 1 minute before the final round
     * @dev Emits {RoundClaimed}.
     */
    function callRound() external nonReentrant {
        require(missionData.pauseTime == 0 || block.timestamp >= missionData.pauseTime +
            ((missionData.roundCount + 1 == missionData.missionRounds)
                ? 1 minutes
                : 5 minutes),                                                   "Mission is paused");               // Ensure mission is not paused (5 mins for normal rounds, 1 min for last round)
        require(!hasWon[msg.sender],                                            "Player already won this mission"); // Ensure player has not already won this round
        require(enrolled[msg.sender],                                           "Not enrolled in mission");         // Ensure caller is enrolled in the mission
        require(block.timestamp >= missionData.missionStart,                    "Mission has not started yet");     // Ensure mission has started
        require(block.timestamp < missionData.missionEnd,                       "Mission has ended");               // Ensure mission has not ended
        require(missionData.missionStatus == Status.Active,                     "Mission is not Active");           // Ensure mission is in Active status
        require(missionData.roundCount < missionData.missionRounds,             "All rounds completed");            // Ensure there are rounds left to play
        if (missionData.missionStatus == Status.Paused) {
            _setStatus(Status.Active);                                                                               // If mission was paused, set status to Active
        }
        
        uint256 progress = (block.timestamp - missionData.missionStart) * 100 /                                     
            (missionData.missionEnd - missionData.missionStart);                                                    // Calculate the progress of the mission in percentage
        uint256 lastAmount = missionData.playersWon.length > 0
            ? missionData.playersWon[missionData.playersWon.length - 1].amountWon                                   // Get the last amount won by a player, or 0 if no players have won yet
            : 0;
        uint256 lastProgress = (lastAmount * 100) / missionData.ethStart;                                           // Calculate the last progress based on the last amount won     
        uint256 payout = ((progress - lastProgress) * missionData.ethStart) / 100;                                  // Calculate the payout for the current round based on the progress and the initial ETH amount

        missionData.ethCurrent   -= payout;                                                                         // shrink remaining pot
        missionData.roundCount++;

        hasWon[msg.sender]      = true;                                                                             // Mark player as having won this round                                           
        missionData.playersWon.push(PlayersWon({
            player:    msg.sender,
            amountWon: payout
        }));

        (bool sent, ) = address(msg.sender).call{value: payout}("");                                                // Transfer the funds to the player
        require(sent, "Payout transfer failed");                                                                    // Ensure the transfer was successful

        emit RoundCalled(msg.sender, missionData.roundCount, payout, missionData.ethCurrent);                       // Emit RoundCalled event with player address, round number, payout amount, and remaining ETH  

        if (missionData.roundCount == missionData.missionRounds) {
            _setStatus(Status.Ended);                                                                               // If all rounds are completed, set status to Ended
            _withdrawFunds(false);                                                                                       // Withdraw funds to MissionFactory contract
        }
        else {
            _setStatus(Status.Paused);                                                                              // If not all rounds are completed, set status to Ready for the next round
        }
        
    }

    // ────────────── Financial Functions ───────────────
    /**
     * @dev Refunds players if the mission fails.
     * This function can be called by the owner or an authorized address.
     */
    function refundPlayers() external nonReentrant onlyOwnerOrAuthorized {
        _refundPlayers();                                                                                           // Call internal refund function
    }
    
	/**
     * @dev Add funds to prize pool.
     */
	function increasePot() external payable {
		require(msg.value > 0, "No funds sent");
		require(msg.sender == address(missionFactory), "Only factory can fund");
		missionData.ethStart 	+= msg.value;
		missionData.ethCurrent 	+= msg.value;
		emit PotIncreased(msg.value, missionData.ethCurrent);
	}

    /**
     * @notice Distributes remaining ETH after mission completion or failure.
     * @dev Sends:
     *      - 25% to factory owner
     *      - 75% to MissionFactory (for future missions)
     * @dev If `force = true`, also withdraws failed refund amounts.
     */
    function withdrawFunds() external nonReentrant onlyOwnerOrAuthorized {
        _withdrawFunds(true);                                                                                       // Call internal withdraw function
    }

    /**
     * @notice Allows owner or authorized to finalize a mission after time expiry.
     * @dev If no rounds claimed → refunds players and marks Failed.
     * @dev If rounds claimed → ends mission and gives remaining ETH to last winner.
     */   
    function forceFinalizeMission() external onlyOwnerOrAuthorized nonReentrant {
        require(block.timestamp > missionData.missionEnd,                                                   "Mission not ended yet");       // Ensure mission has ended
        require(missionData.missionStatus == Status.Active || missionData.missionStatus == Status.Paused,   "Mission cannot be finalized"   // Ensure mission is either Active or Paused
        );

        // Case 1: No rounds completed → refund all players (mark as Failed)
        if (missionData.roundCount == 0) {
            _setStatus(Status.Failed);
            _refundPlayers();
        } else {
            // Case 2: Some rounds completed → End mission and payout remaining pot
            _setStatus(Status.Ended);
            _withdrawFunds(false);
        }
    }

    /**
     * @dev Fallback function to handle incoming ETH.
     * This function is called when the contract receives ETH without matching calldata.
     * It allows the contract to accept ETH and handle it appropriately.
     */
    receive() external payable {}

    /**
     * @dev Fallback function to handle incoming ETH with non-matching calldata.
     * This function is called when the contract receives ETH with non-matching calldata.
     * It allows the contract to accept ETH and handle it appropriately.
     */
    fallback() external payable {}

    // ───────────────── View Functions ─────────────────
    /**
     * @dev Returns the MissionData structure.
     */
    function getMissionData() external view returns (MissionData memory) {
        return missionData;
    }

    // ──────────────── Internal Helpers ────────────────
    /**
     * @dev Sets the status of the mission.
     * @param newStatus The new status to set for the mission.
     */
    function _setStatus(Status newStatus) internal {
        Status oldStatus = missionData.missionStatus;                                   // Store the old status
        missionData.missionStatus = newStatus;                                          // Update the mission status    
        missionFactory.setMissionStatus(newStatus);                   					// Update the status in MissionFactory
        if (newStatus == Status.Paused) {
            missionData.pauseTime = block.timestamp;                                    // Record the time when the mission was paused
            emit MissionPaused(block.timestamp);
        }
        else if (newStatus == Status.Active) {
            missionData.pauseTime = 0;                                                  // Reset pause time when the mission is active
            emit MissionResumed(block.timestamp);
        }
        emit MissionStatusChanged(oldStatus, newStatus, block.timestamp);
    }

    /**
     * @notice Distributes remaining ETH after mission completion or failure.
     * @dev Sends:
     *      - 25% to factory owner
     *      - 75% to MissionFactory (for future missions)
     * @dev If `force = true`, also withdraws failed refund amounts.
     */
    function _withdrawFunds(bool force) internal {
        require(missionData.missionStatus == Status.Ended || 
                missionData.missionStatus == Status.Failed, "Mission not ended");       // Ensure mission is ended
        uint256 balance = address(this).balance;
        require(balance > 0,                                "No funds to withdraw");    // Ensure there are funds to withdraw

        uint256 distributable;
        if (force) {
            distributable = balance;                                                    // If force is true, all funds are distributable
        } else {
            uint256 unclaimable = _getTotalFailedRefunds();                             // Get total failed refunds for all players  
            if (unclaimable > balance) {                                                // If unclaimable amount exceeds the balance      
                unclaimable = balance;                                                  // safety check
            }
            distributable = balance - unclaimable;                                      // Calculate distributable amount by subtracting unclaimable amounts
        }

        require(distributable > 0, "No funds to withdraw");                             // Ensure there are funds to withdraw after deducting unclaimable amounts

        uint256 ownerShare = (distributable * 25) / 100;                                // Calculate the owner's share (25% of distributable funds)     
        uint256 factoryShare = distributable - ownerShare;                              // Calculate the factory's share (75% of distributable funds)     

        (bool ok1, ) = payable(missionFactory.owner()).call{value: ownerShare}("");     // Attempt to transfer the owner's share to the MissionFactory owner
        require(ok1, "Owner transfer failed");                                          // Ensure the transfer was successful   

        (bool ok2, ) = payable(address(missionFactory)).call{value: factoryShare}("");  // Attempt to transfer the factory's share to the MissionFactory contract
        require(ok2, "Factory transfer failed");                                        // Ensure the transfer was successful 

        missionFactory.registerMissionFunds(factoryShare, missionData.missionType);     // Register the factory's share with the MissionFactory for future mission bonuses

        emit FundsWithdrawn(ownerShare, factoryShare);                        // Emit event for funds withdrawal  
    }

    /**
     * @dev Returns the total amount of failed refunds for all players.
     * This function iterates through all players and sums their failed refund amounts.
     * @return total The total amount of failed refunds for all players.
     */
    function _getTotalFailedRefunds() internal view returns (uint256 total) {
        for (uint256 i = 0; i < missionData.players.length; i++) {
            address player = missionData.players[i];
            total += failedRefundAmounts[player];
        }
    }

    /**
     * @dev Refunds players if the mission fails.
     * This function is internal and can only be called when the mission is in Failed status.
     * It ensures that the mission has ended and that the enrollment period has passed.
     * It refunds all enrolled players their enrollment amount.
     */
    function _refundPlayers() internal {
        require(block.timestamp >= missionData.enrollmentEnd,           "Enrollment still ongoing");            // Ensure enrollment has ended
        require(missionData.missionStatus == Status.Failed,             "Mission is not in Failed status");     // Ensure mission is in Failed status
        require(missionData.players.length > 0,                         "No players to refund");                // Ensure there are players to refund
        bool _force = true;
        for (uint256 i = 0; i < missionData.players.length; i++) {
            address player = missionData.players[i];                                                            // Get the player address
            if (!refunded[player]) {                                                                            // Check if player has not been refunded
                (bool ok, ) = payable(player).call{ value: missionData.enrollmentAmount }("");                  // Attempt to transfer the refund amount to the player
                if (ok) {
                    refunded[player] = true;                                                                    // If transfer successful, mark player as refunded
                    missionData.refundedPlayers.push(player);                                                   // If transfer successful, track refunded player
                    emit PlayerRefunded(player, missionData.enrollmentAmount);                                  // Emit PlayerRefunded event with player address and amount
                } else {
                    failedRefundAmounts[player] += missionData.enrollmentAmount;                                // Track failed refund amounts for players
                    emit RefundFailed(player, missionData.enrollmentAmount);                                    // Log the failure, but don’t revert
                    _force = false;                                                                             // Set force to false if any refund fails     
                }
            }
        }
        _withdrawFunds(_force);                                                                                 // Withdraw funds to MissionFactory contract 
    }

}
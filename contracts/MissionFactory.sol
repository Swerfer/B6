// SPDX-License-Identifier: MIT
pragma solidity ^0.8.30;

// ───────────────────── Imports ────────────────────────
import "@openzeppelin/contracts/access/Ownable.sol";
import "@openzeppelin/contracts/proxy/Clones.sol";
import "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import "@openzeppelin/contracts/token/ERC20/utils/SafeERC20.sol";
import "@openzeppelin/contracts/utils/ReentrancyGuard.sol";

// ────────────────────── Enums ─────────────────────────
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
    using SafeERC20 for IERC20;
    using Clones    for address;
    
    // ────────────────── Events ───────────────────────
    /** 
     * @dev Events emitted by the MissionFactory contract.
     * These events are used to log important actions and state changes within the contract.
     */
    event AuthorizedAddressAdded                (address        indexed addr                                            );
    event AuthorizedAddressRemoved              (address        indexed addr                                            );
    event MissionStatusUpdated                  (address        indexed mission,        Status      indexed status      );
    event MissionCreated                        (address        indexed missionAddress, MissionType indexed missionType );
    event MissionFactoryOwnershipTransferred    (address        indexed newOwner                                        );
    event FundsReceived                         (address        indexed sender,         uint256             amount      );
    event MissionFundsRegistered                (MissionType    indexed missionType,    uint256             amount      );
    event FundsWithdrawn                        (address        indexed to,             uint256             amount      );    

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

    // ────────────── State Variables ───────────────────
    /**
     * @dev State variables for the MissionFactory contract.
     * These variables store the state of the contract, including authorized addresses, reserved funds, mission statuses, and the implementation address for missions.
     */
    mapping(address => bool)        public authorized;              // Mapping to track authorized addresses
    mapping(MissionType => uint256) public reservedFunds;           // Track funds by type
    mapping(address => Status)      public missionStatus;           // Mapping to hold the status of each mission
    address[]                       public missions;                // Array to hold all mission addresses
    address                         public missionImplementation;   // Address of the Mission implementation contract for creating new missions

    // ─────────────────── Structs ──────────────────────
    /**
     * @dev Struct to hold information about players who won the mission.
     * Contains the player's address and the amount they won.
     */
    struct PlayersWon {
        address player;
        uint256 amountWon;
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

    // ─────────── Internal Helper Functions ────────────
    /**
     * @dev Internal common handler for incoming ETH.
     */
    function _handleFunds() internal {
        require(msg.value > 0, "Must send ETH to fund manager");
        emit FundsReceived(msg.sender, msg.value);
    }

    // ──────────────── Admin Functions ─────────────────
    /**
     * @dev Authorizes an address to perform actions on behalf of the MissionFactory.
     * @param account The address to authorize.
     */
    function authorize(address account) external onlyOwnerOrAuthorized() {
        require(account != address(0),  "Invalid address");                         // Ensure the account is valid
        authorized[account] = true;                                                 // Authorize the account
    }

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
     * @dev Transfers ownership of the MissionFactory contract.
     * @param newOwner The address of the new owner.
     */
    function transferOwnership(address newOwner) public override onlyOwnerOrAuthorized {
        address oldOwner = owner();                                                 // Get the current owner address
        require(newOwner != address(0), "Invalid new owner");                       // Ensure the new owner address is valid
        require(newOwner != oldOwner,   "New owner is the same as current owner");  // Ensure the new owner is different from the current owner
        super.transferOwnership(newOwner);                                          // Transfer ownership to the new owner

        authorized[oldOwner] = true;                                                // Ensure the old owner remains authorized

        if (authorized[newOwner]) {
            authorized[newOwner] = false;                                           // Remove authorization for the new owner if they were already authorized
        }
        emit MissionFactoryOwnershipTransferred(newOwner);                               // Emit event for ownership transfer
    }

    // ───────────── Core Factory Functions ─────────────
    /**
     * @dev Creates a new mission with the specified parameters.
     * @param _missionType          The type of the mission.
     * @param _enrollmentStart      The start time for enrollment.
     * @param _enrollmentEnd        The end time for enrollment.
     * @param _enrollmentAmount     The amount required for enrollment.
     * @param _enrollmentMinPlayers The minimum number of players required to start the mission.
     * @param _enrollmentMaxPlayers The maximum number of players allowed to enroll.
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
        uint8           _enrollmentMaxPlayers,  // Maximum number of players allowed to enroll
        uint256         _missionStart,          // Start time for the mission
        uint256         _missionEnd,            // End time for the mission
        uint8           _missionRounds          // Number of rounds in the mission
        ) external payable onlyOwnerOrAuthorized returns (address) {
            require(_enrollmentStart < _enrollmentEnd,                  "Enrollment start must be before end");                         // Ensure enrollment start is before end
            require(_enrollmentMinPlayers >= 10,                        "Minimum players must be greater or equal to 10");              // Ensure minimum players is greater than zero
            require(_enrollmentMaxPlayers >= _enrollmentMinPlayers * 2, "Max players must be greater than or equal to min players");    // Ensure max players is at least 2x min players      
            require(_enrollmentAmount > 0,                              "Enrollment amount must be greater than zero");                 // Ensure enrollment amount is greater than zero
            require(_missionStart > _enrollmentEnd,                     "Mission start must be after enrollment end");                  // Ensure mission start is after enrollment end
            require(_missionStart < _missionEnd,                        "Mission start must be before end");                            // Ensure mission start is before end
            require(_missionRounds > 0,                                 "Mission rounds must be greater than zero");                    // Ensure mission rounds is greater than zero
            require(_missionRounds <= 100,                              "Mission rounds must be less than or equal to 100");            // Ensure mission rounds does not exceed 255

			address clone = missionImplementation.clone(); 								// EIP-1167 minimal proxy

            Mission(clone).initialize(
				owner(),																// Set the owner of the mission to the owner of MissionFactory
				address(this),															// Set the MissionFactory address
                _missionType,                                                           // Set the type of the mission
                _enrollmentStart,                                                       // Set the enrollment start time
                _enrollmentEnd,                                                         // Set the enrollment end time
                _enrollmentAmount,                                                      // Set the enrollment amount
                _enrollmentMinPlayers,                                                  // Set the minimum players required
                _enrollmentMaxPlayers,                                                  // Set the maximum players allowed
                _missionStart,                                                          // Set the mission start time
                _missionEnd,                                                            // Set the mission end time
                _missionRounds                                                          // Set the number of rounds in the mission
            );

        missions.push(clone);                                                			// Add the new mission to the list of missions
        emit MissionCreated(clone, _missionType);                                       // Emit event for mission creation

        // Calculate allocation based on mission type
        uint256 allocation = 0;
        if (_missionType == MissionType.Hourly)             allocation = reservedFunds[_missionType] / 24;  // Hourly missions get 1/24th of the reserved funds
        else if (_missionType == MissionType.QuarterDaily)  allocation = reservedFunds[_missionType] / 4;   // QuartDaily missions get 1/4th of the reserved funds
        else if (_missionType == MissionType.BiDaily)       allocation = reservedFunds[_missionType] / 2;   // BiDaily missions get 1/2 of the reserved funds
        else if (_missionType == MissionType.Daily)         allocation = reservedFunds[_missionType] / 7;   // Daily missions get 1/7th of the reserved funds
        else if (_missionType == MissionType.Weekly)        allocation = reservedFunds[_missionType] / 4;   // Weekly missions get 1/4th of the reserved funds
        else if (_missionType == MissionType.Monthly)       allocation = reservedFunds[_missionType] / 12;  // Monthly missions get 1/12th of the reserved funds

        if (allocation > 0 && address(this).balance >= allocation) {
            reservedFunds[_missionType] -= allocation;
            (bool sent, ) = payable(clone).call{value: allocation}("");
            require(sent, "Funding failed");
        }

        return clone;						                                                         // Return the address of the newly created mission
    }

    /**
     * @dev Sets the status of a mission.
     * @param missionAddress The address of the mission to update.
     * @param status The new status to set for the mission.
     */
    function setMissionStatus(address missionAddress, Status status) external onlyOwnerOrAuthorized {
        require(missionAddress != address(0),                   "Invalid mission address");         // Ensure mission address is valid
        require(missionStatus[missionAddress] != Status.Ended,  "Mission already ended");           // Ensure mission is not already ended
        require(missionStatus[missionAddress] != Status.Failed, "Mission already failed");          // Ensure mission is not already failed
        missionStatus[missionAddress] = status;                                                     // Update the mission status
		emit MissionStatusUpdated(missionAddress, status);
    } 
    
    // ──────────────── Financial Functions ─────────────
    /**
     * @dev Public entry-point for authorized callers to send ETH.
     */
    function FundManager() external payable onlyOwnerOrAuthorized nonReentrant() {
        _handleFunds();                                                             // Handle incoming funds
    }

    /**
     * @dev Registers mission funds for a specific mission type.
     * @param _type The type of the mission.
     * @param _amount The amount of funds to register.
     */
    function registerMissionFunds(MissionType _type, uint256 _amount) external {
        require(authorized[msg.sender], "Not authorized");
        reservedFunds[_type] += _amount;
        emit MissionFundsRegistered(_type, _amount);
    }

    /**
     * @dev Called when ETH is sent with empty calldata.
     *      Falls back to the same logic as fundManager().
     */
    receive() external payable {
        _handleFunds();         // Handle incoming funds
    }

    /**
     * @dev Called when ETH is sent with non-matching calldata.
     *      Also falls back to fundManager logic if any ETH is attached.
     */
    fallback() external payable {
        if (msg.value > 0) {
            _handleFunds();     // Handle incoming funds if any ETH is attached
        }
    }

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
     * @dev Checks if an address is authorized.
     * @param account The address to check.
     * @return bool indicating whether the address is authorized.
     */
    function isAuthorized(address account) external view returns (bool) {
        require(account != address(0), "Invalid address");                          // Ensure the account is valid
        return authorized[account];                                                 // Return whether the account is authorized
    }

    /**
     * @dev Returns the status of a mission.
     * @param missionAddress The address of the mission to check.
     * @return mission data of the mission.
     */
    function getMissionData(address missionAddress) external view returns (Mission.MissionData memory) {
        require(missionAddress != address(0), "Invalid mission address");          // Ensure mission address is valid
        return Mission(missionAddress).getMissionData();                           // Return the mission data from the Mission contract
    }

    /**
     * @dev Returns the status of a mission.
     * @return all missions and their statuses.
     */
    function getAllMissions() external view returns (address[] memory, Status[] memory)
    {
        uint256 len = missions.length;
        address[] memory allMissions = new address[](len);
        Status[] memory allStatuses = new Status[](len);

        for (uint256 i = 0; i < len; i++) {
            address missionAddr = missions[i];
            allMissions[i] = missionAddr;
            allStatuses[i] = missionStatus[missionAddr];
        }

        return (allMissions, allStatuses);
    }

    /**
     * @dev Returns the addresses of missions filtered by status.
     * @param filter The status to filter missions by.
     * @return An array of mission addresses that match the specified status.
     */
    function getMissionsByStatus(Status filter) external view returns (address[] memory)
    {
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
    function getMissionsEndened() external view returns (address[] memory) {
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
 * @title   Mission
 * @author  Dennis Bakker
 * @notice  Manages a multi-round ETH-backed mission game where players enroll, compete across timed rounds,
 *          and receive proportional payouts. Integrates with a central MissionFactory for authorization, status tracking,
 *          fee collection, and final fund management.
 * @dev     Upgradeable via OpenZeppelin’s Initializable; tracks mission lifecycle (Pending → Enrolling → Active → Paused → Ended/Failed),
 *          enforces enrollment bounds, per-round timing and payout calculation, owner/authorized controls, safe ETH transfers,
 *          and automatic fee split (25% owner, 75% MissionFactory for future mission bonuses) or full refunds on failure.
 *
 * State Variables:
 * - missionFactory                  : Interface to central MissionFactory for status updates, authorization checks, and fee withdrawal.
 * - enrolled, hasWon, refunded : Mappings to track player enrollment, round wins, and refund status.
 * - missionData                : Struct capturing mission parameters, timing, ETH balances, players list, per-round records, etc.
 *
 * Core Features:
 * - initialize(...)                    : Sets up mission parameters, funds initial ETH pot, registers with MissionFactory.
 * - enrollPlayer(...)                  : Allows valid addresses to join by paying the exact enrollment fee.
 * - armMission()                       : Transitions to Active or triggers refunds if minimum players unmet.
 * - callRound()                        : Lets each enrolled player claim their share based on elapsed time progress,
 *                                        pauses between rounds, and auto-ends or pauses after each claim.
 * - withdrawFunds(), _withdrawFunds()  : Splits final pot (25% fee to owner, 75% to MissionFactory) when mission ends.
 * - refundPlayers(), _refundPlayers()  : Returns enrollment fees to all if mission fails.
 *
 * Events:
 * - MissionStatusChanged, PlayerEnrolled, MissionArmed, RoundCalled, PlayerRefunded,
 *   FundsWithdrawn, MissionPaused, MissionResumed, RefundFailed
 *
 * Usage:
 * 1. Deploy via a proxy, calling initialize(...) with owner, MissionFactory, timing, player limits, and rounds.
 * 2. Enroll at least `enrollmentMinPlayers` before `enrollmentEnd` by sending `enrollmentAmount` ETH.
 * 3. After enrollment, owner or authorized address calls `armMission()`.
 * 4. Players call `callRound()` once each round to claim payouts in real time.
 * 5. On final round, contract auto-ends and splits remaining ETH.
 *
 * Security:
 * - ReentrancyGuard on fund-moving functions.
 * - Strict timing and status checks.
 * - Safe ETH transfer patterns with require on `call`.
 */ 

contract Mission is Ownable, ReentrancyGuard {

    // ───────────────────── Events ─────────────────────
    event MissionStatusChanged  (Status     indexed previousStatus, Status  indexed newStatus,      uint256 timestamp                   );
    event PlayerEnrolled        (address    indexed player,         uint256         amount,         uint256 totalPlayers                );
    event MissionArmed          (uint256            timestamp,      uint256         totalPlayers                                        );
    event RoundCalled           (address    indexed player,         uint8   indexed roundNumber,    uint256 payout, uint256 ethRemaining);
    event PlayerRefunded        (address    indexed player,         uint256         amount                                              );
    event FundsWithdrawn        (address    indexed to,             uint256         fee,            uint256 remainder                   );
    event MissionPaused         (uint256            timestamp                                                                           );
    event MissionResumed        (uint256            timestamp                                                                           );
    event RefundFailed          (address    indexed player,         uint256         amount                                              ); 

    // ──────────────────── Modifiers ───────────────────
    /**
     * @dev Modifier to restrict access to the owner or an authorized address.
     * This is used for functions that can only be called by the owner or an authorized address.
     */
    modifier onlyOwnerOrAuthorized() {
        require(
            msg.sender == owner() || missionFactory.isAuthorized(msg.sender),
            "Not owner or authorized"
        );
        _;
    }

    // ──────────────── State Variables ─────────────────
    /**
     * @dev Reference to the MissionFactory contract.
     * This contract manages the overall mission lifecycle and player interactions.
     */
    MissionFactory              public missionFactory; 
    mapping(address => bool)    public enrolled;
    mapping(address => bool)    public hasWon;
    mapping(address => bool)    public refunded;
    MissionData                 public missionData;
    bool                        public _initialized;                  // Flag to track if the contract has been initialized

    // ──────────────────── Structs ───────────────────── 
    /**
     * @dev Struct to hold information about players who won the mission.
     * Contains the player's address and the amount they won.
     */
    struct PlayersWon {
        address player;
        uint256 amountWon;
    }

    /**
     * @dev Struct to hold all mission data.
     * Contains information about players, mission status, enrollment details, and financials.
     */
    struct MissionData {
        address[]       players;
        MissionType     missionType;
        Status          missionStatus;
        uint256         enrollmentStart;
        uint256         enrollmentEnd;
        uint256         enrollmentAmount;
        uint8           enrollmentMinPlayers;
        uint8           enrollmentMaxPlayers;
        uint256         missionStart;
        uint256         missionEnd;
        uint8           missionRounds;
        uint8           roundCount;
        uint256         ethStart;    
        uint256         ethCurrent;
        PlayersWon[]    playersWon;
        uint256         pauseTime;
    }

    // ────────────────── Constructor ───────────────────
    /**
     * @dev Constructor for the Mission contract.
     * Initializes the contract with the owner set to address(0) to prevent accidental ownership.
     * The actual ownership will be set during the initialization phase.
     */
    constructor() Ownable(address(0)) {}      

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
        require(!_initialized, "Already initialized");                  // Ensure the contract is not already initialized
        require(_handleFunds(), "Must send ETH to initialize");         // Ensure some ETH is sent during initialization
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
    }

    // ──────────── Core Mission Functions ──────────────
    /**
     * @dev Enrolls a player in the mission.
     * @param player The address of the player to enroll.
     */
    function enrollPlayer(address player) external payable nonReentrant {
        // Logic to enroll a player in the mission
        // This function should check if the player can be enrolled based on the mission's rules
        require(player != address(0),                                           "Invalid player address");          // Ensure player address is valid
        require(missionData.enrollmentStart <= block.timestamp,                 "Enrollment has not started yet");  // Ensure enrollment has started
        require(missionData.enrollmentEnd >= block.timestamp,                   "Enrollment has ended");            // Ensure enrollment has not ended  
        require(missionData.players.length < missionData.enrollmentMaxPlayers,  "Max players reached");             // Ensure max players limit is not exceeded
        require(msg.value == missionData.enrollmentAmount,                      "Incorrect enrollment amount");     // Ensure the correct amount is sent for enrollment
        require(!enrolled[player],                                              "Player already enrolled");         // Ensure player is not already enrolled

        missionData.players.push(player);                                                                           // Add player to the players array
        enrolled[player] = true;                                                                                    // Mark player as enrolled
        missionData.ethStart    += msg.value;                                                                       // Update the starting ETH amount for the mission
        missionData.ethCurrent  += msg.value;                                                                       // Update the current ETH amount for the mission
        emit PlayerEnrolled(player, msg.value, uint256(missionData.players.length));                                // Emit PlayerEnrolled event with player address, amount, and total players count
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
     * @dev Calls the current round of the mission.
     * This function can be called by any enrolled player to participate in the current round.
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
        require(missionData.missionEnd > missionData.missionStart,              "Mission start time must be before end time"); // Ensure mission start time is before end time
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
            _withdrawFunds();                                                                                       // Withdraw funds to MissionFactory contract
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
     * @dev Withdraws funds from the mission contract.
     * This function can be called by the owner or an authorized address.
     */
    function withdrawFunds() external nonReentrant onlyOwnerOrAuthorized {
        _withdrawFunds();                                                                                           // Call internal withdraw function
    }

    /**
     * @dev Fallback function to handle incoming ETH.
     * This function is called when the contract receives ETH without matching calldata.
     * It allows the contract to accept ETH and handle it appropriately.
     */
    receive() external payable {
        _handleFunds();                                                                                     // Handle the incoming funds
    }

    /**
     * @dev Fallback function to handle calls to the contract.
     * This function is called when the contract receives a call without matching function signature.
     * It allows the contract to accept calls and handle them appropriately.
     */
    fallback() external payable {
        _handleFunds();                                                                                     // Handle the incoming funds
    }

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
        missionFactory.setMissionStatus(address(this), Status(newStatus));                   // Update the status in MissionFactory
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
     * @dev Handles incoming funds to the contract.
     * This function is called when ETH is sent to the contract.
     * It ensures that the funds are handled correctly and updates the mission data.
     */ 
    function _handleFunds() internal {
        require(msg.value > 0, "No funds sent");                                                           // Ensure some ETH is sent
        missionData.ethStart    += msg.value;                                                              // Update the starting ETH amount for the mission
        missionData.ethCurrent  += msg.value;                                                              // Update the current ETH amount for the mission
    }

    /**
     * @dev Withdraws funds from the mission contract.
     * This function is internal and can only be called when the mission is in Ended or Failed status.
     * It ensures that the mission has ended and that there are funds to withdraw.
     */
    function _withdrawFunds() internal {
        require(missionData.missionStatus == Status.Ended || 
                missionData.missionStatus == Status.Failed,                     "Mission not ended");       // Ensure mission is ended
        uint256 balance = address(this).balance;
        require(balance > 0,                                                    "No funds to withdraw");    // Ensure there are funds to withdraw

        // Compute 25% fee
        uint256 fee = balance / 4;                                                                          // equivalent to 25%
        uint256 remainder = balance - fee;                                                                  // the other 75%

        // 1) Pay the fee to the MissionFactory's owner
        address payable mgrOwner = payable(missionFactory.owner());                                         // Get the owner of the MissionFactory contract  
        (bool ok, ) = mgrOwner.call{ value: fee }("");                                                      // Attempt to transfer the fee to the owner    
        require(ok,                                                             "Owner transfer failed");   // Ensure the transfer was successful    

        // 2) Send the rest to the MissionFactory contract
        (ok, ) = payable(address(missionFactory)).call{ value: remainder }("");                             // Attempt to transfer the remaining funds to the MissionFactory contract
        require(ok,                                                             "Manager transfer failed"); // Ensure the transfer to MissionFactory contract was successful
        missionFactory.registerMissionFunds(missionData.missionType, remainder);                            // Register the remaining funds with the MissionFactory for the specific mission type
        emit FundsWithdrawn(mgrOwner, fee, remainder);
    }

    /**
     * @dev Refunds players if the mission fails.
     * This function is internal and can only be called when the mission is in Failed status.
     * It ensures that the mission has ended and that the enrollment period has passed.
     * It refunds all enrolled players their enrollment amount.
     */
    function _refundPlayers() internal {
        require(block.timestamp >= missionData.enrollmentEnd,                   "Enrollment still ongoing");        // Ensure enrollment has ended
        require(missionData.missionStatus == Status.Failed,                     "Mission is not in Failed status"); // Ensure mission is in Failed status
        for (uint8 i = 0; i < missionData.players.length; i++) {
            address player = missionData.players[i];                                                                // Get the player address
            uint256 amount = missionData.enrollmentAmount;                                                          // Get the enrollment amount for refund
            (bool ok, ) = player.call{ value: amount }("");                                                         // Attempt to transfer the refund amount to the player
            if (ok) {
                refunded[player] = true;                                                                            // Mark player as refunded
                emit PlayerRefunded(player, amount);
            } else {
                emit RefundFailed(player, amount);                                                                  // Log the failure, but don’t revert
            }
        }
        _withdrawFunds();
    }

}
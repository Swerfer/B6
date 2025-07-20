// SPDX-License-Identifier: MIT
pragma solidity ^0.8.30;

import "@openzeppelin/contracts-upgradeable/proxy/utils/Initializable.sol";
import "@openzeppelin/contracts-upgradeable/access/OwnableUpgradeable.sol";
import "@openzeppelin/contracts-upgradeable/utils/ReentrancyGuardUpgradeable.sol";

import "./MissionTypes.sol";
import "./B6Manager.sol";

/**
 * @title   B6Mission
 * @author  Dennis Bakker
 * @notice  Manages a multi-round ETH-backed mission game where players enroll, compete across timed rounds,
 *          and receive proportional payouts. Integrates with a central B6Manager for authorization, status tracking,
 *          fee collection, and final fund management.
 * @dev     Upgradeable via OpenZeppelin’s Initializable; tracks mission lifecycle (Pending → Enrolling → Active → Paused → Ended/Failed),
 *          enforces enrollment bounds, per-round timing and payout calculation, owner/authorized controls, safe ETH transfers,
 *          and automatic fee split (25% owner, 75% B6Manager for future mission bonuses) or full refunds on failure.
 *
 * State Variables:
 * - b6Manager                  : Interface to central B6Manager for status updates, authorization checks, and fee withdrawal.
 * - enrolled, hasWon, refunded : Mappings to track player enrollment, round wins, and refund status.
 * - missionData                : Struct capturing mission parameters, timing, ETH balances, players list, per-round records, etc.
 *
 * Core Features:
 * - initialize(...)                    : Sets up mission parameters, funds initial ETH pot, registers with B6Manager.
 * - enrollPlayer(...)                  : Allows valid addresses to join by paying the exact enrollment fee.
 * - armMission()                       : Transitions to Active or triggers refunds if minimum players unmet.
 * - callRound()                        : Lets each enrolled player claim their share based on elapsed time progress,
 *                                        pauses between rounds, and auto-ends or pauses after each claim.
 * - withdrawFunds(), _withdrawFunds()  : Splits final pot (25% fee to owner, 75% to B6Manager) when mission ends.
 * - refundPlayers(), _refundPlayers()  : Returns enrollment fees to all if mission fails.
 *
 * Events:
 * - MissionStatusChanged, PlayerEnrolled, MissionArmed, RoundCalled, PlayerRefunded,
 *   FundsWithdrawn, MissionPaused, MissionResumed, RefundFailed
 *
 * Usage:
 * 1. Deploy via a proxy, calling initialize(...) with owner, B6Manager, timing, player limits, and rounds.
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

contract B6Mission is Initializable, OwnableUpgradeable, ReentrancyGuardUpgradeable { 
    // ───────────────────── State Variables ─────────────────────
    /**
     * @dev Reference to the B6Manager contract.
     * This contract manages the overall mission lifecycle and player interactions.
     */
    B6Manager public b6Manager; 
    mapping(address => bool) public enrolled;
    mapping(address => bool) public hasWon;
    mapping(address => bool) public refunded;
    B6MissionData public missionData;

    // ──────────────────── Modifiers ───────────────────
    /**
     * @dev Modifier to restrict access to the owner or an authorized address.
     * This is used for functions that can only be called by the owner or an authorized address.
     */
    modifier onlyOwnerOrAuthorized() {
        require(
            msg.sender == owner() || b6Manager.isAuthorized(msg.sender),
            "Not owner or authorized"
        );
        _;
    }

    // ───────────────────── Structs ────────────────────
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
    struct B6MissionData {
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

    // ───────────────────── Enums ──────────────────────
    /**
     * @dev Enum to represent the status of the mission.
     * The mission can be in one of several states: Pending, Enrolling, Ready, Active, Paused, Ended, or Failed.
     */
    enum Status {
        Pending,
        Enrolling,
        Active,
        Paused,
        Ended,
        Failed
    }

    // ───────────────────── Events ─────────────────────

    // ‣ Mission lifecycle
    event MissionStatusChanged(
        Status indexed  previousStatus,
        Status indexed  newStatus,
        uint256         timestamp
    );

    // ‣ Enrollment
    event PlayerEnrolled(
        address indexed player,
        uint256         amount,
        uint256         totalPlayers
    );

    // ‣ Mission arming
    event MissionArmed(
        uint256         timestamp,
        uint256         totalPlayers
    );

    // ‣ Round execution
    event RoundCalled(
        address indexed player,
        uint8 indexed   roundNumber,
        uint256         payout,
        uint256         ethRemaining
    );

    // ‣ Refunds
    event PlayerRefunded(
        address indexed player,
        uint256         amount
    );

    // ‣ Withdrawals
    event FundsWithdrawn(
        address indexed to,
        uint256         fee,
        uint256         remainder
    );

    // ‣ Pauses and resumes
    event MissionPaused(
        uint256         timestamp
    );
    event MissionResumed(
        uint256         timestamp
    );

    // ‣ Refund failures
    event RefundFailed(
        address indexed player, 
        uint256         amount
    ); 

    // ───────────────────── Initializer ────────────────────
    /**
     * @dev Initializes the B6Mission contract.
     * This function sets the initial values for the mission and registers it with the B6Manager.
     * It can only be called once during contract deployment.
     * @param _owner                The address of the owner of the contract.
     * @param _b6Manager            The address of the B6Manager contract.
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
        address         _b6Manager,
        MissionType     _missionType,
        uint256         _enrollmentStart,
        uint256         _enrollmentEnd,
        uint256         _enrollmentAmount,
        uint8           _enrollmentMinPlayers,
        uint8           _enrollmentMaxPlayers,
        uint256         _missionStart,
        uint256         _missionEnd,
        uint8           _missionRounds
    ) external payable initializer {
        __Ownable_init(_owner);
        _transferOwnership(_owner);
        b6Manager = B6Manager(payable(_b6Manager));                                                     // Set the B6Manager contract reference

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
        missionData.missionStatus           = Status.Pending;                   // Set initial status to Pending
        missionData.pauseTime               = 0;                                // Initialize pause time to 0
        missionData.players                 = new address[](0);                 // Initialize players array
        missionData.playersWon              = new PlayersWon[](0);              // Initialize playersWon array
        b6Manager.setMissionStatus(address(this), 
            B6Manager.Status(uint8(Status.Pending)));                           // Register mission status in B6Manager
    }

    // ─────────────── External functions ───────────────
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
            _setStatus(Status.Ended);                                                                                // If all rounds are completed, set status to Ended
            _withdrawFunds();                                                                                       // Withdraw funds to B6Manager contract
        }
        else {
            _setStatus(Status.Paused);                                                                               // If not all rounds are completed, set status to Ready for the next round
        }
        
    }

    /**
     * @dev Withdraws funds from the mission contract.
     * This function can be called by the owner or an authorized address.
     */
    function withdrawFunds() external nonReentrant onlyOwnerOrAuthorized {
        _withdrawFunds();                                                                                   // Call internal withdraw function
    }

    /**
     * @dev Refunds players if the mission fails.
     * This function can be called by the owner or an authorized address.
     */
    function refundPlayers() external nonReentrant onlyOwnerOrAuthorized {
        _refundPlayers();                                                                                           // Call internal refund function
    }
    
    /**
     * @dev Returns the B6MissionData structure.
     */
    function getB6MissionData() external view returns (B6MissionData memory) {
        return missionData;
    }

    // ─────────────── Internal functions ───────────────
    /**
     * @dev Sets the status of the mission.
     * @param newStatus The new status to set for the mission.
     */
    function _setStatus(Status newStatus) internal {
        Status oldStatus = missionData.missionStatus;                                   // Store the old status
        missionData.missionStatus = newStatus;                                          // Update the mission status    
        b6Manager.setMissionStatus(address(this), B6Manager.Status(uint8(newStatus)));  // Update the status in B6Manager
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
     * @dev Withdraws funds from the mission contract.
     * Only callable by the owner or an authorized address when the mission is ended or failed.
     */
    function _withdrawFunds() internal {
        require(missionData.missionStatus == Status.Ended || 
                missionData.missionStatus == Status.Failed,                     "Mission not ended");       // Ensure mission is ended
        uint256 balance = address(this).balance;
        require(balance > 0,                                                    "No funds to withdraw");    // Ensure there are funds to withdraw

        // Compute 25% fee
        uint256 fee = balance / 4;                                                                          // equivalent to 25%
        uint256 remainder = balance - fee;                                                                  // the other 75%

        // 1) Pay the fee to the B6Manager's owner
        address payable mgrOwner = payable(b6Manager.owner());                                              // Get the owner of the B6Manager contract  
        (bool ok, ) = mgrOwner.call{ value: fee }("");                                                      // Attempt to transfer the fee to the owner    
        require(ok,                                                             "Owner transfer failed");   // Ensure the transfer was successful    

        // 2) Send the rest to the B6Manager contract
        (ok, ) = payable(address(b6Manager)).call{ value: remainder }("");                                  // Attempt to transfer the remaining funds to the B6Manager contract
        require(ok,                                                             "Manager transfer failed"); // Ensure the transfer to B6Manager contract was successful
        b6Manager.registerMissionFunds(missionData.missionType, remainder);                                 // Register the remaining funds with the B6Manager for the specific mission type
        emit FundsWithdrawn(mgrOwner, fee, remainder);
    }

    /**
     * @dev Refunds players if the mission fails.
     * This function can be called by the owner or an authorized address.
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
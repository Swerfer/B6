// SPDX-License-Identifier: MIT
pragma solidity ^0.8.30;

import "@openzeppelin/contracts-upgradeable/proxy/utils/Initializable.sol";
import "@openzeppelin/contracts-upgradeable/access/OwnableUpgradeable.sol";
import "@openzeppelin/contracts-upgradeable/utils/ReentrancyGuardUpgradeable.sol";
import "./B6Manager.sol";

contract B6Mission is Initializable, OwnableUpgradeable, ReentrancyGuardUpgradeable { 
    // Reference to the B6Manager contract
    B6Manager public b6Manager; 

    struct PlayersWon {
        address player;
        uint256 amountWon;
    }

    // add at contract level, for O(1) “has already won?” checks
    mapping(address => bool) public enrolled;
    mapping(address => bool) public hasWon;
    mapping(address => bool) public refunded;

    struct B6MissionData {
        address[]       players;
        string          missionName;
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

    enum Status {
        Pending,
        Enrolling,
        Ready,
        Active,
        Paused,
        Ended,
        Failed
    }

    // Store mission data in a state variable
    B6MissionData public missionData;

    event RefundFailed(address indexed player, uint256 amount); // Event to log refund failures

    function initialize(
        address         _owner,
        address         _b6Manager,
        string memory   _missionName,
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
        require(msg.value > 0,                      "Insufficient initial funding");                    // Ensure initial funding is sufficient
        require(_b6Manager != address(0),           "Invalid B6Manager address");                       // Ensure B6Manager address is valid
        require(bytes(_missionName).length > 0,     "Mission name cannot be empty");                    // Ensure mission name is not empty
        require(_enrollmentStart < _enrollmentEnd,  "Enrollment start must be before end");             // Ensure enrollment start is before end
        require(_enrollmentAmount > 0,              "Enrollment amount must be greater than 0");        // Ensure enrollment amount is greater than 0
        require(_enrollmentMinPlayers >= 10,        "Enrollment min players must be minimum 10");       // Ensure minimum players is 10
        require(_enrollmentMaxPlayers >= _enrollmentMinPlayers, 
                                "Enrollment max players must be greater than or equal to min players"); // Ensure max players is greater than or equal to min players
        require(_missionStart < _missionEnd,        "Mission start must be before end");                // Ensure mission start is before end
        require(_missionRounds > 0,                 "Mission must have at least one round");            // Ensure mission has at least one round
        b6Manager = B6Manager(payable(_b6Manager));                                                     // Set the B6Manager contract reference

        // Initialize mission data
        missionData.missionName             = _missionName;
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

    // ──────────────── Mission functions ───────────────
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
    }

    /**
     * @dev Starts the mission.
     * Only callable by the owner or an authorized address when the mission is ready.
     */
    function startMission() external nonReentrant {
        require(msg.sender == owner() || b6Manager.isAuthorized(msg.sender),    "Not owner or authorized");         // Ensure only owner or authorized can start the mission);
        require(block.timestamp >= missionData.enrollmentEnd,                   "Enrollment still ongoing");        // Ensure enrollment has ended
        require(block.timestamp >= missionData.missionStart,                    "Mission start time not reached");  // Ensure mission start time has been reached
        if (missionData.players.length < missionData.enrollmentMinPlayers) {
            setStatus(Status.Failed);                                                                               // If not enough players, refund and set status to Failed
            _refundPlayers();
        }
        else
        {
            setStatus(Status.Active);                                                                               // Set mission status to Active
        }
    }

    function callRound() external nonReentrant {
        require(missionData.pauseTime == 0 || block.timestamp >= missionData.pauseTime +
            ((missionData.roundCount + 1 == missionData.missionRounds)
                ? 1 minutes
                : 5 minutes),                                                   "Mission is paused");               // Ensure mission is not paused (5 mins for normal rounds, 1 min for last round)
        require(!hasWon[msg.sender],                                            "Player already won this mission"); // Ensure player has not already won this round
        require(enrolled[msg.sender],                                           "Not enrolled in mission");         // Ensure caller is enrolled in the mission
        require(missionData.missionStatus == Status.Active,                     "Mission is not Active");           // Ensure mission is in Active status
        require(missionData.roundCount < missionData.missionRounds,             "All rounds completed");            // Ensure there are rounds left to play
        require(missionData.missionEnd > block.timestamp,                       "Mission has ended");               // Ensure mission has not ended
        require(missionData.missionEnd > missionData.missionStart,              "Mission start time must be before end time"); // Ensure mission start time is before end time
        if (missionData.missionStatus == Status.Paused) {
            setStatus(Status.Active);                                                                               // If mission was paused, set status to Active
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

        if (missionData.roundCount == missionData.missionRounds) {
            setStatus(Status.Ended);                                                                                // If all rounds are completed, set status to Ended
            _withdrawFunds();                                                                                       // Withdraw funds to B6Manager contract
        }
        else {
            setStatus(Status.Paused);                                                                               // If not all rounds are completed, set status to Ready for the next round
        }
        
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
            } else {
                emit RefundFailed(player, amount);                                                                  // Log the failure, but don’t revert
            }
        }
        _withdrawFunds();
    }

    function refundPlayers() external nonReentrant {
        require(msg.sender == owner() || b6Manager.isAuthorized(msg.sender),    "Not owner or authorized");         // Ensure only owner or authorized can start the mission);
        _refundPlayers();                                                                                           // Call internal refund function
    }

    /**
     * @dev Returns the B6MissionData structure.
     */
    function getB6MissionData() external view returns (B6MissionData memory) {
        return missionData;
    }

    /**
     * @dev Sets the status of the mission.
     * @param newStatus The new status to set for the mission.
     */
    function setStatus(Status newStatus) internal {
        missionData.missionStatus = newStatus;                                          // Update the mission status    
        b6Manager.setMissionStatus(address(this), B6Manager.Status(uint8(newStatus)));  // Update the status in B6Manager
        if (newStatus == Status.Paused) {
            missionData.pauseTime = block.timestamp;                                    // Record the time when the mission was paused
        }
        else if (newStatus == Status.Active) {
            missionData.pauseTime = 0;                                                  // Reset pause time when the mission is active
        }
    }

    /**
     * @dev Withdraws funds from the mission contract.
     * Only callable by the owner or an authorized address when the mission is ended or failed.
     */
    function _withdrawFunds() internal {
        require(missionData.missionStatus == Status.Ended || 
                missionData.missionStatus == Status.Failed,                     "Mission not ended");       // Ensure mission is ended
        uint256 amount = address(this).balance;                                                             // Get the contract's balance
        require(amount > 0, "No funds to withdraw");                                                        // Ensure there are funds to withdraw
        (bool ok,) = address(b6Manager).call{value: amount}("");                                            // Transfer the funds to the B6Manager contract
        require(ok, "Transfer failed");
    }

    /**
     * @dev Withdraws funds from the mission contract.
     * This function can be called by the owner or an authorized address.
     */
    function withDrawFunds() external nonReentrant {
        require(msg.sender == owner() || b6Manager.isAuthorized(msg.sender),    "Not owner or authorized"); // Ensure only owner or authorized can start the mission);
        _withdrawFunds();                                                                                   // Call internal withdraw function
    }
}
// SPDX-License-Identifier: MIT
pragma solidity ^0.8.30;

// ───────────────────── Imports ────────────────────────
import "@openzeppelin/contracts/access/Ownable.sol";
import "@openzeppelin/contracts/proxy/Clones.sol";
import "@openzeppelin/contracts/token/ERC20/utils/SafeERC20.sol";
import "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import "@openzeppelin/contracts/utils/ReentrancyGuard.sol";
import "@openzeppelin/contracts/utils/Strings.sol";

import "./CommonTypes.sol";
import "./B6Mission.sol";

contract B6Manager is Ownable, ReentrancyGuard {
    using SafeERC20 for IERC20;
    using Clones    for address;
    using Strings   for uint256;
    
    // ────────────── State Variables ───────────────────

    mapping(address => bool)    public  authorized;     // Mapping to track authorized addresses
    mapping(MissionType => uint256) public reservedFunds;   // Track funds by type
    mapping(address => Status)      public missionStatus;   // Mapping to hold the status of each mission
    address[]                       public missions;        // Array to hold all mission addresses

    event AuthorizedAddressAdded(address indexed addr);
    event AuthorizedAddressRemoved(address indexed addr);
    event MissionStatusUpdated(address indexed mission, Status status);
    event MissionCreated(address indexed missionAddress);
    event B6ManagerOwnershipTransferred(address indexed newOwner);
    event FundsReceived(address indexed sender, uint256 amount);
    event MissionFundsRegistered(MissionType indexed missionType, uint256 amount);
    event FundsWithdrawn(address indexed to, uint256 amount, uint256 missionType);

    // ────────────────── Modifiers ─────────────────────
    /**
     * @dev Modifier that allows only the owner or an authorized address to call.
     */
    modifier onlyOwnerOrAuthorized() {
        require(
            msg.sender == owner() || authorized[msg.sender],
            "Not owner or B6Manager authorized"
        );
        _;
    }

    constructor() Ownable(msg.sender) {}

    // ────────────────── Functions ─────────────────────
    /**
     * @dev Public entry-point for authorized callers to send ETH.
     */
    function FundManager() external payable onlyOwnerOrAuthorized nonReentrant() {
        _handleFunds();                                                             // Handle incoming funds
    }

    /**
     * @dev Creates a new mission with the specified parameters.
     * @param _owner                The owner of the mission.
     * @param _b6Manager            The address of the B6Manager contract.
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
        address         _owner,                 // Owner of the mission
        address         _b6Manager,             // Address of the B6Manager contract
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
            require(_owner != address(0),                               "Invalid owner address");                                       // Ensure valid addresses are provided
            require(_b6Manager != address(0),                           "Invalid B6Manager address");                                   // Ensure valid addresses are provided
            require(_enrollmentStart < _enrollmentEnd,                  "Enrollment start must be before end");                         // Ensure enrollment start is before end
            require(_enrollmentMinPlayers >= 10,                        "Minimum players must be greater or equal to 10");              // Ensure minimum players is greater than zero
            require(_enrollmentMaxPlayers >= _enrollmentMinPlayers * 2, "Max players must be greater than or equal to min players");    // Ensure max players is at least 2x min players      
            require(_enrollmentAmount > 0,                              "Enrollment amount must be greater than zero");                 // Ensure enrollment amount is greater than zero
            require(_missionStart > _enrollmentEnd,                     "Mission start must be after enrollment end");                  // Ensure mission start is after enrollment end
            require(_missionStart < _missionEnd,                        "Mission start must be before end");                            // Ensure mission start is before end
            require(_missionRounds > 0,                                 "Mission rounds must be greater than zero");                    // Ensure mission rounds is greater than zero
            require(_missionRounds <= 100,                              "Mission rounds must be less than or equal to 100");            // Ensure mission rounds does not exceed 255

            B6Mission mission = new B6Mission();                                        // Create a new mission instance
            mission.initialize(
                _owner,                                                                 // Set the owner of the mission
                _b6Manager,                                                             // Set the B6Manager address
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

        missions.push(address(mission));                                                // Add the new mission to the list of missions
        emit MissionCreated(address(mission));                                          // Emit event for mission creation

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
            (bool sent, ) = payable(address(mission)).call{value: allocation}("");
            require(sent, "Funding failed");
        }

        return address(mission);                                                        // Return the address of the newly created mission
    }

    /**
     * @dev Authorizes an address to perform actions on behalf of the B6Manager.
     * @param account The address to authorize.
     */
    function authorize(address account) external onlyOwnerOrAuthorized() {
        require(account != address(0),  "Invalid address");                         // Ensure the account is valid
        authorized[account] = true;                                                 // Authorize the account
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
        require(status != Status.Pending,                       "Cannot set status to Pending");    // Ensure status is not set to Pending
        missionStatus[missionAddress] = status;                                                     // Update the mission status
    }

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
     * @dev Transfers ownership of the B6Manager contract.
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
        emit B6ManagerOwnershipTransferred(newOwner);                               // Emit event for ownership transfer
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
     * @dev Internal common handler for incoming ETH.
     */
    function _handleFunds() internal {
        require(msg.value > 0, "Must send ETH to fund manager");
        emit FundsReceived(msg.sender, msg.value);
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

    function withdrawFunds(uint256 amount) external onlyOwnerOrAuthorized nonReentrant {
        address mgrOwner = owner();                                                             // Get the owner of the B6Manager contract
        require(mgrOwner != address(0), "Invalid manager owner");                               // Ensure the manager owner is valid
        if (amount == 0) {
            amount = address(this).balance;                                                     // If no amount specified, withdraw all funds
        }
        require(amount <= address(this).balance, "Insufficient balance");                       // Ensure the contract has enough balance to withdraw
        (bool ok, ) = payable(mgrOwner).call{ value: amount }("");                              // Attempt to transfer the specified amount to the manager owner
        require(ok, "Transfer failed");                                                         // Ensure the transfer was successful
        emit FundsWithdrawn(mgrOwner, amount, 0);                                               // Emit event for funds withdrawal
    }
}
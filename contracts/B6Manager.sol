// SPDX-License-Identifier: MIT
pragma solidity ^0.8.30;

// ───────────────────── Imports ────────────────────────
import "@openzeppelin/contracts/access/Ownable.sol";
import "@openzeppelin/contracts/proxy/Clones.sol";
import "@openzeppelin/contracts/token/ERC20/utils/SafeERC20.sol";
import "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import "@openzeppelin/contracts/utils/ReentrancyGuard.sol";
import "@openzeppelin/contracts/utils/Strings.sol"; 

import "./B6Mission.sol";

contract B6Manager is Ownable, ReentrancyGuard {
    using SafeERC20 for IERC20;
    using Clones    for address;
    using Strings   for uint256;

    mapping(address => bool)    public  authorized;     // Mapping to track authorized addresses

    enum Status {                                       // Status of the mission
        Pending,
        Enrolling,
        Ready,
        Active,
        Paused,
        Ended,
        Failed
    }

    mapping(address => Status)  public  missionStatus;  // Mapping to hold the status of each mission
    address[] public missions;                          // Array to hold all mission addresses

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

    function setMissionStatus(address missionAddress, Status status) external onlyOwnerOrAuthorized {
        // Logic to set the status of a mission
        // This function can be used to change the status of the mission
        // For example, from Pending to Enrolling, or from Active to Ended
        missionStatus[missionAddress] = status;
    }

    function isAuthorized(address account) external view returns (bool) {
        return authorized[account];
    }
}
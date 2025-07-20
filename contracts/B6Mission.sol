// SPDX-License-Identifier: MIT
pragma solidity ^0.8.30;

import "@openzeppelin/contracts/proxy/utils/Initializable.sol";
import "@openzeppelin/contracts/access/Ownable.sol";
import "./B6Manager.sol";

contract B6Mission is Initializable { 
    // Reference to the B6Manager contract
    B6Manager public b6Manager; 

    struct B6MissionData {
        address[]       players;
        string          missionName;
        uint256         enrollmentStart;
        uint256         enrollmentEnd;
        uint256         enrollmentAmount;
        uint8           enrollmentMinPlayers;
        uint8           enrollmentMaxPlayers;
        uint256         missionStart;
        uint256         missionDuration;
        uint8           missionRounds;
        uint8           roundCount;
        uint256         ethStart;    
        uint256         ethCurrent;
        uint8           roundProgress;
        address[]       playersWon;
    }

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
        uint256         _missionDuration,
        uint8           _missionRounds
    ) external payable initializer {
        __Ownable_init(_owner); // Initialize Ownable with the contract deployer as the owner
        _transferOwnership(_owner);
        require(_b6Manager != address(0), "B6Manager address cannot be zero");
        b6Manager = B6Manager(payable(_b6Manager));
        
        // Initialize mission data
        B6MissionData memory missionData;
        missionData.missionName             = _missionName;
        missionData.enrollmentStart         = _enrollmentStart;
        missionData.enrollmentEnd           = _enrollmentEnd;
        missionData.enrollmentAmount        = _enrollmentAmount;
        missionData.enrollmentMinPlayers    = _enrollmentMinPlayers;
        missionData.enrollmentMaxPlayers    = _enrollmentMaxPlayers;
        missionData.missionStart            = _missionStart;
        missionData.missionDuration         = _missionDuration;
        missionData.missionRounds           = _missionRounds;

        if (msg.value > 0) {
            (bool success, ) = address(b6Manager).call{value: msg.value}("");
            require(success, "Forwarding creation fee to B6Manager failed");
        }
    }

}
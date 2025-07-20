// SPDX-License-Identifier: MIT
pragma solidity ^0.8.30;

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
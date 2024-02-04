Feature: Basic Event Store Functionality
  Created by henrikbd on 2022-07-18
  This feature tests the basic functionality of an postgres event store.

  Background:
    Given an "mysql" event store is provisioned

  Scenario: Successful append
    Given I create subscription "a"
    When I append 5 events
    Then the event store should contain 5 events
    And subscription "a" should receive 5 events
    When I append another 5 events
    Then subscription "a" should have received 10 events

  Scenario: Append conflict
    When I append 1 event for stream "a"
    And I append an event with version 1 for stream "a"
    Then appending another event with version 1 for stream "a" should fail

  Scenario: Replay
    Given I append 5 events
    And I create subscription "b"
    Then subscription "b" should receive 5 events

  Scenario: Check event payload
    Given a payload of "fixtures/event_payload_small.json"
    When I append an event with the payload
    Then the fetched event should have the equal payload

  Scenario: Check large event payload
    Given a payload of "fixtures/event_payload_large.json"
    When I append an event with the payload
    Then the fetched event should have the equal payload

  Scenario: Out of order timestamps
    Given I create subscription "a"
    When I make concurrent append requests one with 500 and 5 with 1 events
    Then subscription "a" should receive 505 events

  Scenario: Error message
    Given an invalid event
    When the event is appended the call should fail

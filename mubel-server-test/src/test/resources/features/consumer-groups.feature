Feature: Tests for the Consumer Groups Feature
  Created by henrik on 2023-11-17

  Background:
    Given an "inmemory" event store is provisioned

  Scenario: Becoming a leader
    Given clients "one" and "two"
    When client "one" joins consumer group "cg-1"
    Then client "one" should become the leader of consumer group "cg-1"
    When client "two" joins consumer group "cg-1"
    Then client "two" should not become the leader
    When client "one" disconnects
    Then client "two" should become the leader of consumer group "cg-1"

  Scenario: Leader can subscribe
    Given a client joins consumer group "cg-2"
    And the client creates subscription "sub-one" with its consumer group token
    And this test is paused for 11 seconds
    When I append 5 events
    Then subscription "sub-one" should have received 5 events

  Scenario: Non-leaders cannot subscribe
    Given a client has an invalid consumer group token
    When the client creates subscription "sub-one" with its consumer group token
    Then the subscription "sub-one" should fail with message "Subscription rejected: Client is not the leader of the consumer group."

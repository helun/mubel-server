Feature: Tests for the Scheduled Events Feature
  Created by henrik on 2023-11-17
  This feature tests the scheduling and delivery of events to clients.

  Scenario: Base case for scheduled events delivery
    Given the client starts a scheduled events subscription for all categories
    When the client schedules an event for 1 second from now
    Then the event should be published to the client after 1 second

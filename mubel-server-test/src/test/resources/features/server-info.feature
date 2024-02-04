Feature: Test for the Server Info Endpoint
  This feature tests the server info endpoint in a system with an in-memory event store.

  Background:
    Given an "inmemory" event store is provisioned

  Scenario: Server info response validation
    When I request server info
    Then the response should contain the provisioned event store
    And the response should contain a "IN_MEMORY" backend

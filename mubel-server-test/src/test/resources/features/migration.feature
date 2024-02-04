Feature: Tests for the Migration Feature
  Created by henrik on 2023-11-17

  Background:
    Given a event store of type "mysql" called "mysql" is provisioned
    And a event store of type "postgresql" called "pg" is provisioned

  Scenario: Migrate to another backend
    Given the "mysql" event store contains 2000 events
    When I copy "mysql" to "pg"
    And wait for copy job to finish
    Then "pg" event store should contain 2000 events
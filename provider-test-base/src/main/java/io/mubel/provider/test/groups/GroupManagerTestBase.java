/*
 * provider-test-base - Multi Backend Event Log
 * Copyright © 2025 Henrik Barratt-Due (henrikbd@hey.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.mubel.provider.test.groups;

import io.mubel.api.grpc.v1.groups.GroupStatus;
import io.mubel.provider.test.AdjustableClock;
import io.mubel.provider.test.TestSubscriber;
import io.mubel.server.spi.groups.GroupManager;
import io.mubel.server.spi.groups.Heartbeat;
import io.mubel.server.spi.groups.JoinRequest;
import io.mubel.server.spi.groups.LeaveRequest;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

import java.time.Clock;
import java.time.Duration;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
public abstract class GroupManagerTestBase {

    public static final String ESID_1 = "esid-1";

    private final AdjustableClock clock = new AdjustableClock();

    @Test
    void first_joined_becomes_leader() {
        var request = new JoinRequest("esid-1", "group-1", "token-1");
        var status = groupManager().join(request).blockFirst();
        assertThat(status.getLeader()).as("should become leader").isTrue();
        assertThat(status.getGroupId()).as("should have correct group id").isEqualTo("group-1");
        assertThat(status.getToken()).as("should have a token").isNotBlank();
    }

    @Test
    void second_to_join_will_not_become_leader() {
        String groupId = "group-2";
        var request1 = new JoinRequest(ESID_1, groupId, "second-joined-token-2-1");
        var request2 = new JoinRequest(ESID_1, groupId, "second-joined-token-2-2");
        joinAndVerifyLeadership(request1);

        StepVerifier.create(groupManager()
                        .join(request2))
                .expectNextMatches(status -> {
                    assertThat(status.getLeader()).as("second should not become leader").isFalse();
                    return true;
                })
                .thenCancel()
                .verify(Duration.ofSeconds(1));
    }

    @Test
    void new_leader_is_designated_when_current_leader_leaves() {
        String groupId = "group-3";
        var request1 = new JoinRequest(ESID_1, groupId, "new-ldr-token-3-1");
        var request2 = new JoinRequest(ESID_1, groupId, "new-ldr-token-3-2");
        joinAndVerifyLeadership(request1);

        var ts = new TestSubscriber<>(groupManager().join(request2));
        groupManager().leave(new LeaveRequest(request1.groupId(), request1.token()));
        assertBecomesLeader(ts);
    }

    @Test
    void latest_to_join_should_become_leader() {
        String groupId = "group-4";
        var request1 = new JoinRequest(ESID_1, groupId, "ljl-token-4-1");
        var request2 = new JoinRequest(ESID_1, groupId, "ljl-token-4-2");
        var request3 = new JoinRequest(ESID_1, groupId, "ljl-token-4-3");
        joinAndVerifyLeadership(request1);
        clock.tick(Duration.ofSeconds(1));
        var firstCandidate = new TestSubscriber<>(groupManager().join(request2));
        clock.tick(Duration.ofSeconds(1));
        var latestCandidate = new TestSubscriber<>(groupManager().join(request3));
        sendHeartbeat(Heartbeat.from(request2));
        sendHeartbeat(Heartbeat.from(request3));
        groupManager().leave(new LeaveRequest(request1.groupId(), request1.token()));
        firstCandidate.assertNotComplete();
        assertBecomesLeader(latestCandidate);
    }

    private void sendHeartbeat(Heartbeat heartbeat) {
        groupManager().heartbeat(heartbeat);
    }

    @Test
    void leader_must_send_heartbeats() {
        String groupId = "group-5";
        var request1 = new JoinRequest(ESID_1, groupId, "leader-hb-token-5-1");
        var status = groupManager().join(request1).blockFirst();
        assertThat(status.getLeader()).as("should become leader").isTrue();
        var heartbeat = Heartbeat.from(request1);
        var heartbeatInterval = Duration.ofSeconds(status.getHearbeatIntervalSeconds());
        clock.tick(heartbeatInterval);
        sendHeartbeat(heartbeat);
        assertLeadership(status);
        clock.tick(heartbeatInterval);
        groupManager().checkClients();
        clock.tick(heartbeatInterval);
        groupManager().checkClients();
        assertNoLeader(status);
    }

    @Test
    void candiates_must_send_heartbeats() {
        String groupId = "hb-group-6";
        var leaderReq = new JoinRequest(ESID_1, groupId, "cand-hb-token-6-1");
        var status = groupManager().join(leaderReq).blockFirst();
        assertLeadership(status);
        var candidateReq = new JoinRequest(ESID_1, groupId, "cand-hb-token-6-2");
        var candidate = new TestSubscriber<>(groupManager().join(candidateReq));
        var heartbeatInterval = Duration.ofSeconds(status.getHearbeatIntervalSeconds());

        clock.tick(heartbeatInterval);
        sendHeartbeat(new Heartbeat(candidateReq.groupId(), leaderReq.token()));
        sendHeartbeat(new Heartbeat(candidateReq.groupId(), candidateReq.token()));
        groupManager().checkClients();
        candidate.assertNotComplete();

        clock.tick(heartbeatInterval);
        sendHeartbeat(new Heartbeat(candidateReq.groupId(), leaderReq.token()));
        groupManager().checkClients();
        assertLeadership(status);
        candidate.assertNotComplete();

        clock.tick(heartbeatInterval);
        sendHeartbeat(new Heartbeat(candidateReq.groupId(), leaderReq.token()));
        groupManager().checkClients();
        assertLeadership(status);
        await().untilAsserted(candidate::assertComplete);
    }

    private void assertNoLeader(GroupStatus status) {
        await().untilAsserted(() -> {
            Optional<GroupStatus> noLeader = groupManager()
                    .leader(status.getGroupId());
            assertThat(noLeader)
                    .as("leader should be removed when two heartbeats are missed")
                    .isEmpty();
        });
    }

    private void assertLeadership(GroupStatus status) {
        Optional<GroupStatus> leader = groupManager()
                .leader(status.getGroupId());
        assertThat(leader)
                .map(GroupStatus::getToken)
                .as("should remain leader")
                .contains(status.getToken());
    }

    private void joinAndVerifyLeadership(JoinRequest request1) {
        StepVerifier.create(groupManager().join(request1))
                .expectNextMatches(status -> {
                    assertThat(status.getLeader())
                            .as("client with token %s should become leader", request1.token())
                            .isTrue();
                    return true;
                })
                .verifyComplete();
    }

    private static void assertBecomesLeader(TestSubscriber<GroupStatus> ts) {
        ts.awaitDone();
        assertThat(ts.values())
                .last()
                .satisfies(status -> assertThat(status.getLeader()).as("new leader should be designated").isTrue());
    }

    protected Clock clock() {
        return clock;
    }

    protected Duration heartbeatInterval() {
        return Duration.ofSeconds(1);
    }

    protected abstract GroupManager groupManager();
}

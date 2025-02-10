/*
 * mubel-provider-inmemory - Multi Backend Event Log
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
package io.mubel.provider.inmemory.groups;

import io.mubel.api.grpc.v1.groups.GroupStatus;
import io.mubel.server.spi.groups.Heartbeat;
import io.mubel.server.spi.groups.JoinRequest;
import io.mubel.server.spi.groups.LeaveRequest;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.TreeSet;

public class GroupState {

    private final String groupId;
    private final Clock clock;
    private final Duration heartbeatInterval;
    private final NavigableSet<GroupEntry> candidates = new TreeSet<>();
    private GroupEntry leader;

    public GroupState(String groupId, Clock clock, Duration heartbeatInterval) {
        this.groupId = groupId;
        this.clock = clock;
        this.heartbeatInterval = heartbeatInterval;
    }

    public GroupStatus join(JoinRequest request) {
        var groupEntry = new GroupEntry(request.token(), clock.instant());
        var statusBuilder = GroupStatus.newBuilder()
                .setGroupId(request.groupId())
                .setToken(request.token())
                .setHearbeatIntervalSeconds((int) heartbeatInterval.toSeconds());

        if (leader == null) {
            leader = groupEntry;
            return statusBuilder
                    .setLeader(true)
                    .build();
        }
        candidates.add(groupEntry);
        return statusBuilder
                .setLeader(false)
                .build();
    }

    public Optional<GroupStatus> leave(LeaveRequest leaveRequest) {
        if (leader != null && leader.token.equals(leaveRequest.token())) {
            leader = null;
            return appointNewLeader(leaveRequest.groupId());
        } else {
            candidates.removeIf(entry -> entry.token.equals(leaveRequest.token()));
        }
        return Optional.empty();
    }

    private Optional<GroupStatus> appointNewLeader(String groupId) {
        if (!candidates.isEmpty()) {
            leader = candidates.pollFirst();
            return Optional.of(GroupStatus.newBuilder()
                    .setGroupId(groupId)
                    .setToken(leader.token)
                    .setLeader(true)
                    .setHearbeatIntervalSeconds((int) heartbeatInterval.toSeconds())
                    .build());
        }
        return Optional.empty();
    }

    public void heartbeat(Heartbeat heartbeat) {
        if (leader != null && leader.token.equals(heartbeat.token())) {
            leader.lastActivity(clock.instant());
        } else {
            candidates.stream()
                    .filter(entry -> entry.token.equals(heartbeat.token()))
                    .findFirst()
                    .ifPresent(entry -> entry.lastActivity = clock.instant());
        }
    }

    public Optional<GroupStatus> leader() {
        return Optional.ofNullable(leader)
                .map(entry -> GroupStatus.newBuilder()
                        .setGroupId(entry.token)
                        .setToken(entry.token)
                        .setLeader(true)
                        .setHearbeatIntervalSeconds((int) heartbeatInterval.toSeconds())
                        .build()
                );
    }

    public CheckResult checkHeartbeats() {
        var now = clock.instant();
        var result = new CheckResult.Builder(groupId);
        if (leader != null && leader.lastActivity.plus(heartbeatInterval).isBefore(now)) {
            if (leader.missedCheck() > 1) {
                result.addRemovedClient(leader);
                leader = null;
                result.newLeader(appointNewLeader("groupId"));
            }
        }
        var staleCandidates = candidates.stream()
                .filter(entry -> entry.lastActivity.plus(heartbeatInterval).isBefore(now))
                .toList();
        return result
                .addAllRemovedClients(staleCandidates)
                .build();
    }


    public static class GroupEntry implements Comparable<GroupEntry> {

        private static final Comparator<GroupEntry> COMPARATOR = Comparator.comparing(GroupEntry::joined).reversed();

        private final String token;
        private final Instant joined;
        private Instant lastActivity;
        private int missedChecks = 0;

        public GroupEntry(String token, Instant joined) {
            this.token = token;
            this.joined = joined;
            this.lastActivity = joined;
        }

        public Instant joined() {
            return joined;
        }

        @Override
        public int compareTo(GroupEntry other) {
            return COMPARATOR.compare(this, other);
        }

        public int missedCheck() {
            missedChecks++;
            return missedChecks;
        }

        public void lastActivity(Instant instant) {
            lastActivity = instant;
            missedChecks = 0;
        }

        public String token() {
            return token;
        }
    }

}

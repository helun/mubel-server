package io.mubel.provider.inmemory.groups;

import io.mubel.api.grpc.v1.groups.GroupStatus;
import io.mubel.server.spi.groups.Heartbeat;
import io.mubel.server.spi.groups.JoinRequest;
import io.mubel.server.spi.groups.LeaveRequest;

import java.time.Instant;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.TreeSet;

public class GroupState {

    private final int heartbeatIntervalSeconds = 10;
    private final NavigableSet<GroupEntry> candidates = new TreeSet<>();
    private GroupEntry leader;

    public GroupStatus join(JoinRequest request) {
        var groupEntry = new GroupEntry(request.token(), Instant.now());
        var statusBuilder = GroupStatus.newBuilder()
                .setGroupId(request.groupId())
                .setToken(request.token())
                .setHearbeatIntervalSeconds(heartbeatIntervalSeconds);

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
            if (!candidates.isEmpty()) {
                leader = candidates.pollFirst();
                return Optional.of(GroupStatus.newBuilder()
                        .setGroupId(leaveRequest.groupId())
                        .setToken(leader.token)
                        .setLeader(true)
                        .setHearbeatIntervalSeconds(heartbeatIntervalSeconds)
                        .build());
            }
        } else {
            candidates.removeIf(entry -> entry.token.equals(leaveRequest.token()));
        }
        return Optional.empty();
    }

    public void heartbeat(Heartbeat heartbeat) {

    }


    public static class GroupEntry implements Comparable<GroupEntry> {

        private final String token;
        private Instant lastActivity;

        public GroupEntry(String token, Instant lastActivity) {
            this.token = token;
        }

        @Override
        public int compareTo(GroupEntry o) {
            return 0;
        }
    }

}

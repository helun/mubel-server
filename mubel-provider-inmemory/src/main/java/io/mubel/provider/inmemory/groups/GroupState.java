package io.mubel.provider.inmemory.groups;

import io.mubel.api.grpc.v1.groups.GroupStatus;
import io.mubel.server.spi.groups.Heartbeat;
import io.mubel.server.spi.groups.JoinRequest;
import io.mubel.server.spi.groups.LeaveRequest;

import java.time.Instant;
import java.util.NavigableSet;
import java.util.TreeSet;

public class GroupState {

    private final NavigableSet<GroupEntry> candidates = new TreeSet<>();
    private GroupEntry leader;

    public GroupStatus join(JoinRequest request) {
        var groupEntry = new GroupEntry(request.token(), Instant.now());
        if (leader == null) {
            return GroupStatus.newBuilder()
                    .setLeader(true)
                    .setGroupId(request.groupId())
                    .setToken(request.token())
                    .build();
        }
        candidates.add(groupEntry);
        return GroupStatus.newBuilder()
                .setLeader(false)
                .setGroupId(request.groupId())
                .setToken(request.token())
                .build();
    }

    public void leave(LeaveRequest leaveRequest) {

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

package io.mubel.provider.inmemory.groups;

import io.mubel.api.grpc.v1.groups.GroupStatus;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public record CheckResult(Optional<GroupStatus> newLeader, List<GroupStatus> removedClients) {

    public static class Builder {
        private final String groupId;
        private Optional<GroupStatus> newLeader = Optional.empty();
        private List<GroupStatus> removedClients;

        public Builder(String groupId) {
            this.groupId = groupId;
        }

        public Builder newLeader(Optional<GroupStatus> newLeader) {
            this.newLeader = newLeader;
            return this;
        }

        public CheckResult build() {
            return new CheckResult(newLeader, Objects.requireNonNullElseGet(removedClients, List::of));
        }

        public Builder addRemovedClient(GroupState.GroupEntry leader) {
            if (removedClients == null) {
                removedClients = new ArrayList<>(2);
            }
            removedClients.add(mapGroupStatus(leader));
            return this;
        }

        private GroupStatus mapGroupStatus(GroupState.GroupEntry leader) {
            return GroupStatus.newBuilder()
                    .setGroupId(groupId)
                    .setToken(leader.token())
                    .setLeader(false)
                    .build();
        }

        public Builder addAllRemovedClients(List<GroupState.GroupEntry> removed) {
            if (removedClients == null) {
                removedClients = new ArrayList<>(removed.size());
            }
            removed.forEach(this::addRemovedClient);
            return this;
        }
    }
}

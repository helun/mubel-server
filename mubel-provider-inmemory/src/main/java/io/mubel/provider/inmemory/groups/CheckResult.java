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

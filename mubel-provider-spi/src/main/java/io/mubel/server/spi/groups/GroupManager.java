package io.mubel.server.spi.groups;

import io.mubel.api.grpc.v1.groups.GroupStatus;
import reactor.core.publisher.Flux;

import java.util.Optional;

public interface GroupManager {

    Flux<GroupStatus> join(JoinRequest request);

    void leave(LeaveRequest leaveRequest);

    void heartbeat(Heartbeat heartbeat);

    void checkClients();

    Optional<GroupStatus> leader(String groupId);
}

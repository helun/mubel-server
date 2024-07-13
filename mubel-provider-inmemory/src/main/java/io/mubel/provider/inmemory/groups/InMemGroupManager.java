package io.mubel.provider.inmemory.groups;

import io.mubel.api.grpc.v1.groups.GroupStatus;
import io.mubel.server.spi.groups.GroupManager;
import io.mubel.server.spi.groups.GroupRequest;
import io.mubel.server.spi.groups.JoinRequest;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;

import java.util.concurrent.ConcurrentHashMap;

public class InMemGroupManager implements GroupManager {

    private final Sinks.Many<GroupMessage> requestSink = Sinks.many().unicast().onBackpressureBuffer();

    private final ConcurrentHashMap<String, GroupState> groups = new ConcurrentHashMap<>();

    @Override
    public Flux<GroupStatus> join(JoinRequest request) {
        var session = new JoinSession(request);
        requestSink.tryEmitNext(session);
        return session.response();
    }

    public void start() {
        requestSink.asFlux()
                .subscribeOn(Schedulers.single())
                .subscribe(this::handleRequest);
    }

    private void handleRequest(GroupMessage incoming) {
        switch (incoming) {
            case JoinSession session -> {
                var joinRequest = session.joinRequest();
                var groupState = groups.computeIfAbsent(joinRequest.groupId(), key -> new GroupState());
                var status = groupState.join(joinRequest);
                session.next(status);
            }
            case LeaveMessage message -> {
                var groupState = getGroupState(message.leaveRequest());
                if (groupState != null) {
                    groupState.leave(message.leaveRequest());
                }
            }
            case HeartbeatMessage message -> {
                var groupState = getGroupState(message.heartbeat());
                if (groupState != null) {
                    groupState.heartbeat(message.heartbeat());
                }
            }
        }
    }

    private GroupState getGroupState(GroupRequest request) {
        return groups.get(request.groupId());
    }
}

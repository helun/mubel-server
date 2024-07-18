package io.mubel.provider.inmemory.groups;

import io.mubel.api.grpc.v1.groups.GroupStatus;
import io.mubel.server.spi.groups.GroupManager;
import io.mubel.server.spi.groups.GroupRequest;
import io.mubel.server.spi.groups.JoinRequest;
import io.mubel.server.spi.groups.LeaveRequest;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;

import java.time.Clock;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class InMemGroupManager implements GroupManager {

    private final Sinks.Many<GroupMessage> requestSink = Sinks.many().unicast().onBackpressureBuffer();

    private final ConcurrentMap<String, GroupState> groups = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, JoinSession> sessions = new ConcurrentHashMap<>();
    private final Clock clock;

    public InMemGroupManager(Clock clock) {
        this.clock = clock;
        requestSink.asFlux()
                .subscribeOn(Schedulers.single())
                .subscribe(this::handleRequest);
    }

    @Override
    public Flux<GroupStatus> join(JoinRequest request) {
        var session = new JoinSession(request, clock.instant());
        requestSink.tryEmitNext(session);
        return session.response();
    }

    @Override
    public void leave(LeaveRequest leaveRequest) {
        requestSink.tryEmitNext(new LeaveMessage(leaveRequest));
    }

    private void handleRequest(GroupMessage incoming) {
        switch (incoming) {
            case JoinSession session -> {
                var joinRequest = session.joinRequest();
                var groupState = groups.computeIfAbsent(joinRequest.groupId(), key -> new GroupState(clock));
                var status = groupState.join(joinRequest);
                session.next(status);
                if (status.getLeader()) {
                    session.complete();
                } else {
                    sessions.putIfAbsent(session.joinRequest().token(), session);
                }
            }
            case LeaveMessage message -> {
                sessions.remove(message.leaveRequest().token());
                var groupState = getGroupState(message.leaveRequest());
                if (groupState != null) {
                    groupState.leave(message.leaveRequest()).ifPresent(newLeader -> {
                        var session = sessions.get(newLeader.getToken());
                        if (session != null) {
                            session.next(newLeader);
                            session.complete();
                            sessions.remove(newLeader.getToken());
                        }
                    });
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

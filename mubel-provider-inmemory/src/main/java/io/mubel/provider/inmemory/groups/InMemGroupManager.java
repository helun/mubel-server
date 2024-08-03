package io.mubel.provider.inmemory.groups;

import io.mubel.api.grpc.v1.groups.GroupStatus;
import io.mubel.server.spi.groups.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;

import java.time.Clock;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class InMemGroupManager implements GroupManager {

    private static final Logger LOG = LoggerFactory.getLogger(InMemGroupManager.class);

    private final Duration heartbeatInterval;

    private final Sinks.Many<GroupMessage> requestSink = Sinks.many().unicast().onBackpressureBuffer();

    private final ConcurrentMap<String, GroupState> groups = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, JoinSession> sessions = new ConcurrentHashMap<>();
    private final Clock clock;

    public InMemGroupManager(Clock clock, Duration heartbeatInterval) {
        this.clock = clock;
        this.heartbeatInterval = heartbeatInterval;
        requestSink.asFlux()
                .subscribeOn(Schedulers.single())
                .subscribe(this::handleRequest, this::handleError);
        Flux.interval(Duration.ofSeconds(1))
                .map(tick -> new CheckGroupsMessage())
                .subscribe(requestSink::tryEmitNext);
    }

    private void handleError(Throwable throwable) {
        LOG.error("Error in group manager", throwable);
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

    @Override
    public void heartbeat(Heartbeat heartbeat) {
        requestSink.tryEmitNext(new HeartbeatMessage(heartbeat));
    }

    @Override
    public Optional<GroupStatus> leader(String groupId) {
        var query = new LeaderQuery(groupId);
        requestSink.tryEmitNext(query);
        return query.response();
    }

    @Override
    public void checkClients() {
        requestSink.tryEmitNext(new CheckGroupsMessage());
    }

    private void handleRequest(GroupMessage incoming) {
        switch (incoming) {
            case JoinSession session -> handleJoin(session);
            case LeaveMessage message -> handleLeave(message);
            case HeartbeatMessage message -> handleHeartbeat(message);
            case LeaderQuery query -> handleLeaderQuery(query);
            case CheckGroupsMessage chk -> checkGroups();
        }
    }

    private void checkGroups() {
        groups.values()
                .stream()
                .map(GroupState::checkHeartbeats)
                .forEach(checkResult -> {
                    checkResult.newLeader().ifPresent(this::promote);
                    checkResult.removedClients().forEach(removed -> {
                        var session = sessions.remove(removed.getToken());
                        if (session != null) {
                            session.complete();
                        }
                    });
                });
    }

    private void promote(GroupStatus newLeader) {
        var session = sessions.get(newLeader.getToken());
        if (session != null) {
            session.next(newLeader);
            session.complete();
            sessions.remove(newLeader.getToken());
        }
    }

    private void handleJoin(JoinSession session) {
        LOG.debug("handling join request: {}", session.joinRequest());
        var joinRequest = session.joinRequest();
        var groupState = groups.computeIfAbsent(joinRequest.groupId(), key -> createGroupState(joinRequest.groupId()));
        var status = groupState.join(joinRequest);
        LOG.debug("join reply: {}", status);
        session.next(status);
        if (status.getLeader()) {
            session.complete();
        } else {
            sessions.putIfAbsent(session.joinRequest().token(), session);
        }
    }

    private GroupState createGroupState(String groupId) {
        return new GroupState(groupId, clock, heartbeatInterval);
    }

    private void handleLeave(LeaveMessage message) {
        LOG.debug("handling leave request: {}", message.leaveRequest());
        sessions.remove(message.leaveRequest().token());
        var groupState = getGroupState(message.leaveRequest());
        if (groupState != null) {
            groupState.leave(message.leaveRequest()).ifPresent(this::promote);
        }
        LOG.debug("leave request handled: {}", message.leaveRequest());
    }

    private void handleHeartbeat(HeartbeatMessage message) {
        var groupState = getGroupState(message.heartbeat());
        if (groupState != null) {
            groupState.heartbeat(message.heartbeat());
        }
        LOG.debug("heartbeat handled: {}", message.heartbeat());
    }

    private void handleLeaderQuery(LeaderQuery query) {
        LOG.debug("handling leader query: {}", query.groupId());
        var groupState = groups.get(query.groupId());
        if (groupState != null) {
            groupState.leader().ifPresentOrElse(query::result, query::empty);
        } else {
            query.empty();
        }
    }

    private GroupState getGroupState(GroupRequest request) {
        return groups.get(request.groupId());
    }
}

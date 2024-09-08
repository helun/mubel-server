package io.mubel.provider.jdbc.groups;

import io.mubel.api.grpc.v1.groups.GroupStatus;
import io.mubel.provider.jdbc.topic.Topic;
import io.mubel.server.spi.groups.*;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class JdbcGroupManager implements GroupManager, LeaderQueries, ApplicationRunner {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcGroupManager.class);

    private final Clock clock;
    private final Jdbi jdbi;
    private final Topic groupsTopic;

    private final ConcurrentMap<String, JoinSession> sessions = new ConcurrentHashMap<>();
    private final Duration heartbeatInterval;
    private final Scheduler scheduler;
    private final GroupManagerOperations operations;

    public JdbcGroupManager(Builder b) {
        this.jdbi = b.jdbi;
        this.groupsTopic = b.groupsTopic;
        this.heartbeatInterval = b.heartbeatInterval;
        this.clock = b.clock;
        this.scheduler = b.scheduler;
        this.operations = b.operations;
    }

    public static JdbcGroupManager.Builder builder() {
        return new Builder();
    }

    public void start() {
        groupsTopic.consumer()
                .subscribeOn(scheduler)
                .doOnSubscribe(sub -> {
                    LOG.info("subscribed to group messages");
                    checkClients();
                })
                .subscribe(this::handleGroupMessage);
    }

    @Override
    public Flux<GroupStatus> join(JoinRequest request) {
        LOG.debug("received join request: {}", request);
        var status = jdbi.inTransaction(h -> {
            insertSession(request, h);
            tryInsertLeader(request, h);
            var leaderToken = operations.leader(request.groupId(), h)
                    .map(GroupStatus::getToken)
                    .orElseThrow();
            LOG.debug("leader token: {}", leaderToken);
            return GroupStatus.newBuilder()
                    .setGroupId(request.groupId())
                    .setToken(request.token())
                    .setLeader(leaderToken.equals(request.token()))
                    .setHearbeatIntervalSeconds((int) heartbeatInterval.getSeconds())
                    .build();
        });
        if (status.getLeader()) {
            LOG.debug("token: {} is leader of group: {}", request.token(), request.groupId());
            return Flux.just(status);
        }
        var session = new JoinSession(request, Instant.now());
        sessions.put(request.token(), session);
        session.next(status);
        return session.response();
    }

    @Override
    public void leave(LeaveRequest leaveRequest) {
        LOG.info("received leave request: {}", leaveRequest);
        completeJoinSession(leaveRequest.token());
        jdbi.useTransaction(h -> {
            deleteSession(leaveRequest, h);
            tryDeleteLeadership(leaveRequest.token(), h)
                    .ifPresentOrElse(leader -> appointNewLeader(leader, h),
                            () -> LOG.debug("No leader to appoint")
                    );
        });
    }


    @Override
    public void heartbeat(Heartbeat heartbeat) {
        jdbi.useHandle(h -> operations.updateHeartbeat(heartbeat.token(), clock.instant(), h));
    }

    @Override
    public void checkClients() {
        LOG.debug("Checking clients");
        jdbi.useTransaction(h ->
                deleteExpiredSessions(h)
                        .forEach(removedToken -> {
                            LOG.debug("Removing expired session: {}", removedToken);
                            completeJoinSession(removedToken);
                            tryDeleteLeadership(removedToken, h)
                                    .ifPresent(leader -> appointNewLeader(leader, h));
                        })
        );
    }

    @Override
    public Optional<GroupStatus> leader(String groupId) {
        return jdbi.withHandle(h -> operations.leader(groupId, h));
    }

    @Override
    public boolean isLeader(String token) {
        return jdbi.withHandle(h -> operations.isLeader(token, h));
    }

    @Override
    public void run(ApplicationArguments args) {
        start();
    }

    private void handleGroupMessage(String message) {
        LOG.debug("Received group message: {}", message);
        var parts = message.split(":", 2);
        switch (parts[0]) {
            case "leave" -> checkSessionsForLeave(parts[1]);
            case "leader" -> checkSessionsForLeader(parts[1]);
        }
    }

    private void checkSessionsForLeader(String token) {
        Optional.ofNullable(sessions.get(token))
                .ifPresent(session -> {
                    var status = GroupStatus.newBuilder()
                            .setGroupId(session.joinRequest().groupId())
                            .setToken(token)
                            .setLeader(true)
                            .setHearbeatIntervalSeconds((int) heartbeatInterval.getSeconds())
                            .build();
                    session.next(status);
                    session.complete();
                });
    }

    private void checkSessionsForLeave(String token) {
        Optional.ofNullable(sessions.remove(token))
                .ifPresent(JoinSession::complete);
    }

    private void tryInsertLeader(JoinRequest request, Handle h) {
        operations.insertLeader(request, h);
    }

    private void insertSession(JoinRequest request, Handle h) {
        operations.insertSession(request, h);
    }

    private void completeJoinSession(String token) {
        Optional.ofNullable(sessions.remove(token))
                .ifPresentOrElse(removed -> {
                    removed.complete();
                    LOG.debug("Join session completed: {}", token);
                }, () -> publishClientLeft(token));
    }

    private void publishClientLeft(String token) {
        groupsTopic.publish("leave:" + token);
    }

    private void appointNewLeader(GroupLeader leader, Handle h) {
        LOG.debug("Appointing new leader for group: {}", leader.groupId);
        Instant maxLastSeen = clock.instant().minus(heartbeatInterval);
        operations.appointNewLeader(leader.groupId, maxLastSeen, h)
                .map(token -> {
                    LOG.debug("New leader token: {}", token);
                    leader.token = token;
                    return leader;
                }).ifPresentOrElse(
                        this::publishLeader,
                        () -> LOG.debug("No new leader found")
                );
    }

    private void publishLeader(GroupLeader newLeader) {
        LOG.debug("Publishing new leader: {}", newLeader.token);
        groupsTopic.publish("leader:" + newLeader.token);
    }

    private void deleteSession(LeaveRequest leaveRequest, Handle h) {
        operations.deleteSession(leaveRequest, h);
    }

    private Optional<GroupLeader> tryDeleteLeadership(String token, Handle h) {
        return operations.tryDeleteLeadership(token, h);
    }

    private List<String> deleteExpiredSessions(Handle h) {
        return operations.deleteExpiredSessions(h, clock.instant().minus(heartbeatInterval));
    }

    public static class GroupLeader {
        final String groupId;
        String token;

        public GroupLeader(String groupId) {
            this.groupId = groupId;
        }
    }

    public static class Builder {
        private Jdbi jdbi;
        private Topic groupsTopic;
        private Duration heartbeatInterval;
        private Clock clock;
        private Scheduler scheduler;
        private GroupManagerOperations operations;

        public Builder jdbi(Jdbi jdbi) {
            this.jdbi = jdbi;
            return this;
        }

        public Builder topic(Topic groupsTopic) {
            this.groupsTopic = groupsTopic;
            return this;
        }

        public Builder heartbeatInterval(Duration heartbeatInterval) {
            this.heartbeatInterval = heartbeatInterval;
            return this;
        }

        public Builder clock(Clock clock) {
            this.clock = clock;
            return this;
        }

        public Builder scheduler(Scheduler scheduler) {
            this.scheduler = scheduler;
            return this;
        }

        public Builder operations(GroupManagerOperations operations) {
            this.operations = operations;
            return this;
        }

        public JdbcGroupManager build() {
            return new JdbcGroupManager(this);
        }
    }

}

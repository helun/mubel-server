package io.mubel.provider.jdbc.groups;

import io.mubel.api.grpc.v1.groups.GroupStatus;
import io.mubel.provider.jdbc.topic.Topic;
import io.mubel.server.spi.groups.*;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class JdbcGroupManager implements GroupManager, LeaderQueries {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcGroupManager.class);

    private final Clock clock;
    private final Jdbi jdbi;
    private final Topic groupsTopic;

    private final ConcurrentMap<String, JoinSession> sessions = new ConcurrentHashMap<>();
    private final Duration heartbeatInterval;
    private final Scheduler scheduler;

    public JdbcGroupManager(
            Jdbi jdbi,
            Topic groupsTopic,
            Duration heartbeatInterval,
            Clock clock,
            Scheduler scheduler) {
        this.jdbi = jdbi;
        this.groupsTopic = groupsTopic;
        this.heartbeatInterval = heartbeatInterval;
        this.clock = clock;
        this.scheduler = scheduler;
    }

    public void start() {
        groupsTopic.consumer()
                .subscribeOn(scheduler)
                .subscribe(this::handleGroupMessage);
    }

    @Override
    public Flux<GroupStatus> join(JoinRequest request) {
        LOG.debug("Received join request: {}", request);
        var status = jdbi.inTransaction(h -> {
            insertSession(request, h);
            tryInsertLeader(request, h);
            var token = getLeaderToken(request, h);
            return GroupStatus.newBuilder()
                    .setGroupId(request.groupId())
                    .setToken(request.token())
                    .setLeader(token.equals(request.token()))
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
        LOG.info("Received leave request: {}", leaveRequest);
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
        jdbi.useHandle(h -> {
            h.createUpdate("UPDATE group_session SET last_seen = ? WHERE token = ?")
                    .bind(0, clock.instant())
                    .bind(1, heartbeat.token())
                    .execute();
        });
    }

    @Override
    public void checkClients() {
        jdbi.useTransaction(h -> {
            deleteExpiredSessions(h)
                    .forEach(removedToken -> {
                        completeJoinSession(removedToken);
                        tryDeleteLeadership(removedToken, h)
                                .ifPresent(leader -> appointNewLeader(leader, h));
                    });
        });
    }

    @Override
    public Optional<GroupStatus> leader(String groupId) {
        return jdbi.withHandle(h -> h.createQuery("SELECT group_id, token FROM group_leader WHERE group_id = ?")
                .bind(0, groupId)
                .map(rowView -> GroupStatus.newBuilder()
                        .setGroupId(rowView.getColumn("group_id", String.class))
                        .setToken(rowView.getColumn("token", String.class))
                        .setLeader(true)
                        .build())
                .findOne()
        );
    }

    @Override
    public boolean isLeader(String token) {
        return jdbi.withHandle(h -> h.createQuery("SELECT COUNT(*) FROM group_leader WHERE token = ?")
                .bind(0, token)
                .mapTo(Integer.class)
                .one() > 0
        );
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


    private static String getLeaderToken(JoinRequest request, Handle h) {
        return h.createQuery("SELECT token FROM group_leader WHERE group_id = ?")
                .bind(0, request.groupId())
                .mapTo(String.class)
                .one();
    }

    private static void tryInsertLeader(JoinRequest request, Handle h) {
        h.createUpdate("INSERT INTO group_leader(group_id, token) VALUES (:group, :token) ON CONFLICT DO NOTHING")
                .bind("group", request.groupId())
                .bind("token", request.token())
                .execute();
    }

    private static void insertSession(JoinRequest request, Handle h) {
        h.createUpdate("INSERT INTO group_session (group_id, token, joined_at, last_seen) VALUES (:group, :token, :timestamp, :timestamp)")
                .bind("group", request.groupId())
                .bind("token", request.token())
                .bind("timestamp", Instant.now())
                .execute();
    }

    private void completeJoinSession(String token) {
        Optional.ofNullable(sessions.remove(token))
                .ifPresentOrElse(JoinSession::complete, () -> publishClientLeft(token));
    }

    private void publishClientLeft(String token) {
        groupsTopic.publish("leave:" + token);
    }

    private void appointNewLeader(GroupLeader leader, Handle h) {
        LOG.debug("Appointing new leader for group: {}", leader.groupId);
        h.createQuery("""
                        INSERT INTO group_leader (group_id, token)
                         SELECT group_id, token
                         FROM group_session
                         WHERE group_id = ?
                           AND last_seen >= ?
                         ORDER BY joined_at DESC
                         LIMIT 1 RETURNING token
                        """)
                .bind(0, leader.groupId)
                .bind(1, clock.instant().minus(heartbeatInterval))
                .mapTo(String.class)
                .findOne()
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

    private static void deleteSession(LeaveRequest leaveRequest, Handle h) {
        h.createUpdate("DELETE FROM group_session WHERE token = ?")
                .bind(0, leaveRequest.token())
                .execute();
    }

    private static Optional<GroupLeader> tryDeleteLeadership(String token, Handle h) {
        return h.createQuery("DELETE FROM group_leader WHERE token = :token RETURNING group_id")
                .bind("token", token)
                .map(rowView -> new GroupLeader(rowView.getColumn("group_id", String.class)))
                .findOne();
    }


    private List<String> deleteExpiredSessions(Handle h) {
        return h.createQuery("DELETE FROM group_session WHERE last_seen < ? RETURNING token")
                .bind(0, clock.instant().minus(heartbeatInterval))
                .mapTo(String.class)
                .list();
    }

    private static class GroupLeader {
        final String groupId;
        String token;

        public GroupLeader(String groupId) {
            this.groupId = groupId;
        }
    }
}

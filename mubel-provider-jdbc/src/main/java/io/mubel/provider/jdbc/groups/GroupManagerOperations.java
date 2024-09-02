package io.mubel.provider.jdbc.groups;

import io.mubel.api.grpc.v1.groups.GroupStatus;
import io.mubel.server.spi.groups.JoinRequest;
import io.mubel.server.spi.groups.LeaveRequest;
import org.jdbi.v3.core.Handle;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

public interface GroupManagerOperations {

    default void insertSession(JoinRequest request, Handle h) {
        h.createUpdate("INSERT INTO group_session (group_id, token, joined_at, last_seen) VALUES (:group, :token, :timestamp, :timestamp)")
                .bind("group", request.groupId())
                .bind("token", request.token())
                .bind("timestamp", Instant.now())
                .execute();
    }

    void insertLeader(JoinRequest request, Handle h);

    default Optional<GroupStatus> leader(String groupId, Handle h) {
        return h.createQuery("SELECT group_id, token FROM group_leader WHERE group_id = ?")
                .bind(0, groupId)
                .map(rowView -> GroupStatus.newBuilder()
                        .setGroupId(rowView.getColumn("group_id", String.class))
                        .setToken(rowView.getColumn("token", String.class))
                        .setLeader(true)
                        .build())
                .findOne();
    }

    default boolean isLeader(String token, Handle h) {
        return h.createQuery("SELECT COUNT(*) FROM group_leader WHERE token = ?")
                .bind(0, token)
                .mapTo(Integer.class)
                .one() > 0;
    }

    default void updateHeartbeat(String token, Instant timestamp, Handle h) {
        h.createUpdate("UPDATE group_session SET last_seen = ? WHERE token = ?")
                .bind(0, timestamp)
                .bind(1, token)
                .execute();
    }

    Optional<String> appointNewLeader(String groupId, Instant deadline, Handle h);

    default void deleteSession(LeaveRequest leaveRequest, Handle h) {
        h.createUpdate("DELETE FROM group_session WHERE token = ?")
                .bind(0, leaveRequest.token())
                .execute();
    }

    Optional<JdbcGroupManager.GroupLeader> tryDeleteLeadership(String token, Handle h);

    List<String> deleteExpiredSessions(Handle h, Instant deadline);

}

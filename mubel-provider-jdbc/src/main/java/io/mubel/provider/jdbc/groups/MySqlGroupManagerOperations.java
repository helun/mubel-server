/*
 * mubel-provider-jdbc - mubel-provider-jdbc
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
package io.mubel.provider.jdbc.groups;

import io.mubel.api.grpc.v1.groups.GroupStatus;
import io.mubel.server.spi.groups.JoinRequest;
import org.jdbi.v3.core.Handle;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

public class MySqlGroupManagerOperations implements GroupManagerOperations {

    @Override
    public void insertLeader(JoinRequest request, Handle h) {
        h.createUpdate("INSERT IGNORE INTO group_leader(group_id, token) VALUES (:group, :token)")
                .bind("group", request.groupId())
                .bind("token", request.token())
                .execute();
    }

    @Override
    public Optional<String> appointNewLeader(String groupId, Instant deadline, Handle h) {
        h.createUpdate("""
                        INSERT INTO group_leader (group_id, token)
                                                SELECT group_id, token
                                                FROM (
                                                    SELECT group_id, token
                                                    FROM group_session
                                                    WHERE group_id = ?
                                                      AND last_seen >= ?
                                                    ORDER BY joined_at DESC
                                                    LIMIT 1
                                                ) AS subquery;
                        """)
                .bind(0, groupId)
                .bind(1, deadline)
                .execute();
        return leader(groupId, h)
                .map(GroupStatus::getToken);
    }

    @Override
    public Optional<JdbcGroupManager.GroupLeader> tryDeleteLeadership(String token, Handle h) {
        return h.createQuery("SELECT group_id FROM group_leader WHERE token = :token")
                .bind("token", token)
                .map(rowView -> new JdbcGroupManager.GroupLeader(rowView.getColumn("group_id", String.class)))
                .findOne()
                .map(groupLeader -> {
                    h.createUpdate("DELETE FROM group_leader WHERE token = :token")
                            .bind("token", token)
                            .execute();
                    return groupLeader;
                });
    }

    @Override
    public List<String> deleteExpiredSessions(Handle h, Instant deadline) {
        var exipiredTokens = h.createQuery("SELECT token FROM group_session WHERE last_seen < ?")
                .bind(0, deadline)
                .mapTo(String.class)
                .list();
        if (!exipiredTokens.isEmpty()) {
            h.createUpdate("DELETE FROM group_session WHERE token IN (<tokens>)")
                    .bindList("tokens", exipiredTokens)
                    .execute();
        }
        return exipiredTokens;
    }
}

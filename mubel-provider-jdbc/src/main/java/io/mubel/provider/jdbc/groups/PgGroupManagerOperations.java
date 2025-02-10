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

import io.mubel.server.spi.groups.JoinRequest;
import org.jdbi.v3.core.Handle;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

public final class PgGroupManagerOperations implements GroupManagerOperations {

    public PgGroupManagerOperations() {
    }

    @Override
    public void insertLeader(JoinRequest request, Handle h) {
        h.createUpdate("INSERT INTO group_leader(group_id, token) VALUES (:group, :token) ON CONFLICT DO NOTHING")
                .bind("group", request.groupId())
                .bind("token", request.token())
                .execute();
    }

    @Override
    public Optional<String> appointNewLeader(String groupId, Instant deadline, Handle h) {
        return h.createQuery("""
                        INSERT INTO group_leader (group_id, token)
                         SELECT group_id, token
                         FROM group_session
                         WHERE group_id = ?
                           AND last_seen >= ?
                         ORDER BY joined_at DESC
                         LIMIT 1 RETURNING token
                        """)
                .bind(0, groupId)
                .bind(1, deadline)
                .mapTo(String.class)
                .findOne();
    }

    @Override
    public Optional<JdbcGroupManager.GroupLeader> tryDeleteLeadership(String token, Handle h) {
        return h.createQuery("DELETE FROM group_leader WHERE token = :token RETURNING group_id")
                .bind("token", token)
                .map(rowView -> new JdbcGroupManager.GroupLeader(rowView.getColumn("group_id", String.class)))
                .findOne();
    }

    @Override
    public List<String> deleteExpiredSessions(Handle h, Instant deadline) {
        return h.createQuery("DELETE FROM group_session WHERE last_seen < ? RETURNING token")
                .bind(0, deadline)
                .mapTo(String.class)
                .list();
    }
}

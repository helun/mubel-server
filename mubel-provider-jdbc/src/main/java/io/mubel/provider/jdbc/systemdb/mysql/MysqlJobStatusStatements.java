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
package io.mubel.provider.jdbc.systemdb.mysql;

import io.mubel.provider.jdbc.systemdb.JobStatusStatements;

public class MysqlJobStatusStatements extends JobStatusStatements {

    @Override
    public String upsert() {
        return """
                INSERT INTO job_status (job_id, state, description, progress, updated_at, created_at, problem_type, problem_title, problem_status, problem_detail)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON DUPLICATE KEY UPDATE
                    state = VALUES(state),
                    description = VALUES(description),
                    progress = VALUES(progress),
                    updated_at = VALUES(updated_at),
                    problem_type = VALUES(problem_type),
                    problem_title = VALUES(problem_title),
                    problem_status = VALUES(problem_status),
                    problem_detail = VALUES(problem_detail)
                    """;
    }
}

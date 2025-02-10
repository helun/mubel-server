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
package io.mubel.provider.jdbc.systemdb.pg;

import io.mubel.provider.jdbc.systemdb.JobStatusStatements;

public class PgJobStatusStatements extends JobStatusStatements {
    @Override
    public String upsert() {
        return """
                INSERT INTO job_status (job_id, state, description, progress, updated_at, created_at, problem_type, problem_title, problem_status, problem_detail)
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (job_id) DO UPDATE SET
                    state = EXCLUDED.state,
                    description = EXCLUDED.description,
                    progress = EXCLUDED.progress,
                    updated_at = EXCLUDED.updated_at,
                    problem_type = EXCLUDED.problem_type,
                    problem_title = EXCLUDED.problem_title,
                    problem_status = EXCLUDED.problem_status,
                    problem_detail = EXCLUDED.problem_detail
                """;
    }
}

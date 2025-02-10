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
package io.mubel.provider.jdbc.systemdb;

import io.mubel.provider.jdbc.support.RepositoryStatements;

public abstract class JobStatusStatements implements RepositoryStatements {

    private static final String SELECT = "SELECT job_id, state, description, progress,  created_at, updated_at, problem_type, problem_title, problem_status, problem_detail FROM job_status ";

    @Override
    public String selectAll() {
        return SELECT + "ORDER BY updated_at ASC";
    }

    @Override
    public String select() {
        return SELECT + "WHERE job_id = ?";
    }

    @Override
    public String delete() {
        return "DELETE FROM job_status WHERE job_id = ?";
    }

    @Override
    public String exists() {
        return "SELECT EXISTS(SELECT 1 FROM job_status WHERE job_id = ?)";
    }
}

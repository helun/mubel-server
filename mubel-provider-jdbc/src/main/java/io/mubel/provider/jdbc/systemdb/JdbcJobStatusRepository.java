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

import io.mubel.api.grpc.v1.server.JobStatus;
import io.mubel.provider.jdbc.support.CrudRepositoryBase;
import io.mubel.provider.jdbc.support.RepositoryStatements;
import io.mubel.server.spi.systemdb.JobStatusRepository;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.statement.Update;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Types;

public class JdbcJobStatusRepository extends CrudRepositoryBase<JobStatus> implements JobStatusRepository {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcJobStatusRepository.class);

    public JdbcJobStatusRepository(Jdbi jdbi, RepositoryStatements statements) {
        super(jdbi, statements, JobStatus.class);
    }

    @Override
    protected Update bind(JobStatus value, Update update) {
        var binded = update.bind(0, value.getJobId())
                .bind(1, value.getState())
                .bind(2, value.getDescription())
                .bind(3, value.getProgress())
                .bind(4, value.getUpdatedAt())
                .bind(5, value.getCreatedAt());
        if (value.hasProblem()) {
            var p = value.getProblem();
            return binded.bind(6, p.getType())
                    .bind(7, p.getTitle())
                    .bind(8, p.getStatus())
                    .bind(9, p.getDetail());
        } else {
            return binded.bindNull(6, Types.VARCHAR)
                    .bindNull(7, Types.VARCHAR)
                    .bindNull(8, Types.SMALLINT)
                    .bindNull(9, Types.VARCHAR);
        }
    }
}

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

import io.mubel.api.grpc.v1.common.ProblemDetail;
import io.mubel.api.grpc.v1.server.JobState;
import io.mubel.api.grpc.v1.server.JobStatus;
import org.jdbi.v3.core.mapper.RowViewMapper;
import org.jdbi.v3.core.result.RowView;

import java.sql.SQLException;

public class JobStatusRowMapper implements RowViewMapper<JobStatus> {

    @Override
    public JobStatus map(RowView rowView) throws SQLException {
        var b = JobStatus.newBuilder()
                .setJobId(rowView.getColumn(1, String.class))
                .setState(rowView.getColumn(2, JobState.class))
                .setDescription(rowView.getColumn(3, String.class))
                .setProgress(rowView.getColumn(4, Integer.class))
                .setUpdatedAt(rowView.getColumn(5, Long.class))
                .setCreatedAt(rowView.getColumn(6, Long.class));


        var problemType = rowView.getColumn(7, String.class);
        if (problemType != null) {
            b.setProblem(
                    ProblemDetail.newBuilder()
                            .setType(problemType)
                            .setTitle(rowView.getColumn(8, String.class))
                            .setStatus(rowView.getColumn(9, Integer.class))
                            .setDetail(rowView.getColumn(10, String.class))
                            .build()
            );
        }
        return b.build();
    }
}

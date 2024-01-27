package io.mubel.provider.jdbc.systemdb;

import io.mubel.api.grpc.JobState;
import io.mubel.api.grpc.JobStatus;
import io.mubel.api.grpc.ProblemDetail;
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

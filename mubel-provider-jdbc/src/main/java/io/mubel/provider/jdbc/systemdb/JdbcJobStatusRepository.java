package io.mubel.provider.jdbc.systemdb;

import io.mubel.api.grpc.JobStatus;
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
        LOG.debug("Binding job status: {}", value);
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

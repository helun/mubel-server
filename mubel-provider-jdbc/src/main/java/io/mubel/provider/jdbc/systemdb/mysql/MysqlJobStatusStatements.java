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

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

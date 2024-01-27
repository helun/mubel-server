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

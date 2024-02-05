package io.mubel.provider.test.systemdb;

import io.mubel.api.grpc.JobState;
import io.mubel.api.grpc.JobStatus;
import io.mubel.api.grpc.ProblemDetail;
import io.mubel.server.spi.exceptions.ResourceNotFoundException;
import io.mubel.server.spi.systemdb.JobStatusRepository;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
public abstract class JobStatusRepositoryTestBase {

    protected abstract JobStatusRepository repository();

    @Test
    void crud_without_problem_detail() {
        var jobStatus = getJobStatus();
        assertThat(repository().put(jobStatus)).isEqualTo(jobStatus);
        assertThat(repository().exists(jobStatus.getJobId())).isTrue();
        assertThat(repository().get(jobStatus.getJobId())).isEqualTo(jobStatus);
        repository().remove(jobStatus.getJobId());
        assertThat(repository().find(jobStatus.getJobId())).isEmpty();
    }

    @Test
    void crud_with_problem_detail() {
        var jobStatus = getJobStatusWithProblem();
        assertThat(repository().put(jobStatus)).isEqualTo(jobStatus);
        assertThat(repository().get(jobStatus.getJobId())).isEqualTo(jobStatus);
    }

    @Test
    void when_entity_exists_then_put_updates_existing_entity() {
        var jobStatus = getJobStatus();
        assertThat(repository().put(jobStatus)).isEqualTo(jobStatus);
        var updatedJobStatus = jobStatus.toBuilder()
                .setProgress(100)
                .setState(JobState.COMPLETED)
                .build();
        assertThat(repository().put(updatedJobStatus)).isEqualTo(updatedJobStatus);
        assertThat(repository().get(jobStatus.getJobId())).isEqualTo(updatedJobStatus);
    }

    @Test
    void get_non_existing_throws_ResourceNotFoundException() {
        assertThatThrownBy(() -> repository().get("missing"))
                .isInstanceOf(ResourceNotFoundException.class);
    }

    JobStatus getJobStatus() {
        return JobStatus.newBuilder()
                .setJobId(UUID.randomUUID().toString())
                .setDescription("test job")
                .setState(JobState.RUNNING)
                .setProgress(98)
                .setUpdatedAt(System.currentTimeMillis())
                .setCreatedAt(System.currentTimeMillis())
                .build();
    }

    JobStatus getJobStatusWithProblem() {
        return JobStatus.newBuilder()
                .setJobId(UUID.randomUUID().toString())
                .setDescription("test job")
                .setState(JobState.FAILED)
                .setProgress(98)
                .setUpdatedAt(System.currentTimeMillis())
                .setCreatedAt(System.currentTimeMillis())
                .setProblem(
                        ProblemDetail.newBuilder()
                                .setType("http://test.com")
                                .setTitle("test title")
                                .setStatus(500)
                                .setDetail("test detail")
                                .build()
                ).build();
    }
}

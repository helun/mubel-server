package io.mubel.server.jobs;

import io.mubel.api.grpc.v1.server.JobState;
import io.mubel.api.grpc.v1.server.JobStatus;
import io.mubel.server.TestApplication;
import io.mubel.server.spi.systemdb.JobStatusRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.test.context.TestPropertySource;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest(classes = TestApplication.class, webEnvironment = SpringBootTest.WebEnvironment.NONE)
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
@TestPropertySource(properties = {
        "mubel.provider.inmemory.enabled=true",
        "mubel.provider.inmemory.system-db=true",
        "spring.autoconfigure.exclude=org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration"
})
class BackgroundJobServiceTest {

    @Autowired
    JobStatusRepository jobStatusRepository;

    @Autowired
    ApplicationEventPublisher publisher;

    @Autowired
    BackgroundJobService service;
    JobStatus jobStatus = JobStatus.newBuilder()
            .setJobId(UUID.randomUUID().toString())
            .setState(JobState.RUNNING)
            .setDescription("Test job")
            .build();

    @AfterEach
    void tearDown() {
        jobStatusRepository.remove(jobStatus.getJobId());
    }

    @Test
    void awaitJobStatus_waits_until_JobStatus_is_becomes_awailable_in_repository() {
        var jobFuture = service.awaitJobStatus(jobStatus.getJobId());
        assertThat(jobFuture).isNotDone();
        jobStatusRepository.put(jobStatus);
        await().untilAsserted(() -> assertThat(jobFuture).isDone());
        assertThat(jobFuture).isCompletedWithValue(jobStatus);
    }

    @Test
    void published_JobStatus_is_persisted_in_repository() {
        publisher.publishEvent(jobStatus);
        await().untilAsserted(() -> assertThat(jobStatusRepository.get(jobStatus.getJobId())).isEqualTo(jobStatus));
    }
}
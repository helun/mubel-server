package io.mubel.server.jobs;

import io.mubel.api.grpc.v1.server.JobStatus;
import io.mubel.server.spi.systemdb.JobStatusRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

@Service
public class BackgroundJobService {

    private static final Logger LOG = LoggerFactory.getLogger(BackgroundJobService.class);

    private final JobStatusRepository jobStatusRepository;

    public BackgroundJobService(JobStatusRepository jobStatusRepository) {
        this.jobStatusRepository = jobStatusRepository;
    }

    @EventListener
    public void onJobStatus(JobStatus status) {
        jobStatusRepository.put(status);
    }

    @Async
    public CompletableFuture<JobStatus> awaitJobStatus(String jobId) {
        Optional<JobStatus> status = Optional.empty();
        while (status.isEmpty()) {
            status = jobStatusRepository.find(jobId);
            if (status.isEmpty()) {
                LOG.debug("waiting for job status {}", jobId);
                sleep();
            }
        }
        return CompletableFuture.completedFuture(status.get());
    }

    private static void sleep() {
        try {
            Thread.sleep(Duration.ofMillis(500));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    public JobStatus getJobStatus(String jobId) {
        return jobStatusRepository.get(jobId);
    }
}

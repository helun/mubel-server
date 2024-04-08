package io.mubel.server.spi.messages;

import io.mubel.api.grpc.v1.server.JobStatus;
import org.springframework.context.ApplicationEvent;

public class JobStatusMessage extends ApplicationEvent {

    private final JobStatus status;

    public JobStatusMessage(Object source, JobStatus status) {
        super(source);
        this.status = status;
    }

    public JobStatus status() {
        return status;
    }
}

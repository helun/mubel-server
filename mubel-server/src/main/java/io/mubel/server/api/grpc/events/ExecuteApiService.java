package io.mubel.server.api.grpc.events;

import io.mubel.api.grpc.v1.events.*;
import io.mubel.server.eventstore.EventStoreManager;
import io.mubel.server.scheduling.PublishTimeCalculator;
import io.mubel.server.scheduling.ScheduledEventQueueNames;
import io.mubel.server.spi.EventStoreContext;
import io.mubel.server.spi.queue.SendRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

@Service
public class ExecuteApiService {

    private static final Logger LOG = LoggerFactory.getLogger(ExecuteApiService.class);
    private final EventStoreManager eventStoreManager;
    private final PublishTimeCalculator publishTimeCalculator;

    public ExecuteApiService(EventStoreManager eventStoreManager, PublishTimeCalculator publishTimeCalculator) {
        this.eventStoreManager = eventStoreManager;
        this.publishTimeCalculator = publishTimeCalculator;
    }

    @Transactional
    public void execute(ExecuteRequest request) {
        LOG.debug("execute: {}", request);
        var ctx = eventStoreManager.eventStoreContext(request.getEsid());
        for (var operation : request.getOperationList()) {
            handleOperation(ctx, request, operation);
        }
    }

    private void handleOperation(EventStoreContext ctx, ExecuteRequest request, Operation operation) {
        switch (operation.getOperationCase()) {
            case APPEND -> handleAppend(ctx, operation.getAppend());
            case SCHEDULEEVENT -> handleSchedule(ctx, request.getEsid(), operation.getScheduleEvent());
            case SCHEDULEDEADLINE -> handleScheduleDeadline(ctx, request.getEsid(), operation.getScheduleDeadline());
            case CANCEL -> handleCancel(ctx, operation.getCancel());
            case OPERATION_NOT_SET -> throw new IllegalArgumentException("Operation must be specified");
        }
    }

    private void handleCancel(EventStoreContext ctx, CancelScheduledOperation operation) {
        ctx.scheduledEventsQueue().delete(operation.getEventIdList()
                .stream()
                .map(UUID::fromString)
                .toList());
    }

    private void handleScheduleDeadline(EventStoreContext ctx, String esid, ScheduleDeadlineOperation operation) {
        var sendRequest = SendRequest.builder()
                .payload(operation.getDeadline().toByteArray())
                .delayMillis(publishTimeCalculator.calculateDelay(operation.getPublishTime()))
                .type("deadline")
                .queueName(ScheduledEventQueueNames.scheduleQueueName(ScheduledEventQueueNames.scheduleQueueName(esid)))
                .build();
        ctx.scheduledEventsQueue().send(sendRequest);
    }

    private void handleSchedule(EventStoreContext ctx, String esid, ScheduleEventOperation operation) {
        var sendRequest = SendRequest.builder()
                .payload(operation.getEvent().toByteArray())
                .delayMillis(publishTimeCalculator.calculateDelay(operation.getPublishTime()))
                .type(operation.getEvent().getType())
                .queueName(ScheduledEventQueueNames.scheduleQueueName(ScheduledEventQueueNames.scheduleQueueName(esid)))
                .build();
        ctx.scheduledEventsQueue().send(sendRequest);
    }

    private void handleAppend(EventStoreContext ctx, AppendOperation append) {
        ctx.eventStore().append(append);
    }
}

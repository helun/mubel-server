package io.mubel.server.spi.support;

import io.mubel.api.grpc.v1.events.*;
import io.mubel.server.spi.eventstore.EventStore;
import io.mubel.server.spi.eventstore.ExecuteRequestHandler;
import io.mubel.server.spi.queue.MessageQueueService;
import io.mubel.server.spi.queue.SendRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.function.LongUnaryOperator;

public class AsyncExecuteRequestHandler implements ExecuteRequestHandler {

    private static final Logger LOG = LoggerFactory.getLogger(AsyncExecuteRequestHandler.class);
    private final RequestQueue<ExecuteRequest, Void> requestQueue;

    private final String esid;
    private final EventStore eventStore;
    private final MessageQueueService scheduledEventsQueue;
    private final Executor worker;
    private final LongUnaryOperator publishDelayCalculatorFn;
    private volatile boolean shouldRun = true;

    public AsyncExecuteRequestHandler(
            String esid,
            EventStore eventStore,
            MessageQueueService scheduledEventsQueue,
            int capacity,
            int timeoutMillis) {
        this.esid = esid;
        this.eventStore = eventStore;
        this.scheduledEventsQueue = scheduledEventsQueue;
        this.publishDelayCalculatorFn = publishTime -> publishTime - System.currentTimeMillis();
        this.requestQueue = new RequestQueue<>(capacity, timeoutMillis);
        worker = Executors.newSingleThreadExecutor(
                r -> new Thread(r, esid + "-execute-request-worker")
        );
    }

    public void start() {
        LOG.debug("starting execute request handler. queue size: {}", requestQueue.size());
        worker.execute(() -> {
            var requestBatch = new ExecuteRequestBatch(esid);
            while (shouldRun) {
                try {
                    requestBatch.add(requestQueue.take());
                    while (requestBatch.canAddMore()) {
                        var next = requestQueue.poll();
                        if (next == null) {
                            break;
                        } else {
                            requestBatch.add(next);
                        }
                    }
                    execute(requestBatch.joinedRequest());
                    requestBatch.complete();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    requestBatch.fail(e);
                    break;
                } catch (Exception e) {
                    requestBatch.fail(e);
                } finally {
                    requestBatch.reset();
                }
            }
        });
    }

    public void stop() {
        shouldRun = false;
    }

    @Override
    public CompletableFuture<Void> handle(ExecuteRequest request) {
        return requestQueue.submit(request);
    }

    private void execute(ExecuteRequest request) {
        for (var operation : request.getOperationList()) {
            handleOperation(request, operation);
        }
    }

    private void handleOperation(ExecuteRequest request, Operation operation) {
        switch (operation.getOperationCase()) {
            case APPEND -> handleAppend(operation.getAppend());
            case SCHEDULEEVENT -> handleSchedule(request.getEsid(), operation.getScheduleEvent());
            case SCHEDULEDEADLINE -> handleScheduleDeadline(request.getEsid(), operation.getScheduleDeadline());
            case CANCEL -> handleCancel(operation.getCancel());
            case OPERATION_NOT_SET -> throw new IllegalArgumentException("Operation must be specified");
        }
    }

    private void handleCancel(CancelScheduledOperation operation) {
        scheduledEventsQueue.delete(operation.getEventIdList()
                .stream()
                .map(UUID::fromString)
                .toList());
    }

    private void handleScheduleDeadline(String esid, ScheduleDeadlineOperation operation) {
        var sendRequest = SendRequest.builder()
                .payload(operation.getDeadline().toByteArray())
                .delayMillis(publishDelayCalculatorFn.applyAsLong(operation.getPublishTime()))
                .type("deadline")
                .queueName(esid + "-dl")
                .build();
        scheduledEventsQueue.send(sendRequest);
    }

    private void handleSchedule(String esid, ScheduleEventOperation operation) {
        var sendRequest = SendRequest.builder()
                .payload(operation.getEvent().toByteArray())
                .delayMillis(publishDelayCalculatorFn.applyAsLong(operation.getPublishTime()))
                .type(operation.getEvent().getType())
                .queueName(esid + "-sc")
                .build();
        scheduledEventsQueue.send(sendRequest);
    }

    private void handleAppend(AppendOperation append) {
        eventStore.append(append);
    }

    private static class ExecuteRequestBatch {

        private static final Logger LOG = LoggerFactory.getLogger(ExecuteRequestBatch.class);

        private static final int MAX_CANCEL_SIZE = 1000;
        private static final int MAX_SCHEDULE_OP_SIZE = 1000;
        private static final int MAX_APPEND_OP_SIZE = 1000;
        private static final int MAX_REQUEST_SIZE = 5;

        private final List<RequestQueue.Entry<ExecuteRequest, Void>> requestBuffer = new ArrayList<>(MAX_REQUEST_SIZE);
        private final ExecuteRequest.Builder joinedRequest = ExecuteRequest.newBuilder();

        private int appendSize = 0;
        private int scheduleSize = 0;
        private int cancelSize = 0;

        public ExecuteRequestBatch(String esid) {
            joinedRequest.setEsid(esid);
        }

        public void add(RequestQueue.Entry<ExecuteRequest, Void> entry) {
            requestBuffer.add(entry);
            analyze(entry.request());
        }

        private void analyze(ExecuteRequest request) {
            for (var operation : request.getOperationList()) {
                switch (operation.getOperationCase()) {
                    case APPEND -> appendSize += operation.getAppend().getEventCount();
                    case SCHEDULEEVENT -> scheduleSize++;
                    case SCHEDULEDEADLINE -> scheduleSize++;
                    case CANCEL -> cancelSize += operation.getCancel().getEventIdCount();
                }
            }
        }

        boolean canAddMore() {
            return requestBuffer.size() < MAX_REQUEST_SIZE
                    && cancelSize < MAX_CANCEL_SIZE
                    && appendSize < MAX_APPEND_OP_SIZE
                    && scheduleSize < MAX_SCHEDULE_OP_SIZE;
        }

        public ExecuteRequest joinedRequest() {
            if (requestBuffer.size() == 1) {
                LOG.debug("batch size 1, append size: {}, cancel size: {}, schedule size: {}", appendSize, cancelSize, scheduleSize);
                return requestBuffer.getFirst().request();
            }
            List<EventDataInput> appends = appendSize > 0 ? new ArrayList<>(appendSize) : List.of();
            for (var rrb : requestBuffer) {
                ExecuteRequest request = rrb.request();
                for (var operation : request.getOperationList()) {
                    switch (operation.getOperationCase()) {
                        case APPEND -> appends.addAll(operation.getAppend().getEventList());
                        case SCHEDULEEVENT -> joinedRequest.addOperation(operation);
                        case SCHEDULEDEADLINE -> joinedRequest.addOperation(operation);
                        case CANCEL -> joinedRequest.addOperation(operation);
                        case OPERATION_NOT_SET -> throw new IllegalArgumentException("Operation must be specified");
                    }
                }
            }

            if (appendSize > 0) {
                joinedRequest.addOperation(Operation.newBuilder()
                        .setAppend(AppendOperation.newBuilder()
                                .addAllEvent(appends)
                                .build()));
            }
            LOG.debug("batch size {}, append size: {}, cancel size: {}, schedule size: {}", requestBuffer.size(), appendSize, cancelSize, scheduleSize);
            return joinedRequest.build();
        }

        public void reset() {
            requestBuffer.clear();
            joinedRequest.clear();
            appendSize = 0;
            scheduleSize = 0;
            cancelSize = 0;
        }

        public void fail(Exception e) {
            requestBuffer.forEach(r -> r.future().completeExceptionally(e));
        }

        public void complete() {
            requestBuffer.forEach(r -> r.future().complete(null));
        }
    }
}

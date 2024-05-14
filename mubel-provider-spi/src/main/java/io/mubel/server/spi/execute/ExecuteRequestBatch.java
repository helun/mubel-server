package io.mubel.server.spi.execute;

import io.mubel.api.grpc.v1.events.AppendOperation;
import io.mubel.api.grpc.v1.events.EventDataInput;
import io.mubel.api.grpc.v1.events.ScheduleDeadlineOperation;
import io.mubel.api.grpc.v1.events.ScheduleEventOperation;
import io.mubel.server.spi.queue.BatchSendRequest;
import io.mubel.server.spi.support.RequestQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.LongUnaryOperator;

class ExecuteRequestBatch {

    private static final Logger LOG = LoggerFactory.getLogger(ExecuteRequestBatch.class);

    private static final int MAX_CANCEL_SIZE = 1000;
    private static final int MAX_SCHEDULE_OP_SIZE = 1000;
    private static final int MAX_APPEND_OP_SIZE = 1000;
    private static final int MAX_REQUEST_SIZE = 5;
    private static final int DEFAULT_APPENDS_SIZE = 256;
    private static final int DEFAULT_SCHEDULED_SIZE = 16;

    private final List<RequestQueue.Entry<InternalExecuteRequest, Void>> requestBuffer = new ArrayList<>(MAX_REQUEST_SIZE);
    private final BatchSendRequest.BatchEntry.Builder batchEntryBuilder = BatchSendRequest.BatchEntry.builder();
    private final LongUnaryOperator publishDelayCalculatorFn;
    private final BatchSendRequest.Builder deadlineBatch = BatchSendRequest.builder();
    private final BatchSendRequest.Builder scheduledEventsBatch = BatchSendRequest.builder();
    private final List<BatchSendRequest> sendRequests = new ArrayList<>(2);
    private final ArrayList<EventDataInput> appends = new ArrayList<>(DEFAULT_APPENDS_SIZE);
    private final ArrayList<BatchSendRequest.BatchEntry> deadlines = new ArrayList<>(DEFAULT_SCHEDULED_SIZE);
    private final ArrayList<BatchSendRequest.BatchEntry> scheduledEvents = new ArrayList<>(DEFAULT_SCHEDULED_SIZE);
    private final ArrayList<UUID> cancels = new ArrayList<>(DEFAULT_SCHEDULED_SIZE);

    private AppendOperation appendOperation;

    private int totalAppendSize = 0;
    private int totalAppendOps = 0;
    private int totalDeadlineSize = 0;
    private int totalScheduledSize = 0;
    private int totalCancelSize = 0;

    public ExecuteRequestBatch(String esid, LongUnaryOperator publishDelayCalculatorFn) {
        deadlineBatch.queueName(esid + "-dl");
        scheduledEventsBatch.queueName(esid + "-sc");
        this.publishDelayCalculatorFn = publishDelayCalculatorFn;
    }

    public void add(RequestQueue.Entry<InternalExecuteRequest, Void> entry) {
        requestBuffer.add(entry);
        analyze(entry.request());
    }

    private void analyze(InternalExecuteRequest request) {
        totalAppendSize += request.appendSize();
        totalAppendOps += request.appendOps();
        totalDeadlineSize += request.deadlineSize();
        totalScheduledSize += request.scheduledSize();
        totalCancelSize += request.cancelSize();
    }

    boolean canAddMore() {
        return requestBuffer.size() < MAX_REQUEST_SIZE
                && totalCancelSize < MAX_CANCEL_SIZE
                && totalAppendSize < MAX_APPEND_OP_SIZE
                && totalDeadlineSize < MAX_SCHEDULE_OP_SIZE
                && totalScheduledSize < MAX_SCHEDULE_OP_SIZE;
    }

    public void consolidate() {
        for (var rrb : requestBuffer) {
            var request = rrb.request();
            consolidateAppend(request);
            consolidateDeadlines(request);
            consolidateScheduled(request);
            consolidateCancels(request);
        }
        if (totalDeadlineSize > 0) {
            sendRequests.add(deadlineBatch
                    .entries(deadlines)
                    .build());
        }
        if (totalScheduledSize > 0) {
            sendRequests.add(scheduledEventsBatch
                    .entries(scheduledEvents)
                    .build());
        }
        LOG.debug("batch size {}, append size: {}, cancel size: {}, deadline size: {}, schedule size: {}", requestBuffer.size(), totalAppendSize, totalCancelSize, totalDeadlineSize, totalScheduledSize);
    }

    private void consolidateCancels(InternalExecuteRequest request) {
        if (request.cancelSize() > 0) {
            var cancels = getCancels();
            for (var op : request.cancelOperations()) {
                for (var eventId : op.getEventIdList()) {
                    cancels.add(UUID.fromString(eventId));
                }
            }
        }
    }

    private void consolidateScheduled(InternalExecuteRequest request) {
        if (request.scheduledSize() > 0) {
            var scheduledEvents = getScheduledEvents();
            for (var op : request.scheduledOperations()) {
                scheduledEvents.add(toBatchEntry(op));
            }
        }
    }

    private void consolidateDeadlines(InternalExecuteRequest request) {
        if (request.deadlineSize() > 0) {
            var deadlines = getDeadlines();
            for (var op : request.deadlineOperations()) {
                deadlines.add(toBatchEntry(op));
            }
        }
    }

    private void consolidateAppend(InternalExecuteRequest request) {
        if (totalAppendOps == 1 && request.appendOps() == 1) {
            appendOperation = request.appendOperations().getFirst();
        } else if (totalAppendOps > 0) {
            var appends = getAppendList();
            for (var op : request.appendOperations()) {
                appends.addAll(op.getEventList());
            }
        }
    }

    private List<UUID> getCancels() {
        if (totalCancelSize > DEFAULT_SCHEDULED_SIZE) {
            cancels.ensureCapacity(totalCancelSize);
        }
        return cancels;
    }

    private List<BatchSendRequest.BatchEntry> getScheduledEvents() {
        if (totalScheduledSize > DEFAULT_SCHEDULED_SIZE) {
            scheduledEvents.ensureCapacity(totalScheduledSize);
        }
        return scheduledEvents;
    }

    private List<BatchSendRequest.BatchEntry> getDeadlines() {
        if (totalDeadlineSize > DEFAULT_APPENDS_SIZE) {
            deadlines.ensureCapacity(totalDeadlineSize);
        }
        return deadlines;
    }

    private List<EventDataInput> getAppendList() {
        if (totalAppendSize > DEFAULT_APPENDS_SIZE) {
            appends.ensureCapacity(totalAppendSize);
        }
        return appends;
    }

    private BatchSendRequest.BatchEntry toBatchEntry(ScheduleDeadlineOperation scheduleDeadline) {
        batchEntryBuilder.clear();
        return batchEntryBuilder
                .payload(scheduleDeadline.getDeadline().toByteArray())
                .type("deadline")
                .delayMillis(publishDelayCalculatorFn.applyAsLong(scheduleDeadline.getPublishTime()))
                .build();
    }

    private BatchSendRequest.BatchEntry toBatchEntry(ScheduleEventOperation operation) {
        batchEntryBuilder.clear();
        return batchEntryBuilder
                .payload(operation.getEvent().toByteArray())
                .type(operation.getEvent().getType())
                .delayMillis(publishDelayCalculatorFn.applyAsLong(operation.getPublishTime()))
                .type(operation.getEvent().getType())
                .build();
    }

    public void reset() {
        requestBuffer.clear();
        appends.clear();
        deadlines.clear();
        scheduledEvents.clear();
        batchEntryBuilder.clear();
        deadlineBatch.clearEntries();
        scheduledEventsBatch.clearEntries();
        sendRequests.clear();
        totalAppendSize = 0;
        totalAppendOps = 0;
        totalDeadlineSize = 0;
        totalScheduledSize = 0;
        totalCancelSize = 0;
    }

    public void fail(Exception e) {
        requestBuffer.forEach(r -> r.future().completeExceptionally(e));
    }

    public void complete() {
        requestBuffer.forEach(r -> r.future().complete(null));
    }

    public List<BatchSendRequest> sendRequests() {
        return sendRequests;
    }

    public AppendOperation appendOperation() {
        return appendOperation != null ? appendOperation :
                AppendOperation.newBuilder()
                        .addAllEvent(appends)
                        .build();
    }

    public List<UUID> cancelIds() {
        return cancels;
    }

    public boolean hasCancels() {
        return totalCancelSize > 0;
    }

    public boolean hasAppendOperation() {
        return totalAppendOps > 0;
    }
}

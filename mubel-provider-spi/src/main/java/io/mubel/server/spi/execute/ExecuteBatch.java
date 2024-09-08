package io.mubel.server.spi.execute;

import com.fasterxml.uuid.impl.UUIDUtil;
import io.mubel.api.grpc.v1.events.AppendOperation;
import io.mubel.api.grpc.v1.events.EventDataInput;
import io.mubel.api.grpc.v1.events.ScheduleDeadlineOperation;
import io.mubel.api.grpc.v1.events.ScheduleEventOperation;
import io.mubel.server.spi.queue.BatchSendRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.LongUnaryOperator;

class ExecuteBatch {

    private static final Logger LOG = LoggerFactory.getLogger(ExecuteBatch.class);

    private static final int MAX_REQUEST_SIZE = 5;
    private static final int DEFAULT_APPENDS_SIZE = 256;
    private static final int DEFAULT_SCHEDULED_SIZE = 16;

    private final List<InternalExecuteRequest> requestBuffer = new ArrayList<>(MAX_REQUEST_SIZE);
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

    public ExecuteBatch(String esid, LongUnaryOperator publishDelayCalculatorFn) {
        deadlineBatch.queueName(esid + "-dl");
        scheduledEventsBatch.queueName(esid + "-sc");
        this.publishDelayCalculatorFn = publishDelayCalculatorFn;
    }

    public void addAll(Iterable<InternalExecuteRequest> entries) {
        for (var entry : entries) {
            requestBuffer.add(entry);
            analyze(entry);
        }
    }

    private void analyze(InternalExecuteRequest request) {
        totalAppendSize += request.appendSize();
        totalAppendOps += request.appendOps();
        totalDeadlineSize += request.deadlineSize();
        totalScheduledSize += request.scheduledSize();
        totalCancelSize += request.cancelSize();
    }

    public void consolidate() {
        for (var request : requestBuffer) {
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
                    cancels.add(UUIDUtil.uuid(eventId));
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

    private void reset() {
        requestBuffer.clear();
        appendOperation = null;
        appends.clear();
        deadlines.clear();
        cancels.clear();
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

    public void fail(Throwable e) {
        requestBuffer.forEach(r -> r.fail(e));
        reset();
    }

    public void complete() {
        requestBuffer.forEach(InternalExecuteRequest::complete);
        reset();
    }

    public List<BatchSendRequest> sendRequests() {
        return sendRequests;
    }

    public AppendOperation appendOperation() {
        return switch (totalAppendOps) {
            case 0 -> null;
            case 1 -> appendOperation;
            default -> AppendOperation.newBuilder()
                    .addAllEvent(appends)
                    .build();
        };
    }

    public List<UUID> cancelIds() {
        return List.copyOf(cancels);
    }

    public boolean hasCancels() {
        return totalCancelSize > 0;
    }

    public boolean hasAppendOperation() {
        return totalAppendOps > 0;
    }
}

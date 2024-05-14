package io.mubel.server.spi.execute;

import io.mubel.api.grpc.v1.events.*;

import java.util.List;

public class InternalExecuteRequest {

    private final ExecuteRequest request;
    private int appendSize = 0;
    private int appendOps = 0;
    private int deadlineSize = 0;
    private int scheduledSize = 0;
    private int cancelSize = 0;

    public static InternalExecuteRequest of(ExecuteRequest request) {
        return new InternalExecuteRequest(request);
    }

    private InternalExecuteRequest(ExecuteRequest request) {
        this.request = request;
        analyze();
    }

    private void analyze() {
        for (var operation : request.getOperationList()) {
            switch (operation.getOperationCase()) {
                case APPEND -> {
                    appendSize += operation.getAppend().getEventCount();
                    appendOps++;
                }
                case SCHEDULEEVENT -> scheduledSize++;
                case SCHEDULEDEADLINE -> deadlineSize++;
                case CANCEL -> cancelSize += operation.getCancel().getEventIdCount();
            }
        }
    }

    public int appendSize() {
        return appendSize;
    }

    public int appendOps() {
        return appendOps;
    }

    public int deadlineSize() {
        return deadlineSize;
    }


    public int scheduledSize() {
        return scheduledSize;
    }

    public int cancelSize() {
        return cancelSize;
    }

    public List<AppendOperation> appendOperations() {
        return request.getOperationList().stream()
                .filter(op -> op.getOperationCase() == Operation.OperationCase.APPEND)
                .map(Operation::getAppend)
                .toList();
    }

    public List<ScheduleDeadlineOperation> deadlineOperations() {
        return request.getOperationList().stream()
                .filter(op -> op.getOperationCase() == Operation.OperationCase.SCHEDULEDEADLINE)
                .map(Operation::getScheduleDeadline)
                .toList();
    }

    public List<ScheduleEventOperation> scheduledOperations() {
        return request.getOperationList().stream()
                .filter(op -> op.getOperationCase() == Operation.OperationCase.SCHEDULEEVENT)
                .map(Operation::getScheduleEvent)
                .toList();
    }

    public List<CancelScheduledOperation> cancelOperations() {
        return request.getOperationList().stream()
                .filter(op -> op.getOperationCase() == Operation.OperationCase.CANCEL)
                .map(Operation::getCancel)
                .toList();
    }
}

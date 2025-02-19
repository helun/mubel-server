/*
 * mubel-provider-spi - Multi Backend Event Log
 * Copyright © 2025 Henrik Barratt-Due (henrikbd@hey.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.mubel.server.spi.execute;

import io.mubel.api.grpc.v1.events.*;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public class InternalExecuteRequest {

    private final CompletableFuture<Void> responseFuture = new CompletableFuture<>();
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

    public void complete() {
        responseFuture.complete(null);
    }

    public CompletableFuture<Void> response() {
        return responseFuture;
    }

    public void fail(Throwable e) {
        responseFuture.completeExceptionally(e);
    }
}

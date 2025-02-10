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

import io.mubel.api.grpc.v1.events.ExecuteRequest;
import io.mubel.server.spi.eventstore.EventStore;
import io.mubel.server.spi.eventstore.ExecuteRequestHandler;
import io.mubel.server.spi.queue.MessageQueueService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class BatchingExecuteRequestHandler implements ExecuteRequestHandler {

    private static final Logger LOG = LoggerFactory.getLogger(BatchingExecuteRequestHandler.class);
    public static final int BUFFER_SIZE = 16;

    private final String esid;
    private final EventStore eventStore;
    private final MessageQueueService scheduledEventsQueue;
    private final ExecuteBatch batch;
    private final BlockingQueue<InternalExecuteRequest> requestBuffer;
    private final ExecutorService executor;
    private final AtomicBoolean shouldRun = new AtomicBoolean(true);

    public BatchingExecuteRequestHandler(
            String esid,
            EventStore eventStore,
            MessageQueueService scheduledEventsQueue) {
        this.esid = esid;
        this.eventStore = eventStore;
        this.scheduledEventsQueue = scheduledEventsQueue;
        this.batch = new ExecuteBatch(esid, publishTime -> publishTime - System.currentTimeMillis());
        this.requestBuffer = new ArrayBlockingQueue<>(BUFFER_SIZE);
        this.executor = start();
    }

    private ExecutorService start() {
        var executor = Executors.newSingleThreadExecutor(r -> new Thread(r, "-execute-request-worker-%s".formatted(esid)));
        executor.submit(() -> {
            try {
                final var batch = new ArrayList<InternalExecuteRequest>(BUFFER_SIZE);
                int maxDrain = BUFFER_SIZE - 1;
                while (shouldRun.get()) {
                    batch.add(requestBuffer.take());
                    requestBuffer.drainTo(batch, maxDrain);
                    handleBatch(batch);
                    batch.clear();
                }
            } catch (Exception e) {
                handleError(e);
            }
        });
        return executor;
    }

    private void handleError(Throwable throwable) {
        LOG.error("Error handling execute request batch", throwable);
        batch.fail(throwable);
    }

    private void handleBatch(List<InternalExecuteRequest> requests) {
        LOG.trace("Handling batch of {} requests", requests.size());
        batch.addAll(requests);
        batch.consolidate();
        if (batch.hasAppendOperation()) {
            eventStore.append(batch.appendOperation());
        }
        for (var request : batch.sendRequests()) {
            scheduledEventsQueue.send(request);
        }
        if (batch.hasCancels()) {
            scheduledEventsQueue.delete(batch.cancelIds());
        }
        batch.complete();
        LOG.trace("batch complete");
    }

    @Override
    public CompletableFuture<Void> handle(ExecuteRequest request) {
        var ir = InternalExecuteRequest.of(request);
        try {
            if (requestBuffer.offer(ir, 1, TimeUnit.SECONDS)) {
                return ir.response();
            } else {
                ir.fail(new TimeoutException("Failed to add request to buffer"));
            }
        } catch (InterruptedException e) {
            ir.fail(e);
        }
        return ir.response();
    }

    @Override
    public void stop() {
        shouldRun.set(false);
        executor.shutdown();
    }
}

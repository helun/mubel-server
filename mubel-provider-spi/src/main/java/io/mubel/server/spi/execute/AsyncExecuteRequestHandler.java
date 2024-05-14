package io.mubel.server.spi.execute;

import io.mubel.api.grpc.v1.events.ExecuteRequest;
import io.mubel.server.spi.eventstore.EventStore;
import io.mubel.server.spi.eventstore.ExecuteRequestHandler;
import io.mubel.server.spi.queue.MessageQueueService;
import io.mubel.server.spi.support.RequestQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.function.LongUnaryOperator;

public class AsyncExecuteRequestHandler implements ExecuteRequestHandler {

    private static final Logger LOG = LoggerFactory.getLogger(AsyncExecuteRequestHandler.class);
    private final RequestQueue<InternalExecuteRequest, Void> requestQueue;

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
            var requestBatch = new ExecuteRequestBatch(esid, publishDelayCalculatorFn);
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
                    requestBatch.consolidate();
                    execute(requestBatch);
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
        return requestQueue.submit(InternalExecuteRequest.of(request));
    }

    private void execute(ExecuteRequestBatch rb) {
        if (rb.hasAppendOperation()) {
            eventStore.append(rb.appendOperation());
        }
        for (var request : rb.sendRequests()) {
            scheduledEventsQueue.send(request);
        }
        if (rb.hasCancels()) {
            scheduledEventsQueue.delete(rb.cancelIds());
        }
    }

}

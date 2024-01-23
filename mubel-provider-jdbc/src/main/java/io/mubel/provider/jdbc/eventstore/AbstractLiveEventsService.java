package io.mubel.provider.jdbc.eventstore;

import io.mubel.api.grpc.EventData;
import io.mubel.api.grpc.GetEventsRequest;
import io.mubel.api.grpc.GetEventsResponse;
import io.mubel.server.spi.DataDispatcher;
import io.mubel.server.spi.DataStream;
import io.mubel.server.spi.LiveEventsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class AbstractLiveEventsService implements LiveEventsService {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractLiveEventsService.class);
    private final DataDispatcher<EventData> dispatcher = DataDispatcher.<EventData>builder()
            .withBufferSize(512)
            .withEndMessage(EventData.newBuilder().build())
            .build();

    private final AtomicBoolean shouldRun = new AtomicBoolean(true);
    private final AtomicBoolean running = new AtomicBoolean(false);

    protected long lastSequenceNo = -1;

    private GetEventsRequest.Builder requestBuilder = GetEventsRequest.newBuilder()
            .setSize(256);

    private final List<Long> backoffSchedule = List.of(
            1000L,
            2000L,
            5000L,
            10000L,
            30000L,
            60000L
    );

    private AtomicInteger errorCount = new AtomicInteger(0);

    private final JdbcEventStore eventStore;
    protected final Executor executor;

    public AbstractLiveEventsService(JdbcEventStore eventStore,
                                     Executor executor) {
        this.eventStore = eventStore;
        this.executor = executor;
    }

    public DataStream<EventData> liveEvents() {
        start();
        return dispatcher.newConsumer();
    }

    public void start() {
        if (running.compareAndSet(false, true)) {
            executor.execute(() -> {
                LOG.info("Starting live events service");
                while (shouldRun()) {
                    try {
                        initLastSequenceNo();
                        run();
                    } catch (Exception e) {
                        LOG.error("Error in live events service", e);
                        backoff();
                    }
                }
            });
        }

    }

    private void backoff() {
        if (!shouldRun()) {
            return;
        }
        try {
            var backoff = backoffSchedule.get(Math.min(errorCount.get(), backoffSchedule.size() - 1));
            LOG.info("Backing off for {}ms", backoff);
            Thread.sleep(backoff);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    protected abstract void run() throws Exception;

    private void resetErrorCounter() {
        errorCount.set(0);
    }

    protected void dispatchNewEvents() throws InterruptedException {
        GetEventsResponse response;
        do {
            LOG.debug("Fetching events from {}", lastSequenceNo);
            response = eventStore.get(requestBuilder.setFromSequenceNo(lastSequenceNo).build());
            LOG.debug("Dispatching {} events", response.getEventCount());
            for (var event : response.getEventList()) {
                dispatcher.dispatch(event);
                lastSequenceNo = event.getSequenceNo();
            }
            resetErrorCounter();
        } while (response.getEventCount() > 0);
    }

    protected boolean shouldRun() {
        return !Thread.currentThread().isInterrupted() && shouldRun.get();
    }

    public void stop() {
        LOG.info("Stopping live events service");
        shouldRun.set(false);
        dispatcher.end();
        onStop();
    }

    protected abstract void onStop();

    private void initLastSequenceNo() {
        if (lastSequenceNo != -1) {
            return;
        }
        lastSequenceNo = eventStore.maxSequenceNo();
        LOG.debug("Initialized last sequence no to {}", lastSequenceNo);
    }
}

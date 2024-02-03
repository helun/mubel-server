package io.mubel.provider.jdbc.eventstore;

import io.mubel.api.grpc.EventData;
import io.mubel.api.grpc.GetEventsRequest;
import io.mubel.api.grpc.GetEventsResponse;
import io.mubel.server.spi.eventstore.LiveEventsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.scheduler.Scheduler;

import java.util.concurrent.atomic.AtomicBoolean;

public abstract class JdbcLiveEventsService implements LiveEventsService {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcLiveEventsService.class);
    private Flux<EventData> liveEvents;

    private final AtomicBoolean shouldRun = new AtomicBoolean(true);
    private final Scheduler scheduler;

    protected long lastSequenceNo = -1;

    private GetEventsRequest.Builder requestBuilder = GetEventsRequest.newBuilder()
            .setSize(256);

    private final JdbcEventStore eventStore;

    public JdbcLiveEventsService(JdbcEventStore eventStore, Scheduler scheduler) {
        this.eventStore = eventStore;
        this.scheduler = scheduler;
    }

    public Flux<EventData> liveEvents() {
        if (liveEvents == null) {
            synchronized (this) {
                if (liveEvents == null) {
                    this.liveEvents = initLiveEvents();
                }
            }
        }
        return liveEvents.share()
                .onBackpressureError();
    }

    private Flux<EventData> initLiveEvents() {
        return Flux.<EventData>push(emitter -> {
                    initLastSequenceNo();
                    while (shouldRun()) {
                        try {
                            run(emitter);
                        } catch (Exception e) {
                            LOG.error("Error in live events service", e);
                            emitter.error(e);
                        }
                    }
                }).subscribeOn(scheduler)
                .doFinally(signal -> stop());
    }

    protected abstract void run(FluxSink<EventData> emitter) throws Exception;

    protected void dispatchNewEvents(FluxSink<EventData> emitter) {
        GetEventsResponse response;
        do {
            LOG.debug("Fetching events from {}", lastSequenceNo);
            response = eventStore.get(requestBuilder.setFromSequenceNo(lastSequenceNo).build());
            LOG.debug("Dispatching {} events", response.getEventCount());
            for (var event : response.getEventList()) {
                emitter.next(event);
                lastSequenceNo = event.getSequenceNo();
            }
        } while (response.getEventCount() > 0);
    }

    protected boolean shouldRun() {
        return !Thread.currentThread().isInterrupted() && shouldRun.get();
    }

    public void stop() {
        LOG.info("Stopping live events service");
        shouldRun.set(false);
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

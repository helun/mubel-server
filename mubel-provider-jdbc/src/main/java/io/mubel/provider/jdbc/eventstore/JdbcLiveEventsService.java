package io.mubel.provider.jdbc.eventstore;

import io.mubel.api.grpc.v1.events.*;
import io.mubel.server.spi.eventstore.LiveEventsService;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.scheduler.Scheduler;

import java.util.concurrent.atomic.AtomicBoolean;

public abstract class JdbcLiveEventsService implements LiveEventsService {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcLiveEventsService.class);
    private volatile Flux<EventData> liveEvents;

    private final AtomicBoolean shouldRun = new AtomicBoolean(true);
    private final Scheduler scheduler;
    private volatile Subscription liveEventsSubscription = null;
    protected long lastSequenceNo = -1;

    private final GetEventsRequest.Builder requestBuilder = GetEventsRequest.newBuilder()
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
                    liveEvents = initLiveEvents()
                            .doOnSubscribe(sub -> {
                                LOG.debug("subscribed to live events");
                                liveEventsSubscription = sub;
                            })
                            .doOnCancel(() -> LOG.debug("unsubscribed from live events"))
                            .doOnError(e -> LOG.error("error in live events service", e))
                            .doOnComplete(() -> LOG.debug("live events service completed"))
                            .doFinally(signal -> LOG.debug("live events service terminated"));
                }
            }
        }
        LOG.debug("returning live events flux");
        return liveEvents.share()
                .onBackpressureError();
    }

    private Flux<EventData> initLiveEvents() {
        LOG.debug("initializing live events service");
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
            LOG.trace("fetching events from sequence no: {}", lastSequenceNo);
            response = eventStore.get(requestBuilder.setSelector(
                                    EventSelector.newBuilder().setAll(
                                            AllSelector.newBuilder().setFromSequenceNo(lastSequenceNo))
                            )
                            .build()
            );
            LOG.trace("dispatching {} events", response.getEventCount());
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
        LOG.info("stopping live events service");
        shouldRun.set(false);
        if (liveEventsSubscription != null) {
            liveEventsSubscription.cancel();
        }
        onStop();
    }

    protected abstract void onStop();

    private void initLastSequenceNo() {
        if (lastSequenceNo != -1) {
            return;
        }
        lastSequenceNo = eventStore.maxSequenceNo();
        LOG.debug("initialized last sequence no to {}", lastSequenceNo);
    }
}

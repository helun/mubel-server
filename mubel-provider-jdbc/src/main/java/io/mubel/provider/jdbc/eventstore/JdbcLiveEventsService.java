/*
 * mubel-provider-jdbc - mubel-provider-jdbc
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
package io.mubel.provider.jdbc.eventstore;

import io.mubel.api.grpc.v1.events.*;
import io.mubel.server.spi.eventstore.LiveEventsService;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.SignalType;
import reactor.core.scheduler.Scheduler;

import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public abstract class JdbcLiveEventsService implements LiveEventsService {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcLiveEventsService.class);
    private final AtomicReference<Flux<EventData>> liveEvents = new AtomicReference<>(null);

    private final AtomicBoolean shouldRun = new AtomicBoolean(true);
    private final Scheduler scheduler;
    private volatile Subscription liveEventsSubscription = null;
    protected long lastSequenceNo = -1;

    private final GetEventsRequest.Builder requestBuilder = GetEventsRequest.newBuilder()
            .setSize(256);

    private final JdbcEventStore eventStore;
    private final CountDownLatch stoppedLatch = new CountDownLatch(1);

    public JdbcLiveEventsService(JdbcEventStore eventStore, Scheduler scheduler) {
        this.eventStore = eventStore;
        this.scheduler = scheduler;
    }

    public Flux<EventData> liveEvents() {
        return liveEvents.updateAndGet(f -> Objects.requireNonNullElseGet(f, this::initLiveEvents));
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
                .doFinally(this::stopped)
                .doOnCancel(() -> LOG.debug("unsubscribed from live events"))
                .doOnError(e -> LOG.error("error in live events service", e))
                .doOnComplete(() -> LOG.debug("live events service completed"))
                .doFinally(signal -> LOG.debug("live events service terminated"))
                .share();
    }

    private void stopped(SignalType signalType) {
        stoppedLatch.countDown();
    }

    protected abstract void run(FluxSink<EventData> emitter) throws Exception;

    protected void dispatchNewEvents(FluxSink<EventData> emitter) {
        GetEventsResponse response;
        do {
            if (!shouldRun()) {
                emitter.complete();
                break;
            }
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
        } while (shouldRun() && response.getEventCount() > 0);
    }

    protected boolean shouldRun() {
        return !Thread.currentThread().isInterrupted() && shouldRun.get();
    }

    public void stop() {
        if (shouldRun.compareAndSet(true, false)) {
            LOG.info("stopping live events service");
            if (liveEventsSubscription != null) {
                liveEventsSubscription.cancel();
            }
            onStop();
            try {
                stoppedLatch.await(3, TimeUnit.SECONDS);
                LOG.info("live events service stopped");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
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

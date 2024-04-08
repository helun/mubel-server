package io.mubel.provider.jdbc.eventstore;

import io.mubel.api.grpc.v1.events.EventData;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.scheduler.Scheduler;

import java.time.Duration;

public class PollingLiveEventsService extends JdbcLiveEventsService {

    private final int pollingIntervalMs;

    public PollingLiveEventsService(int pollingIntervalMs, JdbcEventStore eventStore, Scheduler scheduler) {
        super(eventStore, scheduler);
        this.pollingIntervalMs = pollingIntervalMs;
    }

    @Override
    protected void run(FluxSink<EventData> emitter) {
        Flux.interval(Duration.ofMillis(pollingIntervalMs))
                .doOnNext(i -> dispatchNewEvents(emitter))
                .blockLast();
    }

    @Override
    protected void onStop() {

    }
}

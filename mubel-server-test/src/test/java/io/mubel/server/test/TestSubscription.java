package io.mubel.server.test;

import io.mubel.api.grpc.EventData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class TestSubscription {

    private static final Logger LOG = LoggerFactory.getLogger(TestSubscription.class);
    private final List<EventData> received = new ArrayList<>();
    private final List<EventData> receivedOutOfOrder = new ArrayList<>();

    private final AtomicBoolean completed = new AtomicBoolean(false);

    private final AtomicReference<Throwable> error = new AtomicReference<>();

    private EventData lastReceived;


    private final ExecutorService subscriberExecutor = Executors.newSingleThreadExecutor();

    public static TestSubscription create(Flux<EventData> subscription) {
        return new TestSubscription(subscription);
    }

    public TestSubscription(final Flux<EventData> subscription) {
        subscription
                .doOnSubscribe(sub -> LOG.info("Starting subscription"))
                .doOnComplete(() -> LOG.info("Subscription completed"))
                .subscribeOn(Schedulers.single())
                .subscribe(this::addEvent, err -> LOG.error("Error in subscription", err));
    }

    public boolean isCompleted() {
        return completed.get();
    }

    public void addEvent(EventData e) {
        if (lastReceived != null) {
            if (lastReceived.getSequenceNo() + 1 != e.getSequenceNo()) {
                LOG.warn("Received out of order event: {} after {}", e.getSequenceNo(), lastReceived.getSequenceNo());
                receivedOutOfOrder.add(e);
            }
        }
        lastReceived = e;
        received.add(e);
    }

    public Optional<Throwable> error() {
        return Optional.ofNullable(error.get());
    }

    public int count() {
        return received.size();
    }

    public int outOfOrderCount() {
        return receivedOutOfOrder.size();
    }

    public void stop() {
        subscriberExecutor.shutdownNow();
    }

    public void throwIfError() throws Throwable {
        if (error.get() != null) {
            throw error.get();
        }
    }
}

package io.mubel.server.test;

import io.mubel.api.grpc.EventData;
import io.mubel.client.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.*;

public class TestSubscription {

    private static final Logger LOG = LoggerFactory.getLogger(TestSubscription.class);
    private final List<EventData> received = new ArrayList<>();
    private final List<EventData> receivedOutOfOrder = new ArrayList<>();
    private final Future<?> subscriptionFuture;

    private EventData lastReceived;


    private final ExecutorService subscriberExecutor = Executors.newSingleThreadExecutor();

    public static TestSubscription create(Subscription<EventData> subscription) {
        return new TestSubscription(subscription);
    }

    public TestSubscription(final Subscription<EventData> subscription) {
        subscriptionFuture = subscriberExecutor.submit(() -> {
            try {
                LOG.info("Starting subscription");
                var event = subscription.next();
                while (event != null) {
                    addEvent(event);
                    event = subscription.next();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Throwable t) {
                LOG.error("Error in subscription", t);
                throw t;
            } finally {
                LOG.info("Subscription stopped");
            }
        });
    }

    public boolean isCompleted() {
        return subscriptionFuture.isDone();
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
        try {
            subscriptionFuture.get(10, TimeUnit.SECONDS);
            return Optional.empty();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            return Optional.of(e.getCause());
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        }
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
        if (subscriptionFuture.state() == Future.State.FAILED) {
            throw subscriptionFuture.exceptionNow();
        }
    }
}

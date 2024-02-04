package io.mubel.server.test;

import io.mubel.api.grpc.ScheduledEvent;
import io.mubel.api.grpc.TriggeredEvents;
import io.mubel.client.Subscription;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ScheduledEventSubscriber {

    private final List<ScheduledEvent> received = new ArrayList<>();
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

    public ScheduledEventSubscriber(Subscription<TriggeredEvents> subscription) {
        executor.execute(() -> {
            try {
                var event = subscription.next();
                while (event != null) {
                    addEvents(event);
                    event = subscription.next();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
    }

    public List<ScheduledEvent> received() {
        return received;
    }

    private void addEvents(TriggeredEvents next) {
        received.addAll(next.getEventList());
    }

    public int count() {
        return received.size();
    }

    public void stop() {
        executor.shutdownNow();
    }

}

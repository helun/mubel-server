package io.mubel.server.spi.scheduled;

import io.mubel.api.grpc.v1.events.EventDataInput;
import io.mubel.server.spi.eventstore.EventStore;
import io.mubel.server.spi.queue.Message;
import io.mubel.server.spi.queue.MessageQueueService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ScheduledEventsHandlerTest {

    @Mock
    MessageQueueService messageQueueService;

    @Mock
    EventStore eventStore;

    ScheduledEventsHandler handler;

    @BeforeEach
    void setup() {
        handler = new ScheduledEventsHandler("esid", eventStore, messageQueueService);
        when(eventStore.getCurrentRevisions(any())).thenAnswer(invocation -> {
            var ids = invocation.<List<String>>getArgument(0);
            return ids.stream()
                    .map(id -> Map.entry(id, 5))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        });
    }

    @Test
    @SuppressWarnings("unchecked")
    void base_case() {
        Flux<Message> subFlux = Flux.fromIterable(List.of(scheduledEventMessage()));
        Flux<Message> emptyFlux = Flux.<Message>empty().delaySubscription(Duration.ofMillis(500));
        when(messageQueueService.receive(any())).thenReturn(subFlux, emptyFlux);
        handler.start();
        await().untilAsserted(() -> {
            verify(eventStore, times(1)).append(any());
        });
        handler.stop();
    }

    private Message scheduledEventMessage() {
        var event = EventDataInput.newBuilder()
                .setId(UUID.randomUUID().toString())
                .setType("test-event")
                .setStreamId(UUID.randomUUID().toString())
                .build();

        return new Message(
                UUID.randomUUID(),
                "esid-sc",
                "test-event",
                event.toByteArray()
        );
    }

}
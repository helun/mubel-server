package io.mubel.server.spi.scheduled;

import io.mubel.api.grpc.v1.events.AppendOperation;
import io.mubel.api.grpc.v1.events.EventDataInput;
import io.mubel.server.spi.eventstore.EventStore;
import io.mubel.server.spi.eventstore.Revisions;
import io.mubel.server.spi.queue.Message;
import io.mubel.server.spi.queue.MessageQueueService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
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
        when(eventStore.getRevisions(any())).thenAnswer(invocation -> {
            var ids = invocation.<List<String>>getArgument(0);
            return ids.stream()
                    .distinct()
                    .map(id -> Map.entry(id, 5))
                    .reduce(new Revisions(2),
                            (r, e) -> r.add(e.getKey(), e.getValue()),
                            (a, b) -> a
                    );
        });
    }

    @Test
    @SuppressWarnings("unchecked")
    void base_case() {
        Flux<Message> subFlux = Flux.fromIterable(List.of(scheduledEventMessage()));
        Flux<Message> emptyFlux = Flux.<Message>empty().delaySubscription(Duration.ofMillis(500));
        when(messageQueueService.receive(any())).thenReturn(subFlux, emptyFlux);
        handler.start();
        await().untilAsserted(() ->
                verify(eventStore, times(1)).append(any())
        );
        handler.stop();
    }

    @Test
    @SuppressWarnings("unchecked")
    void multiple_events_for_same_stream_in_same_batch() {
        var streamId = UUID.randomUUID().toString();
        Flux<Message> subFlux = Flux.fromIterable(List.of(
                scheduledEventMessage(streamId),
                scheduledEventMessage(streamId))
        );
        Flux<Message> emptyFlux = Flux.<Message>empty().delaySubscription(Duration.ofMillis(500));
        when(messageQueueService.receive(any())).thenReturn(subFlux, emptyFlux);
        handler.start();
        await().untilAsserted(() -> {
            var captor = ArgumentCaptor.forClass(AppendOperation.class);
            verify(eventStore, times(1)).append(captor.capture());
            var appendOp = captor.getValue();
            assertThat(appendOp.getEventList()).hasSize(2)
                    .map(EventDataInput::getRevision)
                    .containsExactly(6, 7);
        });
        handler.stop();
    }

    private Message scheduledEventMessage() {
        return scheduledEventMessage(UUID.randomUUID().toString());
    }

    private Message scheduledEventMessage(String streamId) {
        var event = EventDataInput.newBuilder()
                .setId(UUID.randomUUID().toString())
                .setType("test-event")
                .setStreamId(streamId)
                .build();

        return new Message(
                UUID.randomUUID(),
                "esid-sc",
                "test-event",
                event.toByteArray()
        );
    }

}
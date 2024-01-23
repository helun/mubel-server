package io.mubel.provider.test;

import io.mubel.api.grpc.AppendRequest;
import io.mubel.api.grpc.EventData;
import io.mubel.api.grpc.GetEventsRequest;
import io.mubel.server.spi.EventStore;
import io.mubel.server.spi.LiveEventsService;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

public abstract class LiveEventsServiceTestBase {

    protected abstract String esid();

    protected abstract EventStore eventStore();

    protected abstract LiveEventsService service();

    @Test
    void baseCase() {
        setupEvents(5);
        TestSubscriber<EventData> testSubscriber = new TestSubscriber<>(service().liveEvents());
        await().during(Duration.ofSeconds(1))
                .untilAsserted(testSubscriber::assertNoValues);
        setupEvents(5);
        await().failFast(testSubscriber::assertNoErrors)
                .until(() -> testSubscriber.values().size() == 5);
        var allEvents = eventStore().get(GetEventsRequest.newBuilder()
                .setEsid(esid())
                .setSize(10)
                .build()
        ).getEventList();
        assertThat(testSubscriber.values()).hasSize(5)
                .map(EventData::getSequenceNo)
                .containsExactly(6L, 7L, 8L, 9L, 10L);

    }

    private List<EventData> setupEvents(int count) {
        var events = Fixtures.createEventInputs(count);
        var request = AppendRequest.newBuilder()
                .setEsid(esid())
                .addAllEvent(events)
                .build();
        eventStore().append(request);
        await().until(() -> eventStore().get(GetEventsRequest.newBuilder()
                .setEsid(esid())
                .setSize(count)
                .build()
        ).getEventCount() == count);
        return eventStore().get(GetEventsRequest.newBuilder()
                .setEsid(esid())
                .setSize(count)
                .build()
        ).getEventList();
    }
}

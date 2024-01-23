package io.mubel.provider.test;

import io.mubel.api.grpc.AppendRequest;
import io.mubel.api.grpc.EventData;
import io.mubel.api.grpc.GetEventsRequest;
import io.mubel.api.grpc.SubscribeRequest;
import io.mubel.server.spi.eventstore.EventStore;
import io.mubel.server.spi.eventstore.ReplayService;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public abstract class ReplayServiceTestBase {

    protected abstract String esid();

    protected abstract EventStore eventStore();

    protected abstract ReplayService service();

    @RepeatedTest(9)
    void replay() {
        int count = 256;
        setupEvents(count);
        var request = SubscribeRequest
                .newBuilder()
                .setEsid(esid())
                .build();
        TestSubscriber<EventData> testSubscriber = new TestSubscriber<>(service().replay(request));


        testSubscriber
                .awaitDone()
                .assertComplete()
                .assertValueCount(count);
    }

    @Test
    void replayFromSequenceNo() {
        int count = 10;
        var events = setupEvents(count);
        var middle = events.get(4);
        var request = SubscribeRequest
                .newBuilder()
                .setEsid(esid())
                .setFromSequenceNo(middle.getSequenceNo())
                .build();
        TestSubscriber<EventData> testSubscriber = new TestSubscriber<>(service().replay(request));
        testSubscriber.awaitCount(5)
                .awaitDone()
                .assertComplete()
                .assertNoErrors();
        assertThat(testSubscriber.values()).hasSize(5);
        var values = testSubscriber.values();
        assertThat(values)
                .containsExactlyElementsOf(events.subList(5, 10));
    }

    @Test
    void emptyReplay() {
        var request = SubscribeRequest
                .newBuilder()
                .setEsid(esid().toString())
                .build();
        TestSubscriber<EventData> testSubscriber = new TestSubscriber(service().replay(request));
        testSubscriber.awaitDone();
        testSubscriber.assertNoErrors();
        testSubscriber.assertNoValues();
    }

    private List<EventData> setupEvents(int count) {
        var events = Fixtures.createEventInputs(count);
        var request = AppendRequest.newBuilder()
                .setEsid(esid())
                .addAllEvent(events)
                .build();
        eventStore().append(request);
        return eventStore().get(GetEventsRequest.newBuilder()
                .setEsid(esid())
                .setSize(count)
                .build()
        ).getEventList();
    }

}

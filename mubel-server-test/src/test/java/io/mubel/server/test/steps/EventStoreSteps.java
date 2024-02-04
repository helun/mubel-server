package io.mubel.server.test.steps;

import com.google.protobuf.ByteString;
import io.cucumber.java.en.And;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import io.grpc.StatusRuntimeException;
import io.mubel.api.grpc.AppendAck;
import io.mubel.api.grpc.AppendRequest;
import io.mubel.api.grpc.GetEventsRequest;
import io.mubel.client.MubelClient;
import io.mubel.server.test.Fixtures;
import io.mubel.server.test.ScenarioContext;
import org.assertj.core.api.AssertionsForClassTypes;
import org.skyscreamer.jsonassert.JSONAssert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

public class EventStoreSteps {

    private static final Logger log = LoggerFactory.getLogger(EventStoreSteps.class);
    private final ScenarioContext scenarioContext;
    private final MubelClient client;

    public EventStoreSteps(ScenarioContext scenarioContext, Configuration configuration) {
        this.scenarioContext = scenarioContext;
        this.client = configuration.client();
    }

    @Given("a payload of {string}")
    public void givenPayload(String payloadPath) {
        scenarioContext.loadPayload(payloadPath);
    }

    @When("I append {int} events")
    @When("I append another {int} events")
    public void append(int count) {
        var esid = scenarioContext.getEsid();
        var events = Fixtures.createEvents(count);

        var appendReq = AppendRequest.newBuilder()
                .setEsid(esid)
                .addAllEvent(events)
                .build();

        var ack = client.append(appendReq);
        assertThat(ack).isNotNull();
        log.info("appended {} events", count);
    }

    @When("I append an event with the payload")
    public void appendWithPayload() {
        final var evt = Fixtures.eventBuilder()
                .setStreamId(UUID.randomUUID().toString())
                .setType("test:payload")
                .setData(ByteString.copyFromUtf8(scenarioContext.getLatestPayload()))
                .build();
        scenarioContext.setLastStreamId(evt.getStreamId());

        final var esid = scenarioContext.getEsid();
        final var appendReq = AppendRequest.newBuilder()
                .setEsid(esid)
                .addEvent(evt)
                .build();
        final var ack = client.append(appendReq);
        assertThat(ack).isNotNull();
    }

    @When("I append {int} event for stream {string}")
    public void appendForStream(int count, String streamAlias) {
        var events = Fixtures.createEvents(count);

        var appendReq = AppendRequest.newBuilder()
                .setEsid(scenarioContext.getEsid())
                .addAllEvent(events)
                .build();
        var streamId = events.getFirst().getStreamId();
        scenarioContext.addStreamAlias(streamAlias, streamId);
        var ack = client.append(appendReq);
        assertThat(ack).isNotNull();
        log.info("appended {} events for stream {}", count, streamAlias);
    }

    @When("I make concurrent append requests one with {int} and {int} with {int} events")
    public void concurrentAppend(int countA, int threadCount, int countB) {
        var esid = scenarioContext.getEsid();

        var tp = Executors.newVirtualThreadPerTaskExecutor();
        var latch = new CountDownLatch(1);

        var tasks = IntStream.range(0, threadCount)
                .mapToObj(i -> (Callable<AppendAck>) () -> {
                    var appendReq = createAppendRequest(esid, countB);
                    latch.await();
                    return client.append(appendReq);
                }).map(tp::submit)
                .toList();
        client.append(createAppendRequest(esid, countA));
        latch.countDown();
        var result = tasks.stream().map(f -> {
            try {
                return f.get();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }).toList();
        assertThat(result).allSatisfy(ack -> assertThat(ack).isNotNull());
    }

    @Given("the {string} event store contains {int} events")
    public void theEventStoreContainsEvents(String alias, int eventCount) {
        var esid = scenarioContext.getEventStore(alias);
        var events = Fixtures.createEvents(eventCount);
        var appendReq = AppendRequest.newBuilder()
                .setEsid(esid)
                .addAllEvent(events)
                .build();
        client.append(appendReq);
    }

    private AppendRequest createAppendRequest(String esid, int eventCount) {
        var eventsB = Fixtures.createEvents(eventCount);
        return AppendRequest.newBuilder()
                .setEsid(esid)
                .addAllEvent(eventsB)
                .build();
    }

    @Given("an invalid event")
    public void invalidEvent() {
        scenarioContext.addGivenEvent(Fixtures.invalidEvent());
    }

    @When("the event is appended the call should fail")
    public void appendGivenShouldFail() {
        assertThatThrownBy(() -> client.append(AppendRequest.newBuilder()
                .setEsid(scenarioContext.getEsid())
                .addAllEvent(scenarioContext.getGivenEvents())
                .build()))
                .isInstanceOf(StatusRuntimeException.class)
                .hasMessageStartingWith("INVALID_ARGUMENT: ");
    }

    @And("I append an event with version {int} for stream {string}")
    public void appendVersionForStream(int version, String streamAlias) {
        var streamId = scenarioContext.getStreamId(streamAlias);
        var event = Fixtures.event(streamId, version);
        var appendReq = AppendRequest.newBuilder()
                .setEsid(scenarioContext.getEsid())
                .addEvent(event)
                .build();
        var ack = client.append(appendReq);
        assertThat(ack).isNotNull();
        log.info("appended event with version {} for stream {}", version, streamAlias);
    }

    @Then("appending another event with version {int} for stream {string} should fail")
    public void appendConflict(int version, String streamAlias) {
        var streamId = scenarioContext.getStreamId(streamAlias);
        var event = Fixtures.event(streamId, version);
        var appendReq = AppendRequest.newBuilder()
                .setEsid(scenarioContext.getEsid())
                .addEvent(event)
                .build();
        assertThatThrownBy(() -> client.append(appendReq))
                .satisfies(e -> AssertionsForClassTypes.assertThat(e).isInstanceOf(StatusRuntimeException.class));
    }

    @Then("the event store should contain {int} events")
    public void checkEventStoreCount(int count) {
        var esid = scenarioContext.getEsid();
        var response = client.get(GetEventsRequest.newBuilder()
                .setEsid(esid)
                .build());
        assertThat(response.getEventCount()).isEqualTo(count);
    }

    @Then("the fetched event should have the equal payload")
    public void checkPayload() throws Exception {
        final var sid = scenarioContext.getLastStreamId();
        final var req = GetEventsRequest.newBuilder()
                .setEsid(scenarioContext.getEsid())
                .setStreamId(sid)
                .build();
        final var response = client.get(req);
        final var result = response.getEventList()
                .stream()
                .findFirst()
                .orElseThrow();
        JSONAssert.assertEquals(scenarioContext.getLatestPayload(), result.getData().toStringUtf8(), true);
    }
}

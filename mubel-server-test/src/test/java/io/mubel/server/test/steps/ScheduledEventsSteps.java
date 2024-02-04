package io.mubel.server.test.steps;

import com.google.protobuf.ByteString;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import io.mubel.api.grpc.ScheduledEvent;
import io.mubel.api.grpc.ScheduledEventsSubscribeRequest;
import io.mubel.client.MubelClient;
import io.mubel.server.test.ScenarioContext;
import io.mubel.server.test.ScheduledEventSubscriber;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

public class ScheduledEventsSteps {

    private final MubelClient client;
    private final ScenarioContext context;

    public ScheduledEventsSteps(Configuration configuration, ScenarioContext context) {
        this.client = configuration.client();
        this.context = context;
    }

    @When("the client starts a scheduled events subscription for all categories")
    public void scheduledEventsSubscription() {
        final var sub = new ScheduledEventSubscriber(client.subscribeToScheduledEvents(
                ScheduledEventsSubscribeRequest.newBuilder()
                        .build(),
                100)
        );
        context.setScheduledEventsSubscription(sub);
    }

    @When("the client schedules an event for {int} second from now")
    public void theClientSchedulesAnEventForSecondFromNow(int publishDuration) {
        ScheduledEvent event = ScheduledEvent.newBuilder()
                .setCategory("test")
                .setId("test-id")
                .setData(ByteString.EMPTY)
                .setPublishTime(System.currentTimeMillis() + publishDuration * 1000L)
                .build();
        client.scheduleEvent(event);
    }

    @Then("the event should be published to the client after {int} second")
    public void theEventShouldBePublishedToTheClientAfterSecond(int duration) {
        var sub = context.getScheduledEventsSubscription();
        await().atLeast(Duration.ofSeconds(duration).minusMillis(100)).untilAsserted(() -> {
            long now = System.currentTimeMillis();
            assertThat(sub.count()).as("No events received").isOne();
            assertThat(sub.received())
                    .first()
                    .satisfies(se -> assertThat(se.getPublishTime())
                            .isBetween(now - 100, now + 100)
                    );
        });
    }
}

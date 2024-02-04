package io.mubel.server.test.steps;

import io.cucumber.java.After;
import io.cucumber.java.en.And;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import io.mubel.api.grpc.GetEventStoreSummaryRequest;
import io.mubel.api.grpc.SubscribeRequest;
import io.mubel.client.MubelClient;
import io.mubel.server.test.ScenarioContext;
import io.mubel.server.test.TestSubscription;
import org.awaitility.Awaitility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.assertj.core.api.Assertions.assertThat;

public class SubscriptionSteps {

    private static final Logger LOG = LoggerFactory.getLogger(SubscriptionSteps.class);

    private final MubelClient client;
    private final ScenarioContext scenarioContext;

    public SubscriptionSteps(Configuration configuration, ScenarioContext scenarioContext) {
        this.client = configuration.client();
        this.scenarioContext = scenarioContext;
    }

    @Given("I create subscription {string}")
    public void createSubscription(String alias) {
        String esid = scenarioContext.getEsid();
        var request = SubscribeRequest.newBuilder()
                .setEsid(esid)
                .build();
        var sub = TestSubscription.create(client.subscribe(request, 1));
        scenarioContext.addSubscription(alias, sub);
        LOG.info("created subscription {}", alias);
    }

    @And("the client creates subscription {string} with its consumer group token")
    public void theClientCreatesSubscriptionWithConsumerGroup(String alias) {
        String esid = scenarioContext.getEsid();
        var session = scenarioContext.getSession("default");
        var request = SubscribeRequest.newBuilder()
                .setConsumerGroupToken(session.getConsumerGroupToken())
                .setEsid(esid)
                .build();
        var sub = TestSubscription.create(session.client().subscribe(request, 10));
        scenarioContext.addSubscription(alias, sub);
    }

    @Given("I create subscription {string} with consumer group {string}")
    public void subscriptionWithConsumerGroup(String alias, String consumerGroup) {
        String esid = scenarioContext.getEsid();
        var request = SubscribeRequest.newBuilder()
                .setEsid(esid)
                .build();

        var sub = TestSubscription.create(client.subscribe(request, 10));
        scenarioContext.addSubscription(alias, sub);
        LOG.info("created subscription {}", alias);
    }

    @Then("subscription {string} should have received {int} events")
    @And("subscription {string} should receive {int} events")
    public void checkSubscriptionCount(String alias, int expectedCount) {
        var sub = scenarioContext.getSubscription(alias);
        assertThat(sub).isNotNull();
        Awaitility.await()
                .failFast(sub::throwIfError)
                .failFast(() -> sub.count() > expectedCount)
                .untilAsserted(() -> assertThat(sub.count()).isEqualTo(expectedCount));
        assertThat(sub.outOfOrderCount())
                .as("subscription %s received %d events out of order", sub, sub.outOfOrderCount())
                .isEqualTo(0);
    }

    @When("subscription {} is disconnected")
    public void cancelSubscription(String alias) {
        var sub = scenarioContext.getSubscription(alias);
        sub.stop();
    }

    @Then("the subscription {string} should fail with message {string}")
    public void theSubscriptionShouldFailWithMessage(String alias, String expectedErrorMessage) {
        var sub = scenarioContext.getSubscription(alias);
        assertThat(sub.error()).as("subscription is expected to fail").isPresent();
        assertThat(sub.error()).hasValueSatisfying(err ->
                assertThat(err).hasMessage(expectedErrorMessage)
        );
    }

    @After
    public void cleanup() {
        scenarioContext.getSubscriptions().forEach(TestSubscription::stop);
    }

    @When("subscription {string} is closed")
    public void subscriptionIsClosed(String alias) {
        var sub = scenarioContext.getSubscription(alias);
        sub.stop();
    }

    @Then("{string} event store should contain {int} events")
    public void eventStoreShouldContainEvents(String alias, int expectedCount) {
        var esid = scenarioContext.getEventStore(alias);
        var summary = client.eventStoreSummary(GetEventStoreSummaryRequest.newBuilder()
                .setEsid(esid)
                .build());
        assertThat(summary.getEventCount()).isEqualTo(expectedCount);
    }
}

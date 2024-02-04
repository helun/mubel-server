package io.mubel.server.test.steps;

import io.cucumber.java.After;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import io.mubel.api.grpc.ConsumerGroupStatus;
import io.mubel.api.grpc.JoinConsumerGroupRequest;
import io.mubel.api.grpc.LeaveConsumerGroupRequest;
import io.mubel.server.test.ScenarioContext;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SuppressWarnings("unchecked")
public class ConsumerGroupSteps {

    private static final Logger LOG = org.slf4j.LoggerFactory.getLogger(ConsumerGroupSteps.class);
    private final ScenarioContext context;

    public ConsumerGroupSteps(ScenarioContext context) {
        this.context = context;
    }

    @When("client {string} joins consumer group {string}")
    public void clientJoinsConsumerGroup(String alias, String consumerGroup) {
        LOG.info("client {} joins consumer group {}", alias, consumerGroup);
        var client = context.getSession(alias).client();
        var future = client.joinConsumerGroup(JoinConsumerGroupRequest.newBuilder()
                .setConsumerGroup(consumerGroup)
                .setEsid(context.getEsid())
                .build());
        context.putAttribute(alias + ".leader.future", future);
    }

    @Given("a client joins consumer group {string}")
    public void aClientJoinsConsumerGroup(String consumerGroup) throws Exception {
        var session = context.getSession("default");
        var future = session.client().joinConsumerGroup(JoinConsumerGroupRequest.newBuilder()
                .setConsumerGroup(consumerGroup)
                .setEsid(context.getEsid())
                .build());
        LOG.info("client joins consumer group {} token {}", consumerGroup, future.get().getToken());
        session.setConsumerGroupToken(future.get().getToken());
    }

    @Then("client {string} should become the leader of consumer group {string}")
    public void clientShouldBecomeTheLeaderOfConsumerGroup(String alias, String consumerGroup) throws Exception {
        var future = (Future<ConsumerGroupStatus>) context.getAttribute(alias + ".leader.future");
        var status = future.get(1, TimeUnit.SECONDS);
        assertThat(status.getLeader()).isTrue();
        assertThat(status.getGroupId()).endsWith(consumerGroup);
        assertThat(status.getToken()).isNotBlank();
        context.getSession(alias).setConsumerGroupToken(status.getToken());
    }

    @Then("client {string} should not become the leader")
    public void clientShouldRemainTheLeaderOfConsumerGroup(String alias) {
        var future = (Future<ConsumerGroupStatus>) context.getAttribute(alias + ".leader.future");
        await().during(Duration.ofSeconds(2))
                .untilAsserted(() -> assertThat(future.isDone()).isFalse());
    }

    @When("client {string} disconnects")
    public void clientDisconnects(String alias) {
        var session = context.getSession(alias);
        session.client().leaveConsumerGroup(LeaveConsumerGroupRequest.newBuilder()
                .setToken(session.getConsumerGroupToken())
                .build());
    }

    @After
    public void cleanup() {
        context.getSessions().forEach(session ->
                session.client().leaveConsumerGroup(LeaveConsumerGroupRequest.newBuilder()
                        .setToken(Objects.requireNonNullElse(session.getConsumerGroupToken(), ""))
                        .build())
        );
    }

    @Given("a client has an invalid consumer group token")
    public void aClientHasAnInvalidConsumerGroupToken() {
        var session = context.getSession("default");
        session.setConsumerGroupToken("invalid");
    }
}

package io.mubel.server.test.steps;

import io.cucumber.java.en.And;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import io.mubel.client.MubelClient;
import io.mubel.server.test.ScenarioContext;
import org.assertj.core.api.AssertionsForClassTypes;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

public class ServerInfoSteps {

    private final MubelClient client;
    private final ScenarioContext context;

    public ServerInfoSteps(Configuration configuration, ScenarioContext context) {
        this.client = configuration.client();
        this.context = context;
    }

    @When("I request server info")
    public void requestServerInfo() {
        context.setServerInfo(client.getServerInfo());
    }


    @Then("the response should contain the provisioned event store")
    public void theResponseShouldContainTheProvisionedEventStore() {
        var serverInfo = context.getServerInfo();
        var esid = context.getEsid();
        assertThat(serverInfo.getEventStoreList())
                .anySatisfy(es -> AssertionsForClassTypes.assertThat(es.getEsid()).isEqualTo(esid));
    }

    @And("the response should contain a {string} backend")
    public void theResponseShouldContainABackend(String backendtype) {
        var serverInfo = context.getServerInfo();
        assertThat(serverInfo.getStorageBackendList())
                .anySatisfy(es -> AssertionsForClassTypes.assertThat(es.getType()).isEqualTo(backendtype));
    }
}

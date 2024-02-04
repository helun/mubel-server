package io.mubel.server.test.steps;

import io.cucumber.java.en.Given;
import io.cucumber.java.en.When;
import io.mubel.server.test.ClientSession;
import io.mubel.server.test.ScenarioContext;

public class ClientSteps {

    private final Configuration configuration;
    private final ScenarioContext context;

    public ClientSteps(Configuration configuration, ScenarioContext context) {
        this.configuration = configuration;
        this.context = context;
    }

    @Given("clients {string} and {string}")
    public void createClients(String alias1, String alias2) {
        context.putSession(alias1, new ClientSession(configuration.newClient()));
        context.putSession(alias2, new ClientSession(configuration.newClient()));
    }

    @When("this test is paused for {int} seconds")
    public void thisTestIsPausedForSeconds(int pause) {
        try {
            Thread.sleep(pause * 1000L);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}

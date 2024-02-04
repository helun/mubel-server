package io.mubel.server.test.steps;

import io.cucumber.java.en.And;
import io.cucumber.java.en.When;
import io.mubel.api.grpc.CopyEventsRequest;
import io.mubel.api.grpc.GetJobStatusRequest;
import io.mubel.api.grpc.JobState;
import io.mubel.api.grpc.JobStatus;
import io.mubel.client.MubelClient;
import io.mubel.server.test.ScenarioContext;

import static org.awaitility.Awaitility.await;

public class SystemOperationsSteps {

    private final ScenarioContext scenarioContext;
    private final MubelClient client;

    public SystemOperationsSteps(ScenarioContext scenarioContext, Configuration configuration) {
        this.scenarioContext = scenarioContext;
        this.client = configuration.client();
    }

    @When("I copy {string} to {string}")
    public void copy(String srcAlias, String destAlias) {
        var src = scenarioContext.getEventStore(srcAlias);
        var dest = scenarioContext.getEventStore(destAlias);
        var request = CopyEventsRequest.newBuilder()
                .setSourceEsid(src)
                .setTargetEsid(dest)
                .build();
        var jobStatus = client.copyEvents(request);
        scenarioContext.putAttribute("copyJob", jobStatus);
    }

    @And("wait for copy job to finish")
    public void waitForCopyJobToFinish() {
        var jobStatus = (JobStatus) scenarioContext.getAttribute("copyJob");
        var request = GetJobStatusRequest.newBuilder()
                .setJobId(jobStatus.getJobId())
                .build();
        await().until(() -> {
            var state = client.jobStatus(request).getState();
            if (state == JobState.FAILED) {
                throw new IllegalStateException("Copy job failed");
            }
            return state == JobState.COMPLETED;
        });
    }
}

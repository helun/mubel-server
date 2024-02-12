package io.mubel.server.test.steps;

import io.cucumber.java.After;
import io.cucumber.java.en.Given;
import io.mubel.api.grpc.*;
import io.mubel.client.MubelClient;
import io.mubel.server.test.ScenarioContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class ProvisionSteps {

    private static final Logger LOG = LoggerFactory.getLogger(ProvisionSteps.class);

    private final MubelClient client;
    private final ScenarioContext context;

    public ProvisionSteps(Configuration configuration, ScenarioContext context) {
        this.client = configuration.client();
        this.context = context;
    }

    @Given("an {string} event store is provisioned")
    public void provision(String type) {
        provision(type, "default");
    }

    @Given("a event store of type {string} called {string} is provisioned")
    public void provision(String type, String alias) {
        final var dbAlias = findDbOfType(type, client.getServerInfo());
        final var esid = "ft_" + UUID.randomUUID().toString().replaceAll("-", "");
        final var request = ProvisionEventStoreRequest.newBuilder()
                .setEsid(esid)
                .setDataFormat(DataFormat.JSON)
                .setWaitForOpen(true)
                .setStorageBackendName(dbAlias)
                .build();
        final var details = client.provision(request).join();
        context.addEventStore(alias, details.getEsid());
        LOG.info("provisioned event store: {}", details);
    }

    @After
    public void cleanup() {
        context.getEventStores().values().forEach(esid -> {
            try {
                client.drop(DropEventStoreRequest.newBuilder()
                        .setEsid(esid)
                        .build()
                ).join();
            } catch (Exception err) {
                LOG.warn("failed to delete event store: {}", esid, err);
                throw new RuntimeException(err);
            }
        });
    }

    private String findDbOfType(String type, ServiceInfoResponse response) {
        var esType = switch (type) {
            case "postgres" -> "PG";
            case "mysql" -> "MYSQL";
            case "inmemory" -> "IN_MEMORY";
            default -> throw new UnsupportedOperationException("unknown event store type: %s".formatted(type));
        };
        return response.getStorageBackendList()
                .stream()
                .filter(info -> info.getType().equals(esType))
                .findFirst()
                .map(StorageBackendInfo::getName)
                .orElseThrow();
    }

}

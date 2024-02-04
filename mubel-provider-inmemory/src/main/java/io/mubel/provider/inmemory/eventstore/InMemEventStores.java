package io.mubel.provider.inmemory.eventstore;

import io.mubel.server.spi.eventstore.EventStoreProvisioner;
import io.mubel.server.spi.eventstore.EventStoreState;
import io.mubel.server.spi.exceptions.ResourceNotFoundException;
import io.mubel.server.spi.model.*;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static io.mubel.provider.inmemory.InMemProvider.PROVIDER_NAME;
import static java.util.Objects.requireNonNull;

public class InMemEventStores implements EventStoreProvisioner {

    private final Set<String> configuredDbs;

    private final ConcurrentMap<String, InMemEventStore> eventStores = new ConcurrentHashMap<>();

    public InMemEventStores(Set<String> configuredDbs) {
        this.configuredDbs = configuredDbs;
    }

    public InMemEventStore get(String esid) {
        return requireNonNull(eventStores.get(esid), () -> "EventStore " + esid + " not found");
    }

    public InMemEventStore create(SpiEventStoreDetails details) {
        return eventStores.computeIfAbsent(
                details.esid(),
                esid -> new InMemEventStore()
        );
    }

    @Override
    public SpiEventStoreDetails provision(ProvisionCommand command) {
        final var esid = command.esid();
        if (configuredDbs.contains(command.storageBackendName())) {
            return new SpiEventStoreDetails(
                    esid,
                    PROVIDER_NAME,
                    BackendType.IN_MEMORY,
                    command.dataFormat(),
                    EventStoreState.PROVISIONED
            );
        } else {
            throw new ResourceNotFoundException("Unknown backend " + command.storageBackendName());
        }
    }

    @Override
    public void drop(DropEventStoreCommand request) {
        eventStores.remove(request.esid());
    }


    public List<StorageBackendProperties> storageBackends() {
        return configuredDbs.stream()
                .map(dbAlias -> new StorageBackendProperties(
                        dbAlias,
                        BackendType.IN_MEMORY,
                        PROVIDER_NAME)
                ).toList();
    }

    public void close(String esid) {

    }
}

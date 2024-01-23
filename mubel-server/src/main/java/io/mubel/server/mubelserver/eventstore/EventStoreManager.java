package io.mubel.server.mubelserver.eventstore;

import io.mubel.api.grpc.DropEventStoreRequest;
import io.mubel.api.grpc.DropEventStoreResponse;
import io.mubel.api.grpc.EventStoreDetails;
import io.mubel.api.grpc.ProvisionEventStoreRequest;
import io.mubel.server.spi.Provider;
import io.mubel.server.spi.eventstore.EventStore;
import io.mubel.server.spi.eventstore.SpiEventStoreDetails;
import io.mubel.server.spi.exceptions.ResourceNotFoundException;
import io.mubel.server.spi.messages.MubelEventEnvelope;
import io.mubel.server.spi.messages.MubelEvents;
import io.mubel.server.spi.systemdb.EventStoreAliasRepository;
import io.mubel.server.spi.systemdb.EventStoreDetailsRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class EventStoreManager implements ApplicationListener<MubelEventEnvelope> {

    private static final Logger LOG = LoggerFactory.getLogger(EventStoreManager.class);
    private final EventStoreAliasRepository aliases;
    private final Map<String, EventStore> eventStores = new ConcurrentHashMap<>();

    private final List<Provider> providers;

    private final EventStoreDetailsRepository detailsRepository;


    public EventStoreManager(EventStoreAliasRepository aliases,
                             List<Provider> providers, EventStoreDetailsRepository detailsRepository
    ) {
        this.aliases = aliases;
        this.providers = providers;
        this.detailsRepository = detailsRepository;
    }

    public SpiEventStoreDetails provision(ProvisionEventStoreRequest request) {
        return providers.stream()
                .filter(provider -> hasBackend(provider, request.getStorageBackendName()))
                .findFirst()
                .orElseThrow(() -> new ResourceNotFoundException("No provider for backend name: " + request.getStorageBackendName()))
                .provision(request);
    }

    public DropEventStoreResponse drop(DropEventStoreRequest request) {
        final var esid = aliases.getEventStoreId(request.getEsid());
        final var details = detailsRepository.getByEsid(esid);
        final var provider = getProvider(details.provider());
        return provider.drop(request);
    }

    private boolean hasBackend(Provider provider, String storageBackendName) {
        return provider.storageBackends()
                .stream()
                .anyMatch(backend -> backend.getName().equals(storageBackendName));
    }

    public EventStore resolveEventStore(String esidOrAlias) {
        final var esid = aliases.getEventStoreId(esidOrAlias);
        final var es = eventStores.get(esid);
        if (es == null) {
            throw new ResourceNotFoundException("No event store found for esid: " + esid);
        }
        return es;
    }

    public void onApplicationEvent(MubelEventEnvelope envelope) {
        switch (envelope.event()) {
            case MubelEvents.EventStoreOpened eventStoreOpened -> handleOpened(eventStoreOpened);
            case MubelEvents.EventStoreClosed eventStoreClosed -> handleClosed(eventStoreClosed);
        }
    }

    private void handleClosed(MubelEvents.EventStoreClosed eventStoreClosed) {
        eventStores.remove(eventStoreClosed.esid());
        LOG.info("Event store closed: {}", eventStoreClosed.esid());
    }

    private void handleOpened(MubelEvents.EventStoreOpened eventStoreOpened) {
        eventStores.put(eventStoreOpened.esid(), eventStoreOpened.eventStore());
        LOG.info("Event store opened: {}", eventStoreOpened.esid());
    }

    public List<Provider> providers() {
        return List.copyOf(providers);
    }

    public List<EventStoreDetails> getAllEventStoreDetails() {
        return detailsRepository.getAll()
                .stream()
                .map(this::mapEventStoreDetails)
                .toList();
    }

    private EventStoreDetails mapEventStoreDetails(SpiEventStoreDetails src) {
        return EventStoreDetails.newBuilder()
                .setEsid(aliases.getAlias(src.esid()))
                .setDataFormat(src.dataFormat())
                .setType(src.type().name())
                .build();
    }

    private Provider getProvider(String providerName) {
        return providers.stream()
                .filter(provider -> provider.name().equals(providerName))
                .findFirst()
                .orElseThrow(() -> new ResourceNotFoundException("No provider found for name: " + providerName));
    }
}

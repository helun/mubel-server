package io.mubel.server.eventstore;

import io.mubel.api.grpc.*;
import io.mubel.server.Providers;
import io.mubel.server.spi.EventStoreContext;
import io.mubel.server.spi.Provider;
import io.mubel.server.spi.eventstore.EventStore;
import io.mubel.server.spi.exceptions.ResourceNotFoundException;
import io.mubel.server.spi.messages.EventStoreEventEnvelope;
import io.mubel.server.spi.messages.EventStoreEvents;
import io.mubel.server.spi.model.SpiEventStoreDetails;
import io.mubel.server.spi.systemdb.EventStoreAliasRepository;
import io.mubel.server.spi.systemdb.EventStoreDetailsRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class EventStoreManager implements ApplicationListener<EventStoreEventEnvelope> {

    private static final Logger LOG = LoggerFactory.getLogger(EventStoreManager.class);
    private final EventStoreAliasRepository aliases;
    private final Map<String, EventStoreContext> eventStores = new ConcurrentHashMap<>();

    private final Providers providers;

    private final EventStoreDetailsRepository detailsRepository;


    public EventStoreManager(EventStoreAliasRepository aliases,
                             Providers providers,
                             EventStoreDetailsRepository detailsRepository
    ) {
        this.aliases = aliases;
        this.providers = providers;
        this.detailsRepository = detailsRepository;
    }

    public EventStore resolveEventStore(String esidOrAlias) {
        return getEventStoreContext(esidOrAlias).eventStore();
    }

    private EventStoreContext getEventStoreContext(String esidOrAlias) {
        final var esid = aliases.getEventStoreId(esidOrAlias);
        final var context = eventStores.get(esid);
        if (context == null) {
            throw new ResourceNotFoundException("No event store found for esid: " + esid);
        }
        return context;
    }

    public void onApplicationEvent(EventStoreEventEnvelope envelope) {
        switch (envelope.event()) {
            case EventStoreEvents.EventStoreOpened eventStoreOpened -> handleOpened(eventStoreOpened);
            case EventStoreEvents.EventStoreClosed eventStoreClosed -> handleClosed(eventStoreClosed);
        }
    }

    private void handleClosed(EventStoreEvents.EventStoreClosed eventStoreClosed) {
        eventStores.remove(eventStoreClosed.esid());
        LOG.info("Event store closed: {}", eventStoreClosed.esid());
    }

    private void handleOpened(EventStoreEvents.EventStoreOpened eventStoreOpened) {
        eventStores.put(eventStoreOpened.esid(), eventStoreOpened.context());
        LOG.info("Event store opened: {}", eventStoreOpened.esid());
    }

    public List<Provider> providers() {
        return providers.all();
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

    public Flux<EventData> subscribe(SubscribeRequest request) {
        return EventDataSubscription.setupSubscription(request, getEventStoreContext(request.getEsid()));
    }

    public EventStoreSummary getSummary(GetEventStoreSummaryRequest request) {
        return eventStores.get(request.getEsid())
                .eventStore()
                .summary()
                .toBuilder()
                .setEsid(request.getEsid())
                .build();
    }

    public Flux<TriggeredEvents> subscribeToScheduledEvents(ScheduledEventsSubscribeRequest request) {
        return null;
    }
}

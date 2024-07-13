package io.mubel.server.eventstore;

import io.mubel.api.grpc.v1.events.Deadline;
import io.mubel.api.grpc.v1.events.DeadlineSubscribeRequest;
import io.mubel.api.grpc.v1.events.EventData;
import io.mubel.api.grpc.v1.events.SubscribeRequest;
import io.mubel.api.grpc.v1.server.EventStoreDetails;
import io.mubel.api.grpc.v1.server.EventStoreSummary;
import io.mubel.api.grpc.v1.server.GetEventStoreSummaryRequest;
import io.mubel.server.Providers;
import io.mubel.server.scheduling.ScheduledEventQueueNames;
import io.mubel.server.spi.EventStoreContext;
import io.mubel.server.spi.Provider;
import io.mubel.server.spi.eventstore.EventStore;
import io.mubel.server.spi.exceptions.ResourceNotFoundException;
import io.mubel.server.spi.messages.EventStoreEventEnvelope;
import io.mubel.server.spi.messages.EventStoreEvents;
import io.mubel.server.spi.model.SpiEventStoreDetails;
import io.mubel.server.spi.queue.ReceiveRequest;
import io.mubel.server.spi.systemdb.EventStoreAliasRepository;
import io.mubel.server.spi.systemdb.EventStoreDetailsRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;

import java.time.Duration;
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
        return getEventStoreContext(request.getEsid())
                .eventStore()
                .summary()
                .toBuilder()
                .setEsid(request.getEsid())
                .build();
    }

    public Flux<Deadline> subcribeToDeadlines(DeadlineSubscribeRequest request) {
        LOG.debug("subscribing to deadlines for esid: {}", request.getEsid());
        var ctx = getEventStoreContext(request.getEsid());
        var queue = ctx.scheduledEventsQueue();
        var receiveRequest = new ReceiveRequest(
                ScheduledEventQueueNames.deadlineQueueName(request.getEsid()),
                Duration.ofSeconds(20)
        );
        return queue.receive(receiveRequest)
                .map(DeadlineMapper::map);
    }

    public EventStoreContext eventStoreContext(String esid) {
        return getEventStoreContext(esid);
    }

}

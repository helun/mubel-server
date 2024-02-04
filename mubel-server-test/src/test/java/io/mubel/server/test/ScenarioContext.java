package io.mubel.server.test;

import io.mubel.api.grpc.EventDataInput;
import io.mubel.api.grpc.ServiceInfoResponse;
import io.mubel.server.test.steps.Configuration;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class ScenarioContext {

    private final Map<String, String> eventStores = new HashMap<>();
    private final Map<String, TestSubscription> subscriptions = new HashMap<>();
    private final Map<String, ScheduledEventSubscriber> scheduledEventsSubscriptions = new HashMap<>();
    private final Map<String, String> streams = new HashMap<>();
    private final Map<String, String> payloads = new HashMap<>();
    private final List<EventDataInput> givenEvents = new ArrayList<>();
    private final Map<String, ClientSession> sessions = new HashMap<>();
    private final Map<String, Object> attributes = new HashMap<>();

    private String latestPayload;
    private String lastStreamId;
    private ServiceInfoResponse serverInfo;

    private final Configuration configuration;

    public ScenarioContext(Configuration configuration) {
        this.configuration = configuration;
    }

    public void addEventStore(String alias, String esid) {
        eventStores.put(alias, esid);
    }

    public String getEventStore(String alias) {
        return eventStores.get(alias);
    }

    public Map<String, String> getEventStores() {
        return eventStores;
    }

    public String getEsid() {
        return eventStores.get("default");
    }

    public void addSubscription(String alias, TestSubscription sub) {
        subscriptions.put(alias, sub);
    }

    public TestSubscription getSubscription(String alias) {
        return subscriptions.get(alias);
    }

    public Collection<TestSubscription> getSubscriptions() {
        return subscriptions.values();
    }

    public void addStreamAlias(String streamAlias, String streamId) {
        streams.put(streamAlias, streamId);
    }

    public String getStreamId(String streamAlias) {
        return streams.get(streamAlias);
    }

    public String loadPayload(String path) {
        latestPayload = path;
        return payloads.computeIfAbsent(path, ScenarioContext::readFileAsString);
    }

    public String getLatestPayload() {
        return loadPayload(latestPayload);
    }

    public static String readFileAsString(String fileName) {
        try (InputStream inputStream = ScenarioContext.class.getClassLoader()
                .getResourceAsStream(fileName);
             Scanner scanner = new Scanner(inputStream, StandardCharsets.UTF_8)) {
            return scanner.useDelimiter("\\A").next();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void setLastStreamId(String streamId) {
        lastStreamId = streamId;
    }

    public String getLastStreamId() {
        return lastStreamId;
    }

    public void addGivenEvent(EventDataInput event) {
        this.givenEvents.add(event);
    }

    public List<EventDataInput> getGivenEvents() {
        return givenEvents;
    }

    public void setServerInfo(ServiceInfoResponse serverInfo) {
        this.serverInfo = serverInfo;
    }

    public ServiceInfoResponse getServerInfo() {
        return serverInfo;
    }

    public void setScheduledEventsSubscription(ScheduledEventSubscriber sub) {
        scheduledEventsSubscriptions.put("default", sub);
    }

    public ScheduledEventSubscriber getScheduledEventsSubscription() {
        return scheduledEventsSubscriptions.get("default");
    }

    public void putSession(String alias, ClientSession client) {
        sessions.put(alias, client);
    }

    public ClientSession getSession(String alias) {
        return sessions.computeIfAbsent(alias, a -> new ClientSession(configuration.newClient()));
    }

    public void putAttribute(String key, Object value) {
        attributes.put(key, value);
    }

    public Object getAttribute(String key) {
        return attributes.get(key);
    }

    public Collection<ClientSession> getSessions() {
        return sessions.values();
    }
}

package io.mubel.provider.jdbc.eventstore;

import org.jdbi.v3.core.Jdbi;

import java.util.HashMap;
import java.util.Map;

public class EventTypes {

    private final Jdbi jdbi;
    private final EventStoreStatements statements;
    private final Map<String, Integer> eventTypes = new HashMap<>();

    public EventTypes(Jdbi jdbi, EventStoreStatements statements) {
        this.jdbi = jdbi;
        this.statements = statements;
    }

    public int getEventTypeId(String eventType) {
        return eventTypes.computeIfAbsent(eventType, this::insertEventType);
    }

    public void init() {
        jdbi.useHandle(h -> h.createQuery(statements.getAllEventTypes())
                .map(rowView -> Map.entry(rowView.getColumn("type", String.class), rowView.getColumn("id", Integer.class)))
                .forEach(e -> eventTypes.put(e.getKey(), e.getValue()))
        );
    }

    private Integer insertEventType(String type) {
        return jdbi.withHandle(h ->
                        h.createUpdate(statements.insertEventType())
                                .bind(0, type)
                                .executeAndReturnGeneratedKeys("id")
                                .map((rs, ctx) -> rs.getInt(1)))
                .one();
    }
}

package io.mubel.provider.jdbc.eventstore;

import org.jdbi.v3.core.Jdbi;

import java.util.UUID;

public class RequestLog {

    private final EventStoreStatements statements;
    private final Jdbi jdbi;

    public RequestLog(Jdbi jdbi, EventStoreStatements statements) {
        this.statements = statements;
        this.jdbi = jdbi;
    }

    /**
     * @param requestId
     * @return false - if request already exists
     */
    public boolean log(UUID requestId) {
        return jdbi.withHandle(h -> h.createUpdate(statements.logRequestSql())
                .bind(0, requestId)
                .execute()) > 0;
    }
}

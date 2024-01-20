package io.mubel.provider.jdbc.eventstore;

import org.jdbi.v3.core.Jdbi;

public class SequenceNumber {
    private static final int START_VALUE = 0;
    private final Jdbi jdbi;
    private long sequenceNo = Long.MIN_VALUE;

    private final EventStoreStatements statements;

    public SequenceNumber(Jdbi jdbi, EventStoreStatements statements) {
        this.statements = statements;
        this.jdbi = jdbi;
    }

    public long next() {
        if (sequenceNo < START_VALUE) {
            throw new IllegalStateException("sequence no for " + statements.eventStoreName() + " has not been initialized");
        }
        sequenceNo++;
        return sequenceNo;
    }

    public void init() {
        sequenceNo = jdbi.withHandle(h -> {
            sequenceNo = h.createQuery(statements.getSequenceNoSql())
                    .mapTo(Long.class)
                    .one();
            return sequenceNo;
        });
    }
}

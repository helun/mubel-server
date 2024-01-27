package io.mubel.provider.jdbc.support;

import io.mubel.server.spi.exceptions.ResourceNotFoundException;
import io.mubel.server.spi.support.Repository;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.statement.Update;

import java.util.List;
import java.util.Optional;

public abstract class CrudRepositoryBase<T> implements Repository<T> {

    private final Jdbi jdbi;
    private final RepositoryStatements statements;
    private final Class<T> type;

    protected CrudRepositoryBase(Jdbi jdbi,
                                 RepositoryStatements statements,
                                 Class<T> type
    ) {
        this.jdbi = jdbi;
        this.statements = statements;
        this.type = type;
    }

    @Override
    public List<T> getAll() {
        return jdbi.withHandle(h ->
                h.createQuery(statements.selectAll())
                        .mapTo(type)
                        .list()
        );
    }

    @Override
    public T put(T value) {
        return jdbi.withHandle(h -> {
            bind(value, h.createUpdate(statements.upsert())).execute();
            return value;
        });
    }

    protected abstract Update bind(T value, Update update);

    @Override
    public T get(String key) {
        return jdbi.withHandle(h ->
                h.createQuery(statements.select())
                        .bind(0, key)
                        .mapTo(type)
                        .findFirst()
                        .orElseThrow(() -> new ResourceNotFoundException(type.getSimpleName() + ": " + key))
        );
    }

    @Override
    public Optional<T> find(String key) {
        return jdbi.withHandle(h ->
                h.createQuery(statements.select())
                        .bind(0, key)
                        .mapTo(type)
                        .findFirst()
        );
    }

    @Override
    public void remove(String key) {
        jdbi.useHandle(h ->
                h.createUpdate(statements.delete())
                        .bind(0, key)
                        .execute()
        );
    }

    @Override
    public boolean exists(String key) {
        return jdbi.withHandle(h ->
                h.createQuery(statements.exists())
                        .bind(0, key)
                        .mapTo(Boolean.class)
                        .one());
    }
}

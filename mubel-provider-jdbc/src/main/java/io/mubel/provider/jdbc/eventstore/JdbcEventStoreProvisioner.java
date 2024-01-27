package io.mubel.provider.jdbc.eventstore;

import io.mubel.api.grpc.DropEventStoreRequest;
import io.mubel.api.grpc.ProvisionEventStoreRequest;
import io.mubel.server.spi.eventstore.EventStoreProvisioner;
import io.mubel.server.spi.model.SpiEventStoreDetails;
import org.jdbi.v3.core.Jdbi;

import javax.sql.DataSource;

public class JdbcEventStoreProvisioner implements EventStoreProvisioner {

    private final DataSource dataSource;
    private final EventStoreStatements statements;

    public static void provision(DataSource ds, EventStoreStatements statements) {
        Jdbi jdbi = Jdbi.create(ds);
        jdbi.useHandle(handle -> {
            handle.execute(statements.ddl());
        });
    }

    public static void drop(DataSource ds, EventStoreStatements statements) {
        Jdbi jdbi = Jdbi.create(ds);
        jdbi.useHandle(handle -> {
            handle.execute(statements.dropSql());
        });
    }

    public JdbcEventStoreProvisioner(DataSource ds, EventStoreStatements statements) {
        this.dataSource = ds;
        this.statements = statements;
    }

    @Override
    public SpiEventStoreDetails provision(ProvisionEventStoreRequest request) {
        JdbcEventStoreProvisioner.provision(dataSource, statements);
        return null;
    }

    @Override
    public void drop(DropEventStoreRequest request) {

    }
}

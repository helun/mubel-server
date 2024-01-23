package io.mubel.provider.jdbc.eventstore;

import io.mubel.api.grpc.DropEventStoreRequest;
import io.mubel.api.grpc.ProvisionEventStoreRequest;
import io.mubel.server.spi.eventstore.EventStoreProvisioner;
import io.mubel.server.spi.eventstore.SpiEventStoreDetails;
import org.jdbi.v3.core.Jdbi;

import javax.sql.DataSource;

public class JdbcEventStoreProvisioner implements EventStoreProvisioner {

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

    @Override
    public SpiEventStoreDetails provision(ProvisionEventStoreRequest request) {
        return null;
    }

    @Override
    public void drop(DropEventStoreRequest request) {

    }
}

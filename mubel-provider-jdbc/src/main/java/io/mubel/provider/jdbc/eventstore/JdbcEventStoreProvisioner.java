package io.mubel.provider.jdbc.eventstore;

import io.mubel.server.spi.eventstore.EventStoreProvisioner;
import io.mubel.server.spi.model.DropEventStoreCommand;
import io.mubel.server.spi.model.ProvisionCommand;
import io.mubel.server.spi.model.SpiEventStoreDetails;
import org.jdbi.v3.core.Jdbi;

import javax.sql.DataSource;

public class JdbcEventStoreProvisioner implements EventStoreProvisioner {

    private final DataSource dataSource;
    private final EventStoreStatements statements;

    public static void provision(DataSource ds, EventStoreStatements statements) {
        Jdbi jdbi = Jdbi.create(ds);
        jdbi.useHandle(h -> statements.ddl().forEach(h::execute));
    }

    public static void drop(DataSource ds, EventStoreStatements statements) {
        Jdbi jdbi = Jdbi.create(ds);

        jdbi.useHandle(h -> statements.dropSql().forEach(h::execute));
    }

    public JdbcEventStoreProvisioner(DataSource ds, EventStoreStatements statements) {
        this.dataSource = ds;
        this.statements = statements;
    }

    @Override
    public SpiEventStoreDetails provision(ProvisionCommand request) {
        JdbcEventStoreProvisioner.provision(dataSource, statements);
        return null;
    }

    @Override
    public void drop(DropEventStoreCommand command) {
        JdbcEventStoreProvisioner.drop(dataSource, statements);
    }
}

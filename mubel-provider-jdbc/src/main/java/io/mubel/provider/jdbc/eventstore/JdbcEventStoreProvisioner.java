package io.mubel.provider.jdbc.eventstore;

import io.mubel.provider.jdbc.support.SqlStatements;
import io.mubel.server.spi.eventstore.EventStoreProvisioner;
import io.mubel.server.spi.model.DropEventStoreCommand;
import io.mubel.server.spi.model.ProvisionCommand;
import io.mubel.server.spi.model.SpiEventStoreDetails;
import org.jdbi.v3.core.Jdbi;

import javax.sql.DataSource;

public class JdbcEventStoreProvisioner implements EventStoreProvisioner {

    private final DataSource dataSource;
    private final SqlStatements provisionStatements;
    private final SqlStatements dropStatements;

    public static void provision(DataSource ds, SqlStatements statements) {
        Jdbi jdbi = Jdbi.create(ds);
        jdbi.useTransaction(h -> statements.forEach(h::execute));
    }

    public static void drop(DataSource ds, SqlStatements statements) {
        Jdbi jdbi = Jdbi.create(ds);

        jdbi.useTransaction(h -> statements.forEach(h::execute));
    }

    public JdbcEventStoreProvisioner(DataSource ds,
                                     SqlStatements provisionStatements,
                                     SqlStatements dropStatements) {
        this.dataSource = ds;
        this.provisionStatements = provisionStatements;
        this.dropStatements = dropStatements;
    }

    @Override
    public SpiEventStoreDetails provision(ProvisionCommand request) {
        JdbcEventStoreProvisioner.provision(dataSource, provisionStatements);
        return null;
    }

    @Override
    public void drop(DropEventStoreCommand command) {
        JdbcEventStoreProvisioner.drop(dataSource, dropStatements);
    }
}

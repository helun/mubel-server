package io.mubel.provider.jdbc.systemdb;

import io.mubel.provider.jdbc.support.CrudRepositoryBase;
import io.mubel.server.spi.model.SpiEventStoreDetails;
import io.mubel.server.spi.systemdb.EventStoreDetailsRepository;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.statement.Update;

public class JdbcEventStoreDetailsRepository extends CrudRepositoryBase<SpiEventStoreDetails> implements EventStoreDetailsRepository {
    
    public JdbcEventStoreDetailsRepository(Jdbi jdbi, EventStoreDetailsStatements statements) {
        super(jdbi, statements, SpiEventStoreDetails.class);
    }

    @Override
    protected Update bind(SpiEventStoreDetails value, Update update) {
        return update.bind(0, value.esid())
                .bind(1, value.provider())
                .bind(2, value.type())
                .bind(3, value.dataFormat())
                .bind(4, value.state());
    }
}

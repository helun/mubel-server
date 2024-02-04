package io.mubel.provider.inmemory.systemdb;

import io.mubel.server.spi.model.SpiEventStoreDetails;
import io.mubel.server.spi.systemdb.EventStoreDetailsRepository;

public class InMemEventStoreDetailsRepository extends InMemCrudRepository<SpiEventStoreDetails> implements EventStoreDetailsRepository {

    @Override
    public SpiEventStoreDetails put(SpiEventStoreDetails value) {
        data.put(value.esid(), value);
        return value;
    }

}

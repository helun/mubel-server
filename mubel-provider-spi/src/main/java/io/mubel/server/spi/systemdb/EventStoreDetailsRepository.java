package io.mubel.server.spi.systemdb;

import io.mubel.server.spi.eventstore.SpiEventStoreDetails;

import java.util.List;

public interface EventStoreDetailsRepository {
    List<SpiEventStoreDetails> getAll();

    SpiEventStoreDetails getByEsid(String esid);
}

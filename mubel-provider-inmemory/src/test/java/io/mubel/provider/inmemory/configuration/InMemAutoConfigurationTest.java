package io.mubel.provider.inmemory.configuration;

import io.mubel.api.grpc.v1.server.DataFormat;
import io.mubel.provider.inmemory.InMemProviderTestApplication;
import io.mubel.server.spi.Provider;
import io.mubel.server.spi.model.ProvisionCommand;
import io.mubel.server.spi.systemdb.EventStoreDetailsRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(classes = InMemProviderTestApplication.class)
@TestPropertySource("classpath:application-test-properties")
class InMemAutoConfigurationTest {

    @Autowired
    EventStoreDetailsRepository eventStoreDetailsRepository;

    @Autowired
    Provider provider;

    @Test
    void provision() {
        String esid = "test-esid";
        var command = new ProvisionCommand(
                UUID.randomUUID().toString(),
                esid,
                DataFormat.JSON,
                "in-memory"
        );
        provider.provision(command);
        var context = provider.openEventStore(esid);
        assertThat(context).isNotNull();
        assertThat(context.eventStore()).isNotNull();
        assertThat(context.replayService()).isNotNull();
        assertThat(context.liveEventsService()).isNotNull();
    }

}
package io.mubel.provider.test.systemdb;

import io.mubel.api.grpc.v1.server.DataFormat;
import io.mubel.server.spi.eventstore.EventStoreState;
import io.mubel.server.spi.exceptions.ResourceNotFoundException;
import io.mubel.server.spi.model.BackendType;
import io.mubel.server.spi.model.SpiEventStoreDetails;
import io.mubel.server.spi.systemdb.EventStoreDetailsRepository;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
public abstract class EventStoreDetailsRepositoryTestBase {

    protected abstract EventStoreDetailsRepository repository();

    @Test
    void crud() {
        var details = getSpiEventStoreDetails();
        assertThat(repository().put(details)).isEqualTo(details);
        assertThat(repository().exists("esid")).isTrue();
        assertThat(repository().get("esid")).isEqualTo(details);
        repository().remove("esid");
        assertThat(repository().find("esid")).isEmpty();
    }

    @Test
    void when_details_exists_then_put_updates_existing_entity() {
        var details = getSpiEventStoreDetails();
        assertThat(repository().put(details)).isEqualTo(details);
        var droppedDetails = details.withState(EventStoreState.DROPPING);
        assertThat(repository().put(droppedDetails)).isEqualTo(droppedDetails);
        assertThat(repository().get("esid")).isEqualTo(droppedDetails);
    }

    @Test
    void get_non_existing_throws_ResourceNotFoundException() {
        assertThatThrownBy(() -> repository().get("missing"))
                .isInstanceOf(ResourceNotFoundException.class);
    }

    private static SpiEventStoreDetails getSpiEventStoreDetails() {
        var details = new SpiEventStoreDetails(
                "esid",
                "test-provider",
                BackendType.IN_MEMORY,
                DataFormat.JSON,
                EventStoreState.PROVISIONED
        );
        return details;
    }
}

package io.mubel.server.eventstore;

import io.mubel.api.grpc.v1.server.DataFormat;
import io.mubel.api.grpc.v1.server.JobState;
import io.mubel.api.grpc.v1.server.JobStatus;
import io.mubel.provider.inmemory.systemdb.InMemEventStoreAliasRepository;
import io.mubel.provider.inmemory.systemdb.InMemEventStoreDetailsRepository;
import io.mubel.server.Providers;
import io.mubel.server.TestProvider;
import io.mubel.server.spi.eventstore.EventStoreState;
import io.mubel.server.spi.exceptions.ResourceNotFoundException;
import io.mubel.server.spi.model.ProvisionCommand;
import io.mubel.server.spi.model.SpiEventStoreDetails;
import io.mubel.server.spi.systemdb.EventStoreAliasRepository;
import io.mubel.server.spi.systemdb.EventStoreDetailsRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.context.ApplicationEventPublisher;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class ProvisionServiceTest {

    ProvisionService service;

    EventStoreDetailsRepository detailsRepository = new InMemEventStoreDetailsRepository();

    EventStoreAliasRepository aliases = new InMemEventStoreAliasRepository();

    @Mock
    ApplicationEventPublisher publisher;
    @Captor
    ArgumentCaptor<JobStatus> publishCaptor;

    TestProvider provider = new TestProvider();


    @BeforeEach
    void setup() {
        var providers = new Providers(List.of(provider));
        service = new ProvisionService(
                providers,
                detailsRepository,
                aliases,
                publisher
        );
    }

    @Test
    void provision_reports_job_status_and_opens_the_new_event_store() {
        var command = new ProvisionCommand(
                "job-id",
                "esid",
                DataFormat.JSON,
                TestProvider.TEST_BACKEND);
        assertThat(service.provision(command)).isDone();
        assertThat(provider.hasOpenEventStore(command.esid()))
                .as("The event store should be opened")
                .isTrue();
        verify(publisher, times(2)).publishEvent(publishCaptor.capture());
        assertPublishedStates(JobState.RUNNING, JobState.COMPLETED);
        assertThat(detailsRepository.get(command.esid()))
                .as("The event store details should be stored")
                .extracting(SpiEventStoreDetails::state)
                .isEqualTo(EventStoreState.PROVISIONED);
    }

    private void assertPublishedStates(JobState... expectedStates) {
        assertThat(publishCaptor.getAllValues()
                .stream()
                .map(JobStatus::getState)
                .toList())
                .as("The job status should be reported")
                .containsExactly(expectedStates);
    }

    @Test
    void failed_provision_reports_failed_job_status_and_fails_future() {
        var command = new ProvisionCommand(
                "job-id",
                "esid",
                DataFormat.JSON,
                "non-existing-backend");
        assertThat(service.provision(command))
                .failsWithin(Duration.ofSeconds(1))
                .withThrowableThat()
                .withCauseInstanceOf(ResourceNotFoundException.class);
        verify(publisher, times(2)).publishEvent(publishCaptor.capture());
        assertPublishedStates(JobState.RUNNING, JobState.FAILED);
    }

}
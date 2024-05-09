package io.mubel.server.spi.queue;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;


class QueueConfigurationsTest {

    @Test
    void dynamic_configuration() {
        Duration defaultVisibilityTimeout = Duration.ofMinutes(1);
        Duration defaultPollIntervall = Duration.ofMillis(500);
        var configs = new QueueConfigurations(List.of(
                new QueueConfiguration("defaults",
                        defaultVisibilityTimeout,
                        defaultPollIntervall
                )
        ));
        var config = configs.getQueue("test", "defaults");
        assertThat(config.name()).isEqualTo("test");
        assertThat(config.visibilityTimeout()).isEqualTo(defaultVisibilityTimeout);
        assertThat(config.polIInterval()).isEqualTo(defaultPollIntervall);
    }

}
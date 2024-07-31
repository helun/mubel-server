package io.mubel.server.configuration;

import io.mubel.server.spi.groups.GroupsProperties;
import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

@ConfigurationProperties(prefix = "mubel")
public record MubelConfigurationProperties(
        QueueConfigParams deadlines,
        @NotNull GroupsProperties groups
) {

    public record QueueConfigParams(
            Visibility visibility,
            Polling polling
    ) {
        public record Visibility(Duration timeout) {

        }

        public record Polling(Duration interval) {

        }
    }


}

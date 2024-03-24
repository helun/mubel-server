package io.mubel.server.configuration;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

@ConfigurationProperties(prefix = "mubel")
public record MubelConfigurationProperties(
        ScheduledEvents scheduledEvents
) {

    public record ScheduledEvents(
            Visibility visibility
    ) {
        public record Visibility(Duration timeout) {

        }
    }


}

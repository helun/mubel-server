package io.mubel.server.spi.groups;

import java.time.Duration;

public record GroupsProperties(
        Duration heartbeatInterval
) {
}

package io.mubel.server.spi.groups;

public record Heartbeat(String groupId, String token) implements GroupRequest {
}

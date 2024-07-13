package io.mubel.server.spi.groups;

public record JoinRequest(String esid, String groupId, String token) implements GroupRequest {
}

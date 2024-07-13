package io.mubel.server.spi.groups;

public record LeaveRequest(String groupId, String token) implements GroupRequest {
}

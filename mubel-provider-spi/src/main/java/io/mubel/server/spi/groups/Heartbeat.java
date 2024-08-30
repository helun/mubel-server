package io.mubel.server.spi.groups;

public record Heartbeat(String groupId, String token) implements GroupRequest {

    public static Heartbeat from(JoinRequest request) {
        return new Heartbeat(request.groupId(), request.token());
    }

}

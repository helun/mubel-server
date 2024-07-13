package io.mubel.provider.inmemory.groups;

import io.mubel.server.spi.groups.Heartbeat;

public record HeartbeatMessage(Heartbeat heartbeat) implements GroupMessage {

}

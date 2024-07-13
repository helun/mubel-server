package io.mubel.provider.inmemory.groups;

import io.mubel.server.spi.groups.LeaveRequest;

public record LeaveMessage(LeaveRequest leaveRequest) implements GroupMessage {
}

package io.mubel.provider.inmemory.groups;

public sealed interface GroupMessage permits HeartbeatMessage, JoinSession, LeaveMessage, LeaderQuery, CheckGroupsMessage {
}

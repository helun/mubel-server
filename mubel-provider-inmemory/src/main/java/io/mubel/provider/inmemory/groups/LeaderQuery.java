package io.mubel.provider.inmemory.groups;

import io.mubel.api.grpc.v1.groups.GroupStatus;
import reactor.core.publisher.Sinks;

import java.util.Optional;

public final class LeaderQuery implements GroupMessage {

    private final String groupId;
    private final Sinks.One<GroupStatus> responseSink = Sinks.one();

    public LeaderQuery(String groupId) {
        this.groupId = groupId;
    }

    public String groupId() {
        return groupId;
    }

    public void result(GroupStatus result) {
        responseSink.tryEmitValue(result);
    }

    public void empty() {
        responseSink.tryEmitEmpty();
    }

    public Optional<GroupStatus> response() {
        return responseSink.asMono()
                .blockOptional();
    }
}

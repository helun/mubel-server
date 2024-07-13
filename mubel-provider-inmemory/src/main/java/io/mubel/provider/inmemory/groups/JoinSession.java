package io.mubel.provider.inmemory.groups;

import io.mubel.api.grpc.v1.groups.GroupStatus;
import io.mubel.server.spi.groups.JoinRequest;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

public final class JoinSession implements GroupMessage {

    private final JoinRequest joinRequest;

    private final Sinks.Many<GroupStatus> stateSink = Sinks.many().unicast().onBackpressureBuffer();

    public JoinSession(JoinRequest join) {
        this.joinRequest = join;
    }

    public void next(GroupStatus state) {
        stateSink.tryEmitNext(state);
    }

    public JoinRequest joinRequest() {
        return joinRequest;
    }

    public Flux<GroupStatus> response() {
        return stateSink.asFlux();
    }
}

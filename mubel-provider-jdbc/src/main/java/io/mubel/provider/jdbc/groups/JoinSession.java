package io.mubel.provider.jdbc.groups;

import io.mubel.api.grpc.v1.groups.GroupStatus;
import io.mubel.server.spi.groups.JoinRequest;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.time.Instant;

public final class JoinSession {

    private final JoinRequest joinRequest;
    private final Instant joinTime;

    private final Sinks.Many<GroupStatus> stateSink = Sinks.many().unicast().onBackpressureBuffer();

    public JoinSession(JoinRequest join, Instant joinTime) {
        this.joinRequest = join;
        this.joinTime = joinTime;
    }

    public void next(GroupStatus state) {
        stateSink.tryEmitNext(state);
    }

    public JoinRequest joinRequest() {
        return joinRequest;
    }

    public Instant joinTime() {
        return joinTime;
    }

    public Flux<GroupStatus> response() {
        return stateSink.asFlux();
    }

    public void complete() {
        stateSink.tryEmitComplete();
    }
}

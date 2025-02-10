/*
 * mubel-provider-inmemory - Multi Backend Event Log
 * Copyright © 2025 Henrik Barratt-Due (henrikbd@hey.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.mubel.provider.inmemory.groups;

import io.mubel.api.grpc.v1.groups.GroupStatus;
import io.mubel.server.spi.groups.JoinRequest;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.time.Instant;

public final class JoinSession implements GroupMessage {

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

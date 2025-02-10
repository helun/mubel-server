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
package io.mubel.provider.inmemory.eventstore;

import io.mubel.api.grpc.v1.events.EventData;
import io.mubel.api.grpc.v1.events.GetEventsRequest;
import io.mubel.api.grpc.v1.events.SubscribeRequest;
import io.mubel.server.spi.eventstore.ReplayService;
import reactor.core.publisher.Flux;

import static java.util.Objects.requireNonNull;

public class InMemReplayService implements ReplayService {

    private final InMemEventStores eventStores;

    public InMemReplayService(InMemEventStores eventStores) {
        this.eventStores = requireNonNull(eventStores);
    }

    @Override
    public Flux<EventData> replay(SubscribeRequest request) {
        final var es = eventStores.get(request.getEsid());
        final var response = es.get(GetEventsRequest.newBuilder()
                .setEsid(request.getEsid())
                .setSelector(request.getSelector())
                .setSize(0)
                .build());
        return Flux.fromIterable(response.getEventList());
    }
}

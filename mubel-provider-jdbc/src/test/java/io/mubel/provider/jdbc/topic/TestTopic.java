/*
 * mubel-provider-jdbc - mubel-provider-jdbc
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
package io.mubel.provider.jdbc.topic;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.util.concurrent.atomic.AtomicInteger;

public class TestTopic implements Topic {

    private final AtomicInteger consumerCount = new AtomicInteger();
    private final Sinks.Many<String> sink = Sinks.many().multicast().directBestEffort();

    @Override
    public void publish(String message) {
        sink.tryEmitNext(message);
    }

    @Override
    public Flux<String> consumer() {
        return sink.asFlux()
                .doOnSubscribe(subscription -> consumerCount.incrementAndGet())
                .doFinally(signalType -> consumerCount.decrementAndGet());
    }

    @Override
    public int consumerCount() {
        return consumerCount.get();
    }
}

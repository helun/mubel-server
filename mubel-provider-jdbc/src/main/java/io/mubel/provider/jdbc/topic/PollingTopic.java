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

import org.jdbi.v3.core.Jdbi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.util.annotation.NonNull;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class PollingTopic implements io.mubel.provider.jdbc.topic.Topic {

    private static final Logger LOG = LoggerFactory.getLogger(PollingTopic.class);

    private final String topicName;
    private final Jdbi jdbi;
    private final Scheduler scheduler;
    private final int pollingIntervalMs;

    private final AtomicReference<Flux<String>> messagesRef = new AtomicReference<>();
    private final AtomicBoolean shouldRun = new AtomicBoolean(true);
    private final AtomicInteger consumerCount = new AtomicInteger(0);
    private final AtomicLong lastId = new AtomicLong(0);

    public PollingTopic(String topicName, int pollingIntervalMs, Jdbi jdbi, Scheduler scheduler) {
        this.topicName = topicName;
        this.pollingIntervalMs = pollingIntervalMs;
        this.jdbi = jdbi;
        this.scheduler = scheduler;
    }

    @Override
    public void publish(String message) {
        jdbi.useHandle(h -> h.execute("INSERT INTO system_messages (topic, message) VALUES (?, ?)", topicName, message));
    }

    @Override
    public Flux<String> consumer() {
        return messagesRef.updateAndGet(flux -> flux != null ? flux : initMessageFlux());
    }

    public int consumerCount() {
        return consumerCount.get();
    }

    private Flux<String> initMessageFlux() {
        LOG.debug("initializing topic {}", topicName);
        shouldRun.set(true);
        initLastMessageId();
        return Flux.interval(Duration.ofMillis(pollingIntervalMs), scheduler)
                .flatMapIterable(i -> findNewMessages())
                .share()
                .doOnSubscribe(sub -> consumerCount.incrementAndGet())
                .doOnCancel(consumerCount::decrementAndGet)
                .onBackpressureError();
    }

    @NonNull
    private Iterable<String> findNewMessages() {
        if (!shouldRun()) {
            return List.of();
        }
        return jdbi.withHandle(h -> h.createQuery("SELECT id, message FROM system_messages WHERE topic = ? AND id > ? ORDER BY id")
                .bind(0, topicName)
                .bind(1, lastId.get())
                .map((rowView) -> {
                    lastId.set(rowView.getColumn("id", Long.class));
                    return rowView.getColumn("message", String.class);
                }).list()
        );
    }

    private void initLastMessageId() {
        var maxId = jdbi.withHandle(h -> h.createQuery("SELECT MAX(id) FROM system_messages WHERE topic = ?")
                .bind(0, topicName)
                .mapTo(Long.class)
                .findFirst()
                .orElse(0L)
        );
        lastId.set(maxId);
    }

    private boolean shouldRun() {
        return !Thread.currentThread().isInterrupted() && shouldRun.get();
    }

}

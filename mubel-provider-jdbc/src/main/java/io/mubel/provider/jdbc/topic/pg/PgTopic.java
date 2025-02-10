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
package io.mubel.provider.jdbc.topic.pg;

import io.mubel.provider.jdbc.topic.Topic;
import org.postgresql.PGNotification;
import org.postgresql.jdbc.PgConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.scheduler.Scheduler;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class PgTopic implements Topic {

    private static final Logger LOG = LoggerFactory.getLogger(PgTopic.class);
    private static final Set<String> CLOSED_SQL_STATES = Set.of("08003", "08006");
    public static final int MESSAGE_MAX_LENGTH = 8000;

    private final String topicName;
    private final DataSource dataSource;
    private final Scheduler scheduler;

    private final AtomicReference<Flux<String>> messagesRef = new AtomicReference<>();
    private final AtomicBoolean shouldRun = new AtomicBoolean(true);
    private final AtomicInteger consumerCount = new AtomicInteger(0);
    private final AtomicBoolean listening = new AtomicBoolean(false);

    private final String listenSql;

    public PgTopic(String topicName,
                   DataSource dataSource,
                   Scheduler scheduler
    ) {
        this.topicName = topicName;
        this.dataSource = dataSource;
        this.scheduler = scheduler;
        listenSql = "LISTEN " + topicName;
    }

    public Flux<String> consumer() {
        return messagesRef.updateAndGet(flux -> Objects.requireNonNullElseGet(flux, this::initMessageFlux));
    }

    public void publish(String message) {
        var validated = validate(message);
        LOG.debug("publishing message: {}", validated);
        try (var connection = dataSource.getConnection()) {
            try (var pstmt = connection.createStatement()) {
                pstmt.execute("NOTIFY %s, '%s'".formatted(topicName, validated));
            }
        } catch (SQLException e) {
            LOG.error("Error publishing message", e);
            throw new RuntimeException(e);
        }
    }

    public int consumerCount() {
        return consumerCount.get();
    }

    private String validate(String input) {
        if (input == null) {
            return "";
        }
        if (input.length() > MESSAGE_MAX_LENGTH) {
            throw new IllegalArgumentException("Message too long");
        }
        return input;
    }

    private Flux<String> initMessageFlux() {
        LOG.debug("initializing topic {}", topicName);
        shouldRun.set(true);
        return Flux.<String>push(emitter -> {
                    try {
                        while (shouldRun()) {
                            consume(emitter);
                        }
                        emitter.complete();
                    } catch (Exception e) {
                        LOG.error("error in live events service", e);
                        emitter.error(e);
                    }
                }).subscribeOn(scheduler)
                .share()
                .doOnSubscribe(sub -> consumerCount.incrementAndGet())
                .doOnCancel(consumerCount::decrementAndGet)
                .onBackpressureError();
    }

    private void disposeMessageFlux() {
        LOG.debug("disposing message flux for topic {}", topicName);
        messagesRef.set(null);
    }

    private void consume(FluxSink<String> emitter) throws SQLException {
        try (var connection = dataSource.getConnection()) {
            PgConnection pgConnection = connection.unwrap(PgConnection.class);
            try (final var stmt = pgConnection.createStatement()) {
                LOG.debug("listening to channel {}", topicName);
                stmt.execute(listenSql);
                listening.set(true);
                while (shouldRun(emitter, pgConnection)) {
                    var notifications = pgConnection.getNotifications(2000);
                    if (hasNotification(notifications)) {
                        publishNotifications(emitter, notifications);
                    }
                }
            }
        } catch (SQLException sqle) {
            if (CLOSED_SQL_STATES.contains(sqle.getSQLState())) {
                LOG.warn("connection closed");
            } else {
                throw sqle;
            }
        } finally {
            listening.set(false);
            LOG.debug("stopped listening to channel {}", topicName);
            disposeMessageFlux();
        }
    }

    private void publishNotifications(FluxSink<String> emitter, PGNotification[] notifications) {
        LOG.debug("received notifications: {}", notifications.length);
        for (PGNotification notification : notifications) {
            emitter.next(notification.getParameter());
        }
    }

    private static boolean hasNotification(PGNotification[] notifications) {
        return notifications != null && notifications.length > 0;
    }

    private boolean shouldRun() {
        return !Thread.currentThread().isInterrupted() && shouldRun.get();
    }

    private boolean shouldRun(FluxSink<?> emitter, PgConnection pgConnection) throws SQLException {
        return shouldRun() && pgConnection.isValid(250) && !emitter.isCancelled();
    }

    public boolean listening() {
        return listening.get();
    }

    public void dispose() {
        shouldRun.set(false);
        disposeMessageFlux();
    }
}

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

import io.mubel.provider.jdbc.Containers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.core.Disposable;
import reactor.core.scheduler.Schedulers;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.awaitility.Awaitility.await;

@Testcontainers
class PgTopicTest {

    static final String TOPIC_NAME = "test_topic";

    @Container
    static PostgreSQLContainer<?> container = Containers.postgreSQLContainer();

    PgTopic topic;

    @BeforeAll
    static void setup() {

    }

    @BeforeEach
    void start() {
        var dataSource = Containers.dataSource(container);
        topic = new PgTopic(TOPIC_NAME, dataSource, Schedulers.boundedElastic());
    }

    @AfterEach
    void tearDown() {
        topic.dispose();
    }

    @Test
    void consumer_receives_message() throws InterruptedException {
        BlockingQueue<String> received = new ArrayBlockingQueue<>(10);
        var disposable = topic.consumer()
                .subscribe(received::add, err -> fail("sub failed", err));
        await().until(() -> topic.consumerCount() == 1);
        String message = "message one";
        topic.publish(message);
        assertMessagePublished(received, disposable, message);
    }

    @Test
    void consumer_cancels_and_consumes_again() throws InterruptedException {
        var d1 = topic.consumer().subscribe(System.out::println, err -> fail("sub failed", err));
        await().until(() -> topic.consumerCount() == 1);
        d1.dispose();
        await().untilAsserted(() -> {
            assertThat(topic.consumerCount()).isZero();
            assertThat(topic.listening()).isFalse();
        });

        BlockingQueue<String> received = new ArrayBlockingQueue<>(10);
        var d2 = topic.consumer().subscribe(received::add, err -> fail("sub failed", err));
        await().until(() -> topic.consumerCount() == 1);
        await().untilAsserted(() -> assertThat(topic.listening()).isTrue());
        String message = "message one";
        topic.publish(message);
        assertMessagePublished(received, d2, message);
    }

    @Test
    void multiple_consumers() {
        BlockingQueue<String> r1 = new ArrayBlockingQueue<>(10);
        var d1 = topic.consumer().subscribe(r1::add, err -> fail("sub failed", err));
        BlockingQueue<String> r2 = new ArrayBlockingQueue<>(10);
        var d2 = topic.consumer().subscribe(r2::add, err -> fail("sub failed", err));
        await().untilAsserted(() ->
                assertThat(topic.consumerCount())
                        .as("two consumers expected")
                        .isEqualTo(2)
        );
        await().untilAsserted(() -> assertThat(topic.listening()).isTrue());

        topic.publish("m1");
        topic.publish("m2");
        topic.publish("m3");
        await().untilAsserted(() -> {
            assertThat(r1).containsExactly("m1", "m2", "m3");
            assertThat(r2).containsExactly("m1", "m2", "m3");
        });
        d1.dispose();
        d2.dispose();
    }

    private static void assertMessagePublished(BlockingQueue<String> received, Disposable disposable, String message) throws InterruptedException {
        var published = received.poll(3, TimeUnit.SECONDS);
        disposable.dispose();
        assertThat(published).isEqualTo(message);
    }

}
package io.mubel.provider.jdbc.topic;

import org.junit.jupiter.api.Test;
import reactor.core.Disposable;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.awaitility.Awaitility.await;

public abstract class TopicTestBase {

    protected static final String TOPIC_NAME = "test_topic";

    abstract protected Topic topic();

    @Test
    void consumer_receives_message() throws InterruptedException {
        BlockingQueue<String> received = new ArrayBlockingQueue<>(10);
        var disposable = topic().consumer()
                .subscribe(received::add, err -> fail("sub failed", err));
        await().until(() -> topic().consumerCount() == 1);
        String message = "message one";
        topic().publish(message);
        assertMessagePublished(received, disposable, message);
    }

    @Test
    void consumer_cancels_and_consumes_again() throws InterruptedException {
        var d1 = topic().consumer().subscribe(System.out::println, err -> fail("sub failed", err));
        await().until(() -> topic().consumerCount() == 1);
        d1.dispose();
        await().untilAsserted(() -> {
            assertThat(topic().consumerCount()).isZero();
        });

        BlockingQueue<String> received = new ArrayBlockingQueue<>(10);
        var d2 = topic().consumer().subscribe(received::add, err -> fail("sub failed", err));
        await().until(() -> topic().consumerCount() == 1);
        String message = "message one";
        topic().publish(message);
        assertMessagePublished(received, d2, message);
    }

    @Test
    void multiple_consumers() {
        BlockingQueue<String> r1 = new ArrayBlockingQueue<>(10);
        var d1 = topic().consumer().subscribe(r1::add, err -> fail("sub failed", err));
        BlockingQueue<String> r2 = new ArrayBlockingQueue<>(10);
        var d2 = topic().consumer().subscribe(r2::add, err -> fail("sub failed", err));
        await().untilAsserted(() ->
                assertThat(topic().consumerCount())
                        .as("two consumers expected")
                        .isEqualTo(2)
        );

        topic().publish("m1");
        topic().publish("m2");
        topic().publish("m3");
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

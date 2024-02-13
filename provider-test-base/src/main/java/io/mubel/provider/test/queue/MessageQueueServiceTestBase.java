package io.mubel.provider.test.queue;

import io.mubel.server.spi.queue.Message;
import io.mubel.server.spi.queue.MessageQueueService;
import io.mubel.server.spi.queue.ReceiveRequest;
import io.mubel.server.spi.queue.SendRequest;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
public abstract class MessageQueueServiceTestBase {

    public static final String QUEUE_NAME = "test-queue";
    public static final String TYPE = "test-type";
    public static final String PAYLOAD = "test-payload";

    protected abstract Duration getVisibilityTimeout();

    protected abstract MessageQueueService service();

    @Test
    void poll_until_timeout_ends_Flux() {
        var request = new ReceiveRequest(QUEUE_NAME, Duration.ofMillis(1000));
        var delayed = service().receive(request);
        await().atMost(request.timeout().plusMillis(100))
                .untilAsserted(() -> assertThat(delayed.blockLast()).isNull());
    }

    @Test
    void polled_items_reappear_in_the_queue_if_not_deleted_within_visibility_timeout() {
        var queue = service();
        queue.send(getSendRequest());
        var request = new ReceiveRequest(QUEUE_NAME, Duration.ofMillis(1000));
        var message = queue.receive(request).blockFirst();

        assertReceived(message);

        await().atLeast(getVisibilityTimeout())
                .untilAsserted(() -> {
                    var message2 = queue.receive(new ReceiveRequest(QUEUE_NAME, getVisibilityTimeout().plusMillis(100)))
                            .blockFirst();
                    assertThat(message2).isNotNull();
                    assertThat(message2).isEqualTo(message);
                });

    }

    @Test
    void deleted_messages_does_not_reappear_in_the_queue() {
        var queue = service();
        var sendRequest = getSendRequest();

        queue.send(sendRequest);
        var request = new ReceiveRequest(QUEUE_NAME, Duration.ofMillis(1000));
        var message = queue.receive(request).blockFirst();

        assertReceived(message);

        queue.delete(List.of(message.messageId()));

        await().atLeast(getVisibilityTimeout())
                .untilAsserted(() -> {
                    var message2 = queue.receive(new ReceiveRequest(QUEUE_NAME, getVisibilityTimeout().plusMillis(100)))
                            .blockFirst();
                    assertThat(message2).isNull();
                });
    }

    private static void assertReceived(Message message) {
        assertThat(message).isNotNull();
        assertThat(message.messageId()).isNotNull();
        assertThat(new String(message.payload())).isEqualTo(PAYLOAD);
        assertThat(message.type()).isEqualTo(TYPE);
    }

    private static SendRequest getSendRequest() {
        return SendRequest.builder()
                .queueName(QUEUE_NAME)
                .type(TYPE)
                .payload(PAYLOAD)
                .build();
    }
}

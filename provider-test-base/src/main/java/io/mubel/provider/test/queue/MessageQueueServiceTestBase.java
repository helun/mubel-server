/*
 * provider-test-base - Multi Backend Event Log
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
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
public abstract class MessageQueueServiceTestBase {

    public static final String QUEUE_NAME = "test-queue";
    public static final String TYPE = "test-type";
    public static final String PAYLOAD = "test-payload";

    public static final Duration VISIBILITY_TIMEOUT = Duration.ofSeconds(2);

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
        long startTime = System.currentTimeMillis();
        queue.send(getSendRequest());
        var request = new ReceiveRequest(QUEUE_NAME, Duration.ofMillis(500));
        var message = queue.receive(request).blockFirst();

        assertReceived(message);
        var shouldReappear = queue.receive(new ReceiveRequest(QUEUE_NAME, VISIBILITY_TIMEOUT.plusMillis(3000)))
                .blockFirst();
        var timeSinceSend = System.currentTimeMillis() - startTime;
        assertThat(shouldReappear).isNotNull();
        assertThat(shouldReappear.messageId()).isEqualTo(message.messageId());
        assertThat(timeSinceSend).isGreaterThan(VISIBILITY_TIMEOUT.toMillis());
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

        sleep(VISIBILITY_TIMEOUT.plusMillis(250));

        var message2 = queue.receive(new ReceiveRequest(QUEUE_NAME, VISIBILITY_TIMEOUT.plusMillis(100)))
                .blockFirst();
        assertThat(message2).isNull();
    }

    @Test
    void messages_should_be_published_according_to_specified_delay() {
        var queue = service();
        var sendRequest1 = sendRequestBuilderWithDelay(Duration.ofSeconds(2))
                .payload("2sec")
                .build();
        var sendRequest2 = sendRequestBuilderWithDelay(Duration.ofSeconds(1))
                .payload("1sec")
                .build();

        var sendTime1 = System.currentTimeMillis();
        queue.send(sendRequest1);
        var sendTime2 = System.currentTimeMillis();
        queue.send(sendRequest2);

        var request = new ReceiveRequest(QUEUE_NAME, Duration.ofSeconds(3), 2);
        var messages = queue.receive(request)
                .map(m -> new RecordedMessage(m, System.currentTimeMillis()))
                .toStream()
                .toList();

        assertThat(messages).hasSize(2);

        var message1 = messages.getFirst();
        assertThat(message1.payloadAsString()).isEqualTo("1sec");
        var delay1 = message1.receiveTime - sendTime2;
        assertThat(delay1)
                .as("message 2 with delay 1 sec should be received first")
                .isBetween(900L, 1500L);

        var message2 = messages.get(1);
        assertThat(message2.payloadAsString()).isEqualTo("2sec");
        var delay2 = message2.receiveTime - sendTime1;
        assertThat(delay2)
                .as("message 1 with delay 2 sec should be received last")
                .isBetween(1900L, 2500L);
    }

    @Test
    void no_more_than_maxMessages_delivered() {
        IntStream.range(0, 10)
                .forEach(i -> service().send(getSendRequest()));
        var received = service().receive(new ReceiveRequest(QUEUE_NAME, Duration.ofSeconds(2), 5))
                .collectList()
                .block();
        assertThat(received).hasSize(5);
    }


    private static SendRequest getSendRequestWithDelay(Duration delay) {
        return SendRequest.builder()
                .queueName(QUEUE_NAME)
                .type(TYPE)
                .payload(PAYLOAD)
                .delayMillis(delay.toMillis())
                .build();
    }

    private static SendRequest.Builder sendRequestBuilderWithDelay(Duration delay) {
        return SendRequest.builder()
                .queueName(QUEUE_NAME)
                .type(TYPE)
                .payload(PAYLOAD)
                .delayMillis(delay.toMillis());
    }

    private void sleep(Duration sleepTime) {
        try {
            Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void assertDuring(Duration duration, Runnable assertion) {
        var start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start < duration.toMillis()) {
            assertion.run();
            sleep(Duration.ofMillis(100));
        }
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

    record RecordedMessage(Message message, long receiveTime) {

        String payloadAsString() {
            return new String(message.payload());
        }

    }
}

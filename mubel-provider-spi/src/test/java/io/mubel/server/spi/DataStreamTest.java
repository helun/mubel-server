package io.mubel.server.spi;

import org.junit.jupiter.api.Test;

import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class DataStreamTest {

    @Test
    void endWhenConsumerIsBlocked() {
        DataStream<String> dataStream = new DataStream<>(10, "$$end");
        Subscriber subscriber = new Subscriber(dataStream);
        Executors.newSingleThreadExecutor().submit(subscriber::run);
        dataStream.end();
        await().untilAsserted(() -> assertThat(subscriber.ended).isTrue());
    }

    static class Subscriber {
        int count = 0;
        boolean ended = false;
        DataStream<String> dataStream;

        Subscriber(DataStream<String> dataStream) {
            this.dataStream = dataStream;
        }

        void run() {
            while (true) {
                try {
                    String v = dataStream.next();
                    if (v == null) {
                        break;
                    }
                    count++;
                } catch (InterruptedException e) {
                    break;
                }
            }
            ended = true;
        }

    }

}
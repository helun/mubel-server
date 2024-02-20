package io.mubel.provider.inmemory.queue;

import io.mubel.provider.test.queue.MessageQueueServiceTestBase;
import io.mubel.server.spi.queue.MessageQueueService;
import io.mubel.server.spi.queue.QueueConfiguration;
import io.mubel.server.spi.queue.QueueConfigurations;
import io.mubel.server.spi.support.IdGenerator;
import org.junit.jupiter.api.AfterEach;

import java.time.Duration;
import java.util.List;

class InMemMessageQueueServiceTest extends MessageQueueServiceTestBase {

    Duration visibilityTimeout = Duration.ofSeconds(1);

    InMemMessageQueueService service = new InMemMessageQueueService(new IdGenerator() {
    }, new QueueConfigurations(List.of(
            new QueueConfiguration(QUEUE_NAME, visibilityTimeout)
    )));

    @AfterEach
    void tearDown() {
        service.reset();
    }

    @Override
    protected Duration getVisibilityTimeout() {
        return visibilityTimeout;
    }

    @Override
    protected MessageQueueService service() {
        return service;
    }
}
package io.mubel.provider.jdbc.queue.pg;

import io.mubel.provider.jdbc.Containers;
import io.mubel.provider.jdbc.queue.JdbcMessageQueueService;
import io.mubel.provider.jdbc.queue.MessageQueueStatements;
import io.mubel.provider.jdbc.queue.SimpleWaitStrategy;
import io.mubel.provider.test.queue.MessageQueueServiceTestBase;
import io.mubel.server.spi.queue.MessageQueueService;
import io.mubel.server.spi.support.IdGenerator;
import org.jdbi.v3.core.Jdbi;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;

@Testcontainers
public class PgMessageQueueServiceTest extends MessageQueueServiceTestBase {

    @Container
    static PostgreSQLContainer<?> container = Containers.postgreSQLContainer();

    static JdbcMessageQueueService service;
    static Jdbi jdbi;
    final static Duration visibilityTimeout = Duration.ofSeconds(1);


    @AfterEach
    void tearDown() {
        jdbi.useHandle(h -> h.execute("TRUNCATE TABLE message_queue"));
    }

    @BeforeAll
    static void setup() {
        var dataSource = Containers.dataSource(container);

        var statements = new MessageQueueStatements();
        jdbi = Jdbi.create(dataSource);
        jdbi.useHandle(h -> statements.ddl().forEach(h::execute));

        service = JdbcMessageQueueService.builder()
                .visibilityTimeout(visibilityTimeout)
                .idGenerator(new IdGenerator() {
                })
                .jdbi(jdbi)
                .waitStrategy(new SimpleWaitStrategy(Duration.ofMillis(250)))
                .statements(statements)
                .build();
        service.start();
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

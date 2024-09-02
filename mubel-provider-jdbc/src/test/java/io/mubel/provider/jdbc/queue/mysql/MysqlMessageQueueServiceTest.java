package io.mubel.provider.jdbc.queue.mysql;

import io.mubel.provider.jdbc.Containers;
import io.mubel.provider.jdbc.queue.BatchDeleteStrategy;
import io.mubel.provider.jdbc.queue.JdbcMessageQueueService;
import io.mubel.provider.jdbc.queue.SimpleWaitStrategy;
import io.mubel.provider.jdbc.support.mysql.MysqlJdbiFactory;
import io.mubel.provider.test.queue.MessageQueueServiceTestBase;
import io.mubel.server.spi.queue.MessageQueueService;
import io.mubel.server.spi.support.IdGenerator;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.statement.Slf4JSqlLogger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;

@Testcontainers
public class MysqlMessageQueueServiceTest extends MessageQueueServiceTestBase {

    @Container
    static JdbcDatabaseContainer<?> container = Containers.mySqlContainer();

    static JdbcMessageQueueService service;
    static Jdbi jdbi;

    static Scheduler scheduler = Schedulers.boundedElastic();

    @AfterEach
    void tearDown() {
        jdbi.useTransaction(h -> h.createUpdate("DELETE FROM message_queue").execute());
    }

    @AfterAll
    static void stop() {
        scheduler.dispose();
        service.stop();
    }

    @BeforeAll
    static void setup() {
        var dataSource = Containers.dataSource(container);

        var statements = new MysqlMessageQueueStatements();
        jdbi = MysqlJdbiFactory.create(dataSource);
        jdbi.useHandle(h -> statements.ddl().forEach(h::execute));
        jdbi.setSqlLogger(new Slf4JSqlLogger());

        service = JdbcMessageQueueService.builder()
                .visibilityTimeout(VISIBILITY_TIMEOUT)
                .idGenerator(new IdGenerator() {
                })
                .jdbi(jdbi)
                .waitStrategy(new SimpleWaitStrategy(Duration.ofMillis(250)))
                .pollStrategy(new MysqlPollStrategy(statements))
                .deleteStrategy(new BatchDeleteStrategy(statements))
                .statements(statements)
                .scheduler(scheduler)
                .build();
        service.start();
    }

    @Override
    protected MessageQueueService service() {
        return service;
    }
}

package io.mubel.provider.jdbc.topic;

import io.mubel.provider.jdbc.Containers;
import io.mubel.provider.jdbc.support.mysql.MysqlJdbiFactory;
import io.mubel.provider.jdbc.systemdb.SystemDbMigrator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

@Testcontainers
class PollingTopicTest extends TopicTestBase {

    @Container
    static MySQLContainer<?> container = Containers.mySqlContainer();

    PollingTopic topic;

    Scheduler scheduler;

    @BeforeAll
    static void setup() {
        var migrator = SystemDbMigrator.migrator(container.getJdbcUrl(), container.getUsername(), container.getPassword());
        migrator.migrate();
    }

    @BeforeEach
    void start() {
        var jdbi = MysqlJdbiFactory.create(Containers.dataSource(container));
        scheduler = Schedulers.boundedElastic();
        topic = new PollingTopic(TOPIC_NAME, 200, jdbi, scheduler);
    }

    @AfterEach
    void tearDown() {
        scheduler.dispose();
    }

    @Override
    protected Topic topic() {
        return topic;
    }
}
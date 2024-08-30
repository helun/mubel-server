package io.mubel.provider.jdbc.groups;

import io.mubel.provider.jdbc.Containers;
import io.mubel.provider.jdbc.systemdb.SystemDbMigrator;
import io.mubel.provider.jdbc.topic.Topic;
import io.mubel.provider.jdbc.topic.pg.PgTopic;
import io.mubel.provider.test.groups.GroupManagerTestBase;
import io.mubel.server.spi.groups.GroupManager;
import org.jdbi.v3.core.Jdbi;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import javax.sql.DataSource;

@Testcontainers
class PgGroupManagerTest extends GroupManagerTestBase {

    @Container
    static PostgreSQLContainer<?> container = Containers.postgreSQLContainer();

    Scheduler scheduler;

    JdbcGroupManager groupManager;

    Topic topic;

    @BeforeAll
    static void setup() {
        var migrator = SystemDbMigrator.migrator(container.getJdbcUrl(), container.getUsername(), container.getPassword());
        migrator.migrate();
    }

    @BeforeEach
    void start() {
        DataSource dataSource = Containers.dataSource(container);
        var jdbi = Jdbi.create(dataSource);
        scheduler = Schedulers.boundedElastic();
        topic = new PgTopic("groups", dataSource, scheduler);
        groupManager = new JdbcGroupManager(
                jdbi,
                topic,
                heartbeatInterval(),
                clock(),
                scheduler
        );
        groupManager.start();
    }

    @AfterEach
    void tearDown() {
        scheduler.dispose();
    }

    @Override
    protected GroupManager groupManager() {
        return groupManager;
    }
}
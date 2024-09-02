package io.mubel.provider.jdbc.groups;

import io.mubel.provider.jdbc.Containers;
import io.mubel.provider.jdbc.support.mysql.MysqlJdbiFactory;
import io.mubel.provider.jdbc.systemdb.SystemDbMigrator;
import io.mubel.provider.jdbc.topic.TestTopic;
import io.mubel.provider.jdbc.topic.Topic;
import io.mubel.provider.test.groups.GroupManagerTestBase;
import io.mubel.server.spi.groups.GroupManager;
import org.jdbi.v3.core.Jdbi;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

@Testcontainers
class MysqlGroupManagerTest extends GroupManagerTestBase {

    @Container
    static MySQLContainer<?> container = Containers.mySqlContainer();

    Scheduler scheduler;

    JdbcGroupManager groupManager;

    Topic topic;

    Jdbi jdbi;

    @BeforeAll
    static void setup() {
        var migrator = SystemDbMigrator.migrator(container.getJdbcUrl(), container.getUsername(), container.getPassword());
        migrator.migrate();
    }

    @BeforeEach
    void start() {
        jdbi = MysqlJdbiFactory.create(Containers.dataSource(container));
        scheduler = Schedulers.boundedElastic();
        topic = new TestTopic();
        groupManager = JdbcGroupManager.builder()
                .jdbi(jdbi)
                .topic(topic)
                .heartbeatInterval(heartbeatInterval())
                .clock(clock())
                .scheduler(scheduler)
                .operations(new MySqlGroupManagerOperations())
                .build();
        groupManager.start();
    }

    @AfterEach
    void tearDown() {
        scheduler.dispose();
        jdbi.useHandle(handle -> {
            handle.execute("TRUNCATE group_session");
            handle.execute("TRUNCATE group_leader");
        });
    }

    @Override
    protected GroupManager groupManager() {
        return groupManager;
    }
}

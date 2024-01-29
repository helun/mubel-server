package io.mubel.provider.jdbc.systemdb.mysql;

import io.mubel.provider.jdbc.Containers;
import io.mubel.provider.jdbc.systemdb.EventStoreDetailsRowMapper;
import io.mubel.provider.jdbc.systemdb.JdbcJobStatusRepository;
import io.mubel.provider.jdbc.systemdb.JobStatusRowMapper;
import io.mubel.provider.jdbc.systemdb.SystemDbMigrator;
import io.mubel.provider.test.systemdb.JobStatusRepositoryTestBase;
import io.mubel.server.spi.systemdb.JobStatusRepository;
import org.jdbi.v3.core.Jdbi;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
public class MysqlJobStatusRepositoryTest extends JobStatusRepositoryTestBase {

    @Container
    static MySQLContainer container = Containers.mySqlContainer();

    static JobStatusRepository repository;

    @BeforeAll
    static void setup() {
        var dataSource = Containers.dataSource(container);
        var migrator = SystemDbMigrator.migrator(container.getJdbcUrl(), container.getUsername(), container.getPassword());
        migrator.migrate();
        var jdbi = Jdbi.create(dataSource)
                .registerRowMapper(new JobStatusRowMapper());
        jdbi.registerRowMapper(new EventStoreDetailsRowMapper());
        repository = new JdbcJobStatusRepository(jdbi, new MysqlJobStatusStatements());
    }

    @Override
    protected JobStatusRepository repository() {
        return repository;
    }
}

package io.mubel.provider.jdbc.eventstore.mysql;

import io.mubel.provider.jdbc.Containers;
import io.mubel.provider.jdbc.eventstore.JdbcEventStoreProvisioner;
import io.mubel.provider.jdbc.queue.mysql.MysqlMessageQueueStatements;
import io.mubel.provider.jdbc.support.SqlStatements;
import org.jdbi.v3.core.Jdbi;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
public class MysqlProvisionTest {

    @Container
    static JdbcDatabaseContainer<?> container = Containers.mySqlContainer();

    @Test
    void drop_removes_all_db_objects() {
        var dataSource = Containers.dataSource(container);
        String eventStoreName = "test_es";
        var statements = new MysqlEventStoreStatements(eventStoreName);
        var queueStatements = new MysqlMessageQueueStatements();
        JdbcEventStoreProvisioner.provision(
                dataSource,
                SqlStatements.of(
                        statements.ddl(),
                        queueStatements.ddl()
                )
        );
        var jdbi = Jdbi.create(dataSource);
        var tableNames = getTableNames(jdbi);
        assertThat(tableNames)
                .hasSize(4);
        
        JdbcEventStoreProvisioner.drop(
                dataSource,
                SqlStatements.of(
                        statements.dropSql(),
                        queueStatements.dropSql()
                )
        );
        assertThat(getTableNames(jdbi)).isEmpty();
    }

    private static List<String> getTableNames(Jdbi jdbi) {
        return jdbi.withHandle(h ->
                h.queryMetadata(dbmd -> dbmd.getTables(container.getDatabaseName(), null, null, null))
                        .map(view -> view.getColumn("table_name", String.class))
                        .list()
        );
    }
}

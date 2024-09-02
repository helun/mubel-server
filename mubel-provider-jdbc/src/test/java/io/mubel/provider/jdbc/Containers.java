package io.mubel.provider.jdbc;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import javax.sql.DataSource;
import java.util.Map;

public final class Containers {

    private static final String USER = "admin";

    private Containers() {
    }

    public static MySQLContainer mySqlContainer() {
        return (MySQLContainer) new MySQLContainer("mysql:latest")
                .withUsername(USER)
                .withPassword(USER)
                .withTmpFs(Map.of("/var/lib/mysql", "rw"))
                .withCommand("--log-bin-trust-function-creators=1");
    }

    @SuppressWarnings("unchecked")
    public static PostgreSQLContainer postgreSQLContainer() {
        return (PostgreSQLContainer) new PostgreSQLContainer("postgres:latest")
                .withDatabaseName("events")
                .withPassword(USER)
                .withUsername(USER)
                .withTmpFs(Map.of("/var/lib/postgresql/data", "rw"))
                .withEnv("POSTGRES_DATA", "/var/lib/postgresql/data")
                .waitingFor(Wait.defaultWaitStrategy());
    }

    public static DataSource dataSource(JdbcDatabaseContainer container) {
        HikariConfig config = new HikariConfig();
        var jdbcUrl = container.getJdbcUrl() + "?serverTimezone=UTC";
        config.setJdbcUrl(jdbcUrl);
        config.setUsername(container.getUsername());
        config.setPassword(container.getPassword());
        config.setDriverClassName(container.getDriverClassName());
        config.setMaximumPoolSize(10);
        return new HikariDataSource(config);
    }
}

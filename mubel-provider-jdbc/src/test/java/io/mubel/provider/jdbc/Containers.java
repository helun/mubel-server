package io.mubel.provider.jdbc;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import javax.sql.DataSource;

public final class Containers {

    private static final String USER = "admin";

    private Containers() {
    }

    public static MySQLContainer mySqlContainer() {
        return new MySQLContainer("mysql:latest")
                .withUsername(USER)
                .withPassword(USER);
    }

    public static PostgreSQLContainer postgreSQLContainer() {
        return (PostgreSQLContainer) new PostgreSQLContainer("postgres:latest")
                .withDatabaseName("events")
                .withPassword(USER)
                .withUsername(USER)
                .waitingFor(Wait.defaultWaitStrategy());
    }

    public static DataSource dataSource(PostgreSQLContainer container) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(container.getJdbcUrl());
        config.setUsername(container.getUsername());
        config.setPassword(container.getPassword());
        config.setDriverClassName(container.getDriverClassName());
        config.setMaximumPoolSize(10);
        return new HikariDataSource(config);
    }

}

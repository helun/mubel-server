package io.mubel.provider.jdbc;

import org.postgresql.ds.PGSimpleDataSource;
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
        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setServerNames(new String[]{container.getHost()});
        ds.setPortNumbers(new int[]{container.getMappedPort(5432)});
        ds.setDatabaseName("events");
        ds.setUser(USER);
        ds.setPassword(USER);
        return ds;
    }

}

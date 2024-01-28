package io.mubel.provider.jdbc.eventstore.configuration;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.mubel.provider.jdbc.support.JdbcDataSources;
import io.mubel.provider.jdbc.support.MubelDataSource;
import io.mubel.provider.jdbc.systemdb.EventStoreDetailsRowMapper;
import io.mubel.provider.jdbc.systemdb.JdbcEventStoreDetailsRepository;
import io.mubel.provider.jdbc.systemdb.JdbcJobStatusRepository;
import io.mubel.provider.jdbc.systemdb.JobStatusRowMapper;
import io.mubel.provider.jdbc.systemdb.pg.PgEventStoreDetailsStatements;
import io.mubel.provider.jdbc.systemdb.pg.PgJobStatusStatements;
import io.mubel.server.spi.systemdb.EventStoreDetailsRepository;
import io.mubel.server.spi.systemdb.JobStatusRepository;
import org.jdbi.v3.core.Jdbi;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.util.concurrent.Executors;

import static io.mubel.server.spi.support.ValueUtil.whenPositive;

@AutoConfiguration
@EnableConfigurationProperties(JdbcProviderProperties.class)
public class JdbcProviderAutoconfiguration {

    @Bean
    public Scheduler jdbcScheduler() {
        return Schedulers.fromExecutorService(Executors.newVirtualThreadPerTaskExecutor(), "jdbc-executor");
    }

    @Bean("dataSources")
    public JdbcDataSources dataSources(JdbcProviderProperties properties) {
        var dataSources = new JdbcDataSources();
        properties.getDatasources().forEach(config -> {
            HikariConfig hikariConfig = new HikariConfig();
            hikariConfig.setPoolName(config.getName());
            hikariConfig.setJdbcUrl(config.getUrl());
            hikariConfig.setUsername(config.getUsername());
            hikariConfig.setPassword(config.getPassword());

            whenPositive(config.getMaximumPoolSize(), hikariConfig::setMaximumPoolSize);
            whenPositive(config.getConnectionTimeout(), hikariConfig::setConnectionTimeout);
            whenPositive(config.getIdleTimeout(), hikariConfig::setIdleTimeout);
            whenPositive(config.getMaxLifetime(), hikariConfig::setMaxLifetime);
            dataSources.add(config.getName(), MubelDataSource.of(new HikariDataSource(hikariConfig), config.getUrl()));
        });
        return dataSources;
    }

    @Bean
    public MubelDataSource systemDbDataSource(JdbcDataSources dataSources, JdbcProviderProperties properties) {
        return dataSources.get(properties.getSystemdb().getDataSource());
    }

    @Bean
    public EventStoreDetailsRepository jdbcEventStoreDetailsRepository(
            @Qualifier("systemDbDataSource") MubelDataSource systemDbDataSource
    ) {
        Jdbi jdbi = Jdbi.create(systemDbDataSource.dataSource())
                .registerRowMapper(new EventStoreDetailsRowMapper());
        return switch (systemDbDataSource.backendType()) {
            case PG -> new JdbcEventStoreDetailsRepository(jdbi, new PgEventStoreDetailsStatements());
            case MYSQL -> new JdbcEventStoreDetailsRepository(jdbi, new PgEventStoreDetailsStatements());
            default ->
                    throw new IllegalArgumentException("No event store details repository implementation for backend type: " + systemDbDataSource.backendType());
        };
    }

    @Bean
    public JobStatusRepository jdbcJobStatusRepository(
            @Qualifier("systemDbDataSource") MubelDataSource systemDbDataSource
    ) {
        Jdbi jdbi = Jdbi.create(systemDbDataSource.dataSource())
                .registerRowMapper(new JobStatusRowMapper());
        return switch (systemDbDataSource.backendType()) {
            case PG -> new JdbcJobStatusRepository(jdbi, new PgJobStatusStatements());
            case MYSQL -> new JdbcJobStatusRepository(jdbi, new PgJobStatusStatements());
            default ->
                    throw new IllegalArgumentException("No job status repository implementation for backend type: " + systemDbDataSource.backendType());
        };
    }
}

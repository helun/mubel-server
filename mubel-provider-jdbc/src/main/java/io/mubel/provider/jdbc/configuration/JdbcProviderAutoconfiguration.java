/*
 * mubel-provider-jdbc - mubel-provider-jdbc
 * Copyright © 2025 Henrik Barratt-Due (henrikbd@hey.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.mubel.provider.jdbc.configuration;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.mubel.provider.jdbc.JdbcProvider;
import io.mubel.provider.jdbc.eventstore.EventStoreFactory;
import io.mubel.provider.jdbc.groups.JdbcGroupManager;
import io.mubel.provider.jdbc.groups.MySqlGroupManagerOperations;
import io.mubel.provider.jdbc.groups.PgGroupManagerOperations;
import io.mubel.provider.jdbc.support.JdbcDataSources;
import io.mubel.provider.jdbc.support.MubelDataSource;
import io.mubel.provider.jdbc.systemdb.*;
import io.mubel.provider.jdbc.systemdb.mysql.MysqlJobStatusStatements;
import io.mubel.provider.jdbc.systemdb.pg.PgEventStoreDetailsStatements;
import io.mubel.provider.jdbc.systemdb.pg.PgJobStatusStatements;
import io.mubel.provider.jdbc.topic.PollingTopic;
import io.mubel.provider.jdbc.topic.Topic;
import io.mubel.provider.jdbc.topic.pg.PgTopic;
import io.mubel.server.spi.Provider;
import io.mubel.server.spi.groups.GroupsProperties;
import io.mubel.server.spi.groups.LeaderQueries;
import io.mubel.server.spi.model.BackendType;
import io.mubel.server.spi.queue.QueueConfigurations;
import io.mubel.server.spi.support.IdGenerator;
import io.mubel.server.spi.systemdb.EventStoreAliasRepository;
import io.mubel.server.spi.systemdb.EventStoreDetailsRepository;
import io.mubel.server.spi.systemdb.JobStatusRepository;
import org.jdbi.v3.core.Jdbi;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cache.CacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.lang.Nullable;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.time.Clock;
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
            if (MubelDataSource.backendType(config.getUrl()) == BackendType.PG) {
                hikariConfig.addDataSourceProperty("rewriteBatchedInserts", "true");
            }

            whenPositive(config.getMaximumPoolSize(), hikariConfig::setMaximumPoolSize);
            whenPositive(config.getConnectionTimeout(), hikariConfig::setConnectionTimeout);
            whenPositive(config.getIdleTimeout(), hikariConfig::setIdleTimeout);
            whenPositive(config.getMaxLifetime(), hikariConfig::setMaxLifetime);
            var ds = new HikariDataSource(hikariConfig);
            dataSources.add(config.getName(), MubelDataSource.of(ds, config.getUrl()));
        });
        return dataSources;
    }

    @Bean
    @Nullable
    @ConditionalOnProperty(prefix = "mubel.provider.jdbc.systemdb", name = "datasource")
    public MubelDataSource systemDbDataSource(JdbcDataSources dataSources, JdbcProviderProperties properties) {
        if (properties.getSystemdb() == null) {
            return null;
        }
        return dataSources.get(properties.getSystemdb().getDataSource());
    }

    @Bean
    @ConditionalOnBean(name = "systemDbDataSource")
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
    @ConditionalOnBean(name = "systemDbDataSource")
    public JobStatusRepository jdbcJobStatusRepository(
            @Qualifier("systemDbDataSource") MubelDataSource systemDbDataSource
    ) {
        Jdbi jdbi = Jdbi.create(systemDbDataSource.dataSource())
                .registerRowMapper(new JobStatusRowMapper());
        return switch (systemDbDataSource.backendType()) {
            case PG -> new JdbcJobStatusRepository(jdbi, new PgJobStatusStatements());
            case MYSQL -> new JdbcJobStatusRepository(jdbi, new MysqlJobStatusStatements());
            default ->
                    throw new IllegalArgumentException("No job status repository implementation for backend type: " + systemDbDataSource.backendType());
        };
    }

    @Bean
    @ConditionalOnBean(name = "systemDbDataSource")
    public Topic groupsTopic(@Qualifier("systemDbDataSource") MubelDataSource systemDbDataSource) {
        return switch (systemDbDataSource.backendType()) {
            case PG -> new PgTopic("groups", systemDbDataSource.dataSource(), Schedulers.boundedElastic());
            case MYSQL -> new PollingTopic("groups",
                    250,
                    Jdbi.create(systemDbDataSource.dataSource()),
                    Schedulers.boundedElastic()
            );
            default ->
                    throw new IllegalArgumentException("No group manager implementation for backend type: " + systemDbDataSource.backendType());
        };
    }

    @Bean
    @ConditionalOnBean(name = "systemDbDataSource")
    public JdbcGroupManager jdbcGroupManager(
            @Qualifier("systemDbDataSource") MubelDataSource systemDbDataSource,
            @Qualifier("groupsTopic") Topic groupsTopic,
            GroupsProperties properties
    ) {
        Jdbi jdbi = Jdbi.create(systemDbDataSource.dataSource());
        var operations = switch (systemDbDataSource.backendType()) {
            case PG -> new PgGroupManagerOperations();
            case MYSQL -> new MySqlGroupManagerOperations();
            default ->
                    throw new IllegalArgumentException("No group manager implementation for backend type: " + systemDbDataSource.backendType());
        };
        return JdbcGroupManager.builder()
                .jdbi(jdbi)
                .operations(operations)
                .scheduler(Schedulers.boundedElastic())
                .heartbeatInterval(properties.heartbeatInterval())
                .topic(groupsTopic)
                .clock(Clock.systemUTC())
                .build();
    }

    @Bean
    @ConditionalOnBean(name = "systemDbDataSource")
    public EventStoreAliasRepository jdbcEventStoreAliasRepository(
            @Qualifier("systemDbDataSource") MubelDataSource systemDbDataSource,
            CacheManager cacheManager
    ) {
        return new JdbcEventStoreAliasRepository(Jdbi.create(systemDbDataSource.dataSource()), cacheManager);
    }

    @Bean
    @ConditionalOnBean(name = "systemDbDataSource")
    public SystemDbMigrator systemDbMigrator(JdbcProviderProperties properties) {
        return new SystemDbMigrator(properties);
    }

    @Bean
    public EventStoreFactory jdbcEventStoreFactory(
            JdbcDataSources dataSources,
            JdbcProviderProperties properties,
            Scheduler scheduler,
            IdGenerator idGenerator,
            QueueConfigurations queueConfigurations
    ) {
        return new EventStoreFactory(dataSources, properties, scheduler, idGenerator, queueConfigurations);
    }

    @Bean
    @ConditionalOnBean(EventStoreFactory.class)
    public Provider jdbcProvider(
            EventStoreFactory eventStoreFactory,
            JdbcDataSources dataSources,
            JdbcProviderProperties properties,
            LeaderQueries leaderQueries) {
        return new JdbcProvider(
                eventStoreFactory,
                dataSources,
                properties,
                leaderQueries
        );
    }

}

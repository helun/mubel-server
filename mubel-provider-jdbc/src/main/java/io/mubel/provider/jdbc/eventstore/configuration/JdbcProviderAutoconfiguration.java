package io.mubel.provider.jdbc.eventstore.configuration;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;

import static io.mubel.server.spi.support.ValueUtil.whenPositive;

@AutoConfiguration
@EnableConfigurationProperties(JdbcProviderProperties.class)
public class JdbcProviderAutoconfiguration {

    @Bean
    public Scheduler jdbcScheduler() {
        return Schedulers.fromExecutorService(Executors.newVirtualThreadPerTaskExecutor(), "jdbc-executor");
    }

    @Bean
    public Map<String, DataSource> dataSources(JdbcProviderProperties properties) {
        Map<String, DataSource> dataSourceMap = new HashMap<>();
        properties.getDatasources().forEach(config -> {
            HikariConfig hikariConfig = new HikariConfig();
            hikariConfig.setJdbcUrl(config.getUrl());
            hikariConfig.setUsername(config.getUsername());
            hikariConfig.setPassword(config.getPassword());

            whenPositive(config.getMaximumPoolSize(), hikariConfig::setMaximumPoolSize);
            whenPositive(config.getConnectionTimeout(), hikariConfig::setConnectionTimeout);
            whenPositive(config.getIdleTimeout(), hikariConfig::setIdleTimeout);
            whenPositive(config.getMaxLifetime(), hikariConfig::setMaxLifetime);
            dataSourceMap.put(config.getName(), new HikariDataSource(hikariConfig));
        });
        return dataSourceMap;
    }
}

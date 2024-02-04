package io.mubel.provider.inmemory.configuration;

import io.mubel.provider.inmemory.InMemProvider;
import io.mubel.provider.inmemory.eventstore.InMemEventStores;
import io.mubel.provider.inmemory.systemdb.InMemEventStoreAliasRepository;
import io.mubel.provider.inmemory.systemdb.InMemEventStoreDetailsRepository;
import io.mubel.provider.inmemory.systemdb.InMemJobStatusRepository;
import io.mubel.server.spi.Provider;
import io.mubel.server.spi.systemdb.EventStoreAliasRepository;
import io.mubel.server.spi.systemdb.EventStoreDetailsRepository;
import io.mubel.server.spi.systemdb.JobStatusRepository;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import java.util.Set;

@AutoConfiguration
@EnableConfigurationProperties(InMemProviderProperties.class)
@ConditionalOnProperty(prefix = "mubel.provider.inmemory", name = "enabled", havingValue = "true")
public class InMemAutoConfiguration {

    @Bean
    public InMemEventStores inmemEventStores(InMemProviderProperties properties) {
        return new InMemEventStores(Set.copyOf(properties.getBackends()));
    }

    @Bean
    public Provider inmemProvider(InMemEventStores eventStores) {
        return new InMemProvider(eventStores);
    }

    @Bean
    @ConditionalOnBean(name = "inmemProvider")
    @ConditionalOnProperty(prefix = "mubel.provider.inmemory", name = "systemdb", havingValue = "true")
    @ConditionalOnMissingBean(EventStoreDetailsRepository.class)
    public EventStoreDetailsRepository inmemEventStoreDetailsRepository() {
        return new InMemEventStoreDetailsRepository();
    }

    @Bean
    @ConditionalOnBean(name = "inmemProvider")
    @ConditionalOnProperty(prefix = "mubel.provider.inmemory", name = "systemdb", havingValue = "true")
    @ConditionalOnMissingBean(JobStatusRepository.class)
    public JobStatusRepository inmemJobStatusRepository() {
        return new InMemJobStatusRepository();
    }

    @Bean
    @ConditionalOnBean(name = "inmemProvider")
    @ConditionalOnProperty(prefix = "mubel.provider.inmemory", name = "systemdb", havingValue = "true")
    @ConditionalOnMissingBean(EventStoreAliasRepository.class)
    public EventStoreAliasRepository inmemEventStoreAliasRepository() {
        return new InMemEventStoreAliasRepository();
    }
}

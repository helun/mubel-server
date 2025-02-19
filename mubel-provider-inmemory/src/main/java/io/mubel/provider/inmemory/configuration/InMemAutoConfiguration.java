/*
 * mubel-provider-inmemory - Multi Backend Event Log
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
package io.mubel.provider.inmemory.configuration;

import io.mubel.provider.inmemory.InMemProvider;
import io.mubel.provider.inmemory.eventstore.InMemEventStores;
import io.mubel.provider.inmemory.groups.InMemGroupManager;
import io.mubel.provider.inmemory.queue.InMemMessageQueueService;
import io.mubel.provider.inmemory.systemdb.InMemEventStoreAliasRepository;
import io.mubel.provider.inmemory.systemdb.InMemEventStoreDetailsRepository;
import io.mubel.provider.inmemory.systemdb.InMemJobStatusRepository;
import io.mubel.server.spi.Provider;
import io.mubel.server.spi.groups.GroupManager;
import io.mubel.server.spi.groups.GroupsProperties;
import io.mubel.server.spi.groups.LeaderQueries;
import io.mubel.server.spi.queue.QueueConfigurations;
import io.mubel.server.spi.support.IdGenerator;
import io.mubel.server.spi.systemdb.EventStoreAliasRepository;
import io.mubel.server.spi.systemdb.EventStoreDetailsRepository;
import io.mubel.server.spi.systemdb.JobStatusRepository;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import java.time.Clock;
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
    public Provider inmemProvider(
            InMemEventStores eventStores,
            InMemMessageQueueService inMemMessageQueueService,
            LeaderQueries leaderQueries
    ) {
        return new InMemProvider(eventStores, inMemMessageQueueService, leaderQueries);
    }

    @Bean
    public InMemMessageQueueService inMemMessageQueueService(
            IdGenerator idGenerator,
            QueueConfigurations queueConfigurations
    ) {
        return new InMemMessageQueueService(idGenerator, queueConfigurations);
    }

    @Bean
    @ConditionalOnBean(name = "inmemProvider")
    @ConditionalOnProperty(prefix = "mubel.provider.inmemory", name = "system-db", havingValue = "true")
    @ConditionalOnMissingBean(EventStoreDetailsRepository.class)
    public EventStoreDetailsRepository inmemEventStoreDetailsRepository() {
        return new InMemEventStoreDetailsRepository();
    }

    @Bean
    @ConditionalOnBean(name = "inmemProvider")
    @ConditionalOnProperty(prefix = "mubel.provider.inmemory", name = "system-db", havingValue = "true")
    @ConditionalOnMissingBean(JobStatusRepository.class)
    public JobStatusRepository inmemJobStatusRepository() {
        return new InMemJobStatusRepository();
    }

    @Bean
    @ConditionalOnBean(name = "inmemProvider")
    @ConditionalOnProperty(prefix = "mubel.provider.inmemory", name = "system-db", havingValue = "true")
    @ConditionalOnMissingBean(EventStoreAliasRepository.class)
    public EventStoreAliasRepository inmemEventStoreAliasRepository() {
        return new InMemEventStoreAliasRepository();
    }

    @Bean
    @ConditionalOnBean(name = "inmemProvider")
    @ConditionalOnProperty(prefix = "mubel.provider.inmemory", name = "system-db", havingValue = "true")
    @ConditionalOnMissingBean(GroupManager.class)
    public InMemGroupManager inmemGroupManager(GroupsProperties properties) {
        return new InMemGroupManager(Clock.systemUTC(), properties.heartbeatInterval());
    }

}

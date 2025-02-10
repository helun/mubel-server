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
package io.mubel.provider.jdbc;

import io.mubel.server.spi.groups.GroupsProperties;
import io.mubel.server.spi.queue.QueueConfiguration;
import io.mubel.server.spi.queue.QueueConfigurations;
import io.mubel.server.spi.support.IdGenerator;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;

import java.time.Duration;
import java.util.List;

@SpringBootApplication
@EnableCaching
public class JdbcProviderTestApplication {
    public static void main(String[] args) {
        SpringApplication.run(JdbcProviderTestApplication.class, args);
    }

    @Bean
    public IdGenerator idGenerator() {
        return new IdGenerator() {
        };
    }

    @Bean
    public QueueConfigurations queueConfigurations() {
        return new QueueConfigurations(List.of(
                new QueueConfiguration("deadlines", Duration.ofMinutes(1), Duration.ofMillis(500))
        ));
    }

    @Bean
    public GroupsProperties groupsProperties() {
        return new GroupsProperties(Duration.ofSeconds(2));
    }

}

/*
 * mubel-provider-spi - Multi Backend Event Log
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
package io.mubel.server.spi.queue;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;


class QueueConfigurationsTest {

    @Test
    void dynamic_configuration() {
        Duration defaultVisibilityTimeout = Duration.ofMinutes(1);
        Duration defaultPollIntervall = Duration.ofMillis(500);
        var configs = new QueueConfigurations(List.of(
                new QueueConfiguration("defaults",
                        defaultVisibilityTimeout,
                        defaultPollIntervall
                )
        ));
        var config = configs.getQueue("test", "defaults");
        assertThat(config.name()).isEqualTo("test");
        assertThat(config.visibilityTimeout()).isEqualTo(defaultVisibilityTimeout);
        assertThat(config.polIInterval()).isEqualTo(defaultPollIntervall);
    }

}
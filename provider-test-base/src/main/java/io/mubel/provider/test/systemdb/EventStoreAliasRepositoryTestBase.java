/*
 * provider-test-base - Multi Backend Event Log
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
package io.mubel.provider.test.systemdb;

import io.mubel.server.spi.systemdb.EventStoreAliasRepository;
import org.junit.jupiter.api.*;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
public abstract class EventStoreAliasRepositoryTestBase {

    protected abstract EventStoreAliasRepository repository();

    final String esid = "esid";

    @Nested
    class Empty_repository {
        @Test
        void getAlias_returns_input_event_store_id_when_no_alias_exists() {
            var repo = repository();
            assertThat(repo.getAlias(esid))
                    .as("should return input value when alias does not exist")
                    .isEqualTo(esid);
        }

        @Test
        void getEventStoreId_returns_input_value_when_no_alias_exists() {
            var repo = repository();
            assertThat(repo.getEventStoreId(esid))
                    .as("should return input value when alias does not exist")
                    .isEqualTo(esid);
        }
    }

    @Nested
    class Configure_alias {

        EventStoreAliasRepository repo = repository();

        String alias = "my:alias";

        @BeforeEach
        void setup() {
            repo.setAlias(esid, alias);
        }

        @Test
        void getAlias_returns_alias_when_alias_exists() {
            assertThat(repo.getAlias(esid))
                    .as("should return alias when alias exists")
                    .isEqualTo(alias);
        }

        @Test
        void getEventStoreId_returns_esid_when_alias_exists() {
            assertThat(repo.getEventStoreId(alias))
                    .as("should return alias when alias exists")
                    .isEqualTo(esid);
        }

        @Test
        void removeAlias_should_remove_alias() {
            repo.removeAlias(alias);
            assertThat(repo.getAlias(esid))
                    .as("should return input value when alias does not exist")
                    .isEqualTo(esid);
            assertThat(repo.getEventStoreId(alias))
                    .as("should return input esid when alias does not exist")
                    .isEqualTo(alias);
        }

        @Test
        void Assigning_alias_to_another_esid_should_remove_alias_from_previous_esid() {
            String esid2 = "esid2";
            repo.setAlias(esid2, alias);
            assertThat(repo.getAlias(esid))
                    .as("should remove alias from previous esid")
                    .isEqualTo(esid);
            assertThat(repo.getEventStoreId(alias))
                    .as("Alias should be assigned to new esid")
                    .isEqualTo(esid2);
        }

    }
    
}

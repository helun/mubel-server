package io.mubel.provider.test.systemdb;

import io.mubel.server.spi.systemdb.EventStoreAliasRepository;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public abstract class EventStoreAliasRepositoryTestBase {

    protected abstract EventStoreAliasRepository repository();

    final String esid = "esid";

    @Test
    void noAliases() {
        var repo = repository();
        assertThat(repo.getAlias(esid))
                .as("should return input value when alias does not exist")
                .isEqualTo(esid);
        assertThat(repo.getEventStoreId(esid))
                .as("should return input value when alias does not exist")
                .isEqualTo(esid);
    }

    @Test
    void setAlias() {
        var repo = repository();
        String alias1 = "my:alias";
        repo.setAlias(esid, alias1);
        assertThat(repo.getAlias(esid))
                .as("Alias should be assigned to esid")
                .isEqualTo(alias1);
        assertThat(repo.getEventStoreId(alias1))
                .as("esid should be assigned to alias")
                .isEqualTo(esid);

        repo.removeAlias(alias1);
        assertThat(repo.getAlias(esid))
                .as("Alias should be removed from esid")
                .isEqualTo(esid);

        String alias2 = "another:alias";
        repo.setAlias(esid, alias2);
        assertThat(repo.getAlias(esid))
                .as("Alias2 should be assigned to esid")
                .isEqualTo(alias2);
    }

    @Test
    void switchAlias() {
        var repo = repository();
        String alias = "my:alias";
        repo.setAlias(esid, alias);
        assertThat(repo.getAlias(esid))
                .as("Alias should be assigned to esid")
                .isEqualTo(alias);
        String esid2 = "esid2";
        repo.setAlias(esid2, alias);
        assertThat(repo.getEventStoreId(alias))
                .as("Alias should be assigned to esid2")
                .isEqualTo(esid2);
        assertThat(repo.getEventStoreId(esid))
                .as("Alias should be removed from esid")
                .isEqualTo(esid);
    }

}

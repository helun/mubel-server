package io.mubel.provider.test.groups;

import io.mubel.server.spi.groups.GroupManager;
import io.mubel.server.spi.groups.JoinRequest;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
public abstract class GroupManagerTestBase {

    @Test
    public void first_joined_becomes_leader() {
        var request = new JoinRequest("esid-1", "group-1", "token-1");
        var status = groupManager().join(request).blockFirst();
        assertThat(status.getLeader()).as("should become leader").isTrue();
        assertThat(status.getGroupId()).as("should have correct group id").isEqualTo("group-1");
        assertThat(status.getToken()).as("should have a token").isNotBlank();
    }

    protected abstract GroupManager groupManager();
}

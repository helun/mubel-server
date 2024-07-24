package io.mubel.provider.inmemory.groups;

import io.mubel.provider.test.groups.GroupManagerTestBase;
import io.mubel.server.spi.groups.GroupManager;
import org.junit.jupiter.api.BeforeEach;

class InMemGroupManagerTest extends GroupManagerTestBase {

    private InMemGroupManager groupManager;

    @BeforeEach
    void start() {
        groupManager = new InMemGroupManager(clock(), heartbeatInterval());
    }

    @Override
    protected GroupManager groupManager() {
        return groupManager;
    }

}
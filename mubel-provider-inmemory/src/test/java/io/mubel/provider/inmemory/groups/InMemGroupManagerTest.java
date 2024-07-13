package io.mubel.provider.inmemory.groups;

import io.mubel.provider.test.groups.GroupManagerTestBase;
import io.mubel.server.spi.groups.GroupManager;
import org.junit.jupiter.api.BeforeAll;

class InMemGroupManagerTest extends GroupManagerTestBase {

    private static final InMemGroupManager GROUP_MANAGER = new InMemGroupManager();

    @BeforeAll
    static void start() {
        GROUP_MANAGER.start();
    }

    @Override
    protected GroupManager groupManager() {
        return GROUP_MANAGER;
    }

}
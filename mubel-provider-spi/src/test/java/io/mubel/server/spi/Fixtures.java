package io.mubel.server.spi;

import java.util.UUID;

public final class Fixtures {

    private Fixtures() {
    }

    public static String uuidString() {
        return UUID.randomUUID().toString();
    }

    public static UUID uuid() {
        return UUID.randomUUID();
    }

}

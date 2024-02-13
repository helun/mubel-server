package io.mubel.server.spi.support;

import java.util.UUID;

public interface IdGenerator {

    default String generateStringId() {
        return java.util.UUID.randomUUID().toString();
    }

    default UUID generate() {
        return java.util.UUID.randomUUID();
    }

}

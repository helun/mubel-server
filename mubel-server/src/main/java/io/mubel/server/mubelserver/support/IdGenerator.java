package io.mubel.server.mubelserver.support;

import org.springframework.stereotype.Component;

@Component
public class IdGenerator {

    public String generate() {
        return java.util.UUID.randomUUID().toString();
    }
}

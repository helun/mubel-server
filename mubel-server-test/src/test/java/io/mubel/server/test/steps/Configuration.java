package io.mubel.server.test.steps;

import io.mubel.client.MubelClient;
import io.mubel.client.MubelClientConfig;

public class Configuration {

    private MubelClient client;

    public MubelClient client() {
        if (client == null) {
            client = new MubelClient(MubelClientConfig.newBuilder()
                    .address("localhost:9090")
                    .build());
        }
        return client;
    }

    public MubelClient newClient() {
        return new MubelClient(MubelClientConfig.newBuilder()
                .address("localhost:9090")
                .build());
    }
}

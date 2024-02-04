package io.mubel.server.test;

import io.mubel.client.MubelClient;

public class ClientSession {

    private final MubelClient client;
    private String consumerGroupToken;

    public ClientSession(MubelClient client) {
        this.client = client;
    }

    public MubelClient client() {
        return client;
    }

    public String getConsumerGroupToken() {
        return consumerGroupToken;
    }

    public void setConsumerGroupToken(String consumerGroupToken) {
        this.consumerGroupToken = consumerGroupToken;
    }
}

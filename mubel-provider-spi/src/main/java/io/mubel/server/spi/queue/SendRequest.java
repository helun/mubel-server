package io.mubel.server.spi.queue;

public record SendRequest(
        String queueName,
        String type,
        byte[] payload,
        long delayMillis
) {

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String queueName;
        private String type;
        private byte[] payload;
        private long delayMillis;

        public Builder queueName(String queueName) {
            this.queueName = queueName;
            return this;
        }

        public Builder type(String type) {
            this.type = type;
            return this;
        }

        public Builder payload(byte[] payload) {
            this.payload = payload;
            return this;
        }

        public Builder payload(String payload) {
            this.payload = payload.getBytes();
            return this;
        }

        public Builder delayMillis(long delayMillis) {
            this.delayMillis = delayMillis;
            return this;
        }

        public SendRequest build() {
            return new SendRequest(queueName, type, payload, delayMillis);
        }
    }

}

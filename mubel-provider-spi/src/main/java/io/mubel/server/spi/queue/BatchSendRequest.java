package io.mubel.server.spi.queue;

import java.util.ArrayList;
import java.util.List;

public record BatchSendRequest(
        String queueName,
        Iterable<BatchEntry> entries
) {

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String queueName;
        private List<BatchEntry> entries;

        public Builder queueName(String queueName) {
            this.queueName = queueName;
            return this;
        }

        public Builder entries(List<BatchEntry> entries) {
            this.entries = new ArrayList<>(entries);
            return this;
        }

        public BatchSendRequest build() {
            return new BatchSendRequest(queueName, entries);
        }

        public boolean hasEntries() {
            return entries != null && !entries.isEmpty();
        }

        public Builder clearEntries() {
            entries = null;
            return this;
        }
    }

    public record BatchEntry(
            String type,
            byte[] payload,
            long delayMillis
    ) {

        public static Builder builder() {
            return new Builder();
        }

        public static class Builder {
            private String type;
            private byte[] payload;
            private long delayMillis;

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

            public void clear() {
                type = null;
                payload = null;
                delayMillis = 0;
            }

            public BatchEntry build() {
                return new BatchEntry(type, payload, delayMillis);
            }
        }
    }
}

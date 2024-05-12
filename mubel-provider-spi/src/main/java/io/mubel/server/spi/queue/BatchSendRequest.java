package io.mubel.server.spi.queue;

import java.util.List;

public record BatchSendRequest(
        String queueName,
        Iterable<BatchEntry> entries
) {

    public static BatchSendRequestBuilder builder() {
        return new BatchSendRequestBuilder();
    }

    public static class BatchSendRequestBuilder {
        private String queueName;
        private List<BatchEntry> entries;

        public BatchSendRequestBuilder queueName(String queueName) {
            this.queueName = queueName;
            return this;
        }

        public BatchSendRequestBuilder entries(List<BatchEntry> entries) {
            if (this.entries == null) {
                this.entries = new java.util.ArrayList<>(entries);
            } else {
                this.entries.addAll(entries);
            }
            return this;
        }

        public BatchSendRequestBuilder entry(BatchEntry entry) {
            if (entries == null) {
                entries = new java.util.ArrayList<>();
            }
            this.entries.add(entry);
            return this;
        }

        public BatchSendRequest build() {
            return new BatchSendRequest(queueName, entries);
        }

        public boolean hasEntries() {
            return entries != null && !entries.isEmpty();
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

package io.mubel.server.spi.queue;

public record BatchSendRequest(
        String queueName,
        Iterable<BatchEntry> entries
) {

    public static BatchSendRequestBuilder builder() {
        return new BatchSendRequestBuilder();
    }

    public static class BatchSendRequestBuilder {
        private String queueName;
        private Iterable<BatchEntry> entries;

        public BatchSendRequestBuilder queueName(String queueName) {
            this.queueName = queueName;
            return this;
        }

        public BatchSendRequestBuilder entries(Iterable<BatchEntry> entries) {
            this.entries = entries;
            return this;
        }

        public BatchSendRequest build() {
            return new BatchSendRequest(queueName, entries);
        }
    }

    public record BatchEntry(
            String type,
            byte[] payload,
            long delayMillis
    ) {

        public static BatchEntryBuilder builder() {
            return new BatchEntryBuilder();
        }

        public static class BatchEntryBuilder {
            private String type;
            private byte[] payload;
            private long delayMillis;

            public BatchEntryBuilder type(String type) {
                this.type = type;
                return this;
            }

            public BatchEntryBuilder payload(byte[] payload) {
                this.payload = payload;
                return this;
            }

            public BatchEntryBuilder payload(String payload) {
                this.payload = payload.getBytes();
                return this;
            }

            public BatchEntryBuilder delayMillis(long delayMillis) {
                this.delayMillis = delayMillis;
                return this;
            }

            public BatchEntry build() {
                return new BatchEntry(type, payload, delayMillis);
            }
        }
    }
}

/*
 * mubel-provider-spi - Multi Backend Event Log
 * Copyright © 2025 Henrik Barratt-Due (henrikbd@hey.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
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

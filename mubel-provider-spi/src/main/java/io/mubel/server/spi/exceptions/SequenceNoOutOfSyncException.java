package io.mubel.server.spi.exceptions;

public class SequenceNoOutOfSyncException extends StorageBackendException {

    public SequenceNoOutOfSyncException(String esid, long currentSeqNo, long actualSeqNo) {
        super("%s: subscription sequenceNo out of sync: was: %d, expected: %d"
                .formatted(esid, actualSeqNo, currentSeqNo + 1)
        );
    }
}

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
package io.mubel.server.spi.execute;

import io.mubel.api.grpc.v1.events.ExecuteRequest;
import io.mubel.server.spi.Fixtures;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.mubel.server.spi.execute.TestOperations.*;
import static org.assertj.core.api.Assertions.assertThat;

class ExecuteBatchTest {

    public static final String ESID = "test";

    @Test
    void should_clear_all_bufferes_after_execute() {
        var batch = new ExecuteBatch(ESID, a -> a);
        var request = ExecuteRequest.newBuilder()
                .setRequestId(ESID)
                .addOperation(appendOperation())
                .addOperation(cancelScheduledOperation(Fixtures.uuid()))
                .addOperation(scheduleDeadlineOperation())
                .addOperation(scheduleEventOperation())
                .build();
        var r1 = InternalExecuteRequest.of(request);
        batch.addAll(List.of(r1));
        batch.consolidate();
        assertThat(batch.hasAppendOperation()).isTrue();
        assertThat(batch.appendOperation())
                .as("Should have append operation")
                .isEqualTo(request.getOperation(0).getAppend());
        assertThat(batch.sendRequests())
                .as("Should have 2 send requests")
                .hasSize(2);
        assertThat(batch.cancelIds())
                .as("Should have 1 cancel id")
                .hasSize(1);
        batch.complete();
        assertThat(batch.hasAppendOperation())
                .as("Append operation should be cleared after batch has been completed")
                .isFalse();
        assertThat(batch.appendOperation())
                .as("Append operation should be cleared after batch has been completed")
                .isNull();
        assertThat(batch.sendRequests())
                .as("Send requests should be cleared after batch has been completed")
                .isEmpty();
        assertThat(batch.cancelIds())
                .as("Cancel ids should be cleared after batch has been completed")
                .isEmpty();
    }

}
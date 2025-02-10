/*
 * provider-test-base - Multi Backend Event Log
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
package io.mubel.provider.test;

import reactor.core.publisher.Flux;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.awaitility.Awaitility.await;

public class TestSubscriber<E> {

    private final List<E> values = new CopyOnWriteArrayList<>();
    private Throwable error;

    private volatile boolean done = false;

    public TestSubscriber(Flux<E> dataStream) {
        dataStream.subscribe(
                values::add,
                err -> error = err,
                () -> done = true
        );
    }

    public TestSubscriber<E> awaitDone() {
        await().untilAsserted(() -> {
            if (done) {
                return;
            }
            throw new AssertionError("DataStream is not done");
        });
        return this;
    }

    public TestSubscriber<E> assertComplete() {
        if (!done) {
            throw new AssertionError("DataStream is not done");
        }
        return this;
    }

    public void assertNotComplete() {
        if (done) {
            throw new AssertionError("DataStream is done");
        }
    }

    public TestSubscriber<E> assertNoErrors() {
        if (error != null) {
            throw new AssertionError("DataStream has error", error);
        }
        return this;
    }

    public TestSubscriber<E> assertValueCount(int count) {
        if (values.size() != count) {
            throw new AssertionError("DataStream has " + values.size() + " values, expected " + count);
        }
        return this;
    }

    public TestSubscriber<E> awaitCount(int count) {
        await().failFast(() -> {
                    if (values.size() > count) {
                        throw new AssertionError("DataStream has " + values.size() + " values, expected " + count);
                    }
                })
                .untilAsserted(() -> {
                    if (values.size() == count) {
                        return;
                    }
                    throw new AssertionError("DataStream has " + values.size() + " values, expected " + count);
                });
        return this;
    }

    public List<E> values() {
        return values;
    }

    public TestSubscriber<E> assertNoValues() {
        if (!values.isEmpty()) {
            throw new AssertionError("DataStream has values");
        }
        return this;
    }

}

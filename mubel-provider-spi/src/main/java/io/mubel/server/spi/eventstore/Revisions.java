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
package io.mubel.server.spi.eventstore;

import java.util.HashMap;
import java.util.Map;

public class Revisions {
    private final static Revisions EMPTY = new Revisions(0);
    private final Map<String, Integer> revisions;

    public Revisions(int initialCapacity) {
        revisions = new HashMap<>(initialCapacity);
    }

    public static Revisions empty() {
        return EMPTY;
    }

    public Revisions add(String streamId, int revision) {
        revisions.put(streamId, revision);
        return this;
    }

    public int nextRevision(String streamId) {
        return revisions.compute(streamId, (k, v) -> v == null ? 0 : v + 1);
    }

}

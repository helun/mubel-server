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
package io.mubel.server.spi.support;

public class Constraints {
    
    public static boolean isNonNull(Object o) {
        return o != null;
    }

    public static String requireNotBlank(String s, String message) {
        if (!isNotBlank(s)) {
            throw new IllegalArgumentException(message);
        }
        return s;
    }

    public static String requireNotBlank(String s) {
        return requireNotBlank(s, "should not be blank");
    }

    public static boolean isNotBlank(String s) {
        return s != null && !s.trim().isEmpty();
    }

    public static boolean between(int value, int minInclusive, int maxExclusive) {
        return value >= minInclusive && value < maxExclusive;
    }
}


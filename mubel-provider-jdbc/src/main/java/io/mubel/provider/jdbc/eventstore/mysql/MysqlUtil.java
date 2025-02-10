/*
 * mubel-provider-jdbc - mubel-provider-jdbc
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
package io.mubel.provider.jdbc.eventstore.mysql;

import io.mubel.server.spi.support.HexCodec;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

public class MysqlUtil {

    public static String decodeHex(String s) {
        var chars = s.toCharArray();
        var hex = new StringBuilder();
        var nonHexSequence = new StringBuilder();
        var maxIndex = chars.length - 1;
        for (int i = 0; i < chars.length; i++) {
            if (i < maxIndex && (chars[i] == '\\' && chars[i + 1] == 'x')) {
                if (!nonHexSequence.isEmpty()) {
                    hex.append(HexCodec.bytesToHex(nonHexSequence.toString().getBytes(StandardCharsets.US_ASCII)));
                    nonHexSequence = new StringBuilder();
                }
                i += 2;
                hex.append(chars[i++]);
                hex.append(chars[i]);
            } else {
                nonHexSequence.append(chars[i]);
            }
        }
        if (!nonHexSequence.isEmpty()) {
            hex.append(HexCodec.bytesToHex(nonHexSequence.toString().getBytes(StandardCharsets.US_ASCII)));
        }
        return hex.toString();
    }

    public static UUID decodeUuid(String s) {
        var hex = decodeHex(s);
        return convertBytesToUUID(HexCodec.hexToBytes(hex));
    }

    private static UUID convertBytesToUUID(byte[] bytes) {
        final var byteBuffer = ByteBuffer.wrap(bytes);
        final long high = byteBuffer.getLong();
        final long low = byteBuffer.getLong();
        return new UUID(high, low);
    }
}

package io.mubel.server.spi.support;

import java.util.function.Consumer;

public class ValueUtil {

    public static void whenPositive(int value, Consumer<Integer> consumer) {
        if (value > 0) {
            consumer.accept(value);
        }
    }

    public static void whenPositive(long value, Consumer<Long> consumer) {
        if (value > 0) {
            consumer.accept(value);
        }
    }

}

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


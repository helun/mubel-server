package io.mubel.schema;

public class Constrains {

    public final static String SAFE_STRING_PTRN = "^[A-Za-z0-9_-]+$";
    public final static String ESID_PTRN = "^[a-z][a-z0-9_-]{1,99}$";
    public final static String PROPERTY_PATH_PTRN = "^([A-Za-z0-9_-])+(\\.?([A-Za-z0-9_-])*)*$";
    public final static String EVENT_TYPE_PTRN = "^([A-Za-z0-9_-])+([\\.:]?([A-Za-z0-9_-])*[\\$\\+]*([A-Za-z0-9_-])*)*$";

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

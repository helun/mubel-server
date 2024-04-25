package io.mubel.server.api.grpc.validation;

import am.ik.yavi.constraint.CharSequenceConstraint;

import java.util.function.Function;
import java.util.regex.Pattern;

public class CommonConstraints {

    public static final Pattern SAFE_STRING_PTRN = Pattern.compile("^[A-Za-z0-9_-]+$");
    public static final Pattern PATH_PTRN = Pattern.compile("^[a-z][a-z0-9_-]{1,99}$");
    public final static Pattern EVENT_TYPE_PTRN = Pattern.compile("^([A-Za-z0-9_-])+([\\.:/]?([A-Za-z0-9_-])*[\\$\\+]*([A-Za-z0-9_-])*)*$");

    public static <T> Function<CharSequenceConstraint<T, String>, CharSequenceConstraint<T, String>> eventType() {
        return type -> type.notBlank().pattern(CommonConstraints.EVENT_TYPE_PTRN);
    }

    public static <T> Function<CharSequenceConstraint<T, String>, CharSequenceConstraint<T, String>> eventId() {
        return c -> c.notBlank().uuid();
    }

    public static <T> Function<CharSequenceConstraint<T, String>, CharSequenceConstraint<T, String>> streamId() {
        return c -> c.notBlank().uuid();
    }

    public static <T> Function<CharSequenceConstraint<T, String>, CharSequenceConstraint<T, String>> esid() {
        return c -> c.notBlank().pattern(CommonConstraints.EVENT_TYPE_PTRN).lessThan(100);
    }

    public static <T> Function<CharSequenceConstraint<T, String>, CharSequenceConstraint<T, String>> safeString() {
        return c -> c.notBlank().pattern(CommonConstraints.SAFE_STRING_PTRN).lessThan(255);
    }

}

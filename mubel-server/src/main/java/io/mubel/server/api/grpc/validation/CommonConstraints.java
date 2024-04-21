package io.mubel.server.api.grpc.validation;

import java.util.regex.Pattern;

public class CommonConstraints {

    public static final Pattern SAFE_STRING_PTRN = Pattern.compile("^[A-Za-z0-9_-]+$");
    public static final Pattern UUID_REGEX =
            Pattern.compile("^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$");
    public final static Pattern EVENT_TYPE_PTRN = Pattern.compile("^([A-Za-z0-9_-])+([\\.:/]?([A-Za-z0-9_-])*[\\$\\+]*([A-Za-z0-9_-])*)*$");

}

package io.mubel.schema;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

class ConstrainsTest {

    static Pattern ESID_PATTERN = Pattern.compile(Constrains.ESID_PTRN);
    static Pattern PROPERTY_PATH_PTRN = Pattern.compile(Constrains.PROPERTY_PATH_PTRN);
    static Pattern EVENT_TYPE_PTRN = Pattern.compile(Constrains.EVENT_TYPE_PTRN);

    @ParameterizedTest
    @ValueSource(strings = {
            "abc_cde",
            "mem-db_ft_1698075106",
            "myns-123"
    })
    void validEsids(String input) {
        assertThat(ESID_PATTERN.matcher(input).matches())
                .as("%s should be a valid esid", input)
                .isTrue();
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "!abc:cde",
            "{'evil': 'json'}",
            "-myns-123:q123",
            "123:qqq",
            "qqq:123",
            "qqq/qwe:abc",
    })
    void invalidEsids(String input) {
        assertThat(ESID_PATTERN.matcher(input).matches())
                .as("%s should not be a valid esid", input)
                .isFalse();
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "simple",
            "simple.with.dot",
            "simple.with.dot.and_underscore",
            "simple.with.dot.and_underscore.123"
    })
    void validPropertyPathPattern(String input) {
        assertThat(PROPERTY_PATH_PTRN.matcher(input).matches())
                .as("%s should be valid", input)
                .isTrue();
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "simple",
            "simple.with.dot",
            "simple.with.dot.and_underscore",
            "simple.with.dot.and_underscore.123",
            "value:with:colons",
            "fully.qualified.java.classname.With.Inner$Class",
            "fully.qualified.dotnet.classname.With.Inner+Class",
    })
    void validEventTypePattern(String input) {
        assertThat(EVENT_TYPE_PTRN.matcher(input).matches())
                .as("%s should be valid", input)
                .isTrue();
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "simple()",
            "frr&qwee"
    })
    void invalidEventTypePattern(String input) {
        assertThat(EVENT_TYPE_PTRN.matcher(input).matches())
                .as("%s should not be valid", input)
                .isFalse();
    }

}
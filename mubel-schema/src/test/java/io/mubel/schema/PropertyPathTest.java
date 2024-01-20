package io.mubel.schema;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class PropertyPathTest {

    PropertyPath path = new PropertyPath();

    @Test
    void currentPath() {
        assertPath("");
        path.push("foo");
        assertPath("foo");
        path.push("bar");
        assertPath("foo.bar");
        path.pop();
        assertPath("foo");
        path.push(0);
        assertPath("foo[0]");
        path.push(1);
        assertPath("foo[1]");
        path.push("foz");
        assertPath("foo[1].foz");
        path.pop();
        path.push(2);
        assertPath("foo[2]");
    }

    void assertPath(String expectedPath) {
        assertThat(path.toString()).isEqualTo(expectedPath);
    }
}
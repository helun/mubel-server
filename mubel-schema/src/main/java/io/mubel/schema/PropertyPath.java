package io.mubel.schema;

import java.util.ArrayDeque;
import java.util.Deque;

public class PropertyPath {

    private Deque<PathElement> path = new ArrayDeque<>();
    private ArrayElement currentArray;

    public void push(String field) {
        if (path.isEmpty()) {
            path.add(PropertyElement.root(field));
        } else {
            path.add(PropertyElement.child(field));
        }
    }

    public void push(int index) {
        if (currentArray == null) {
            currentArray = ArrayElement.create(index);
            path.add(currentArray);
        } else {
            currentArray.index(index);
        }

    }

    public void pop() {
        currentArray = null;
        if (!path.isEmpty()) {
            path.removeLast();
            if (!path.isEmpty() && path.getLast() instanceof ArrayElement ae) {
                currentArray = ae;
            }
        }
    }

    @Override
    public String toString() {
        var sb = new StringBuilder();
        for (var p : path) {
            sb.append(p);
        }
        return sb.toString();
    }

    private static abstract class PathElement {

    }

    private static class PropertyElement extends PathElement {

        private final String name;
        private final boolean isRoot;

        private PropertyElement(String name, boolean isRoot) {
            this.name = name;
            this.isRoot = isRoot;
        }

        static PropertyElement root(String name) {
            return new PropertyElement(name, true);
        }

        static PropertyElement child(String name) {
            return new PropertyElement(name, false);
        }

        @Override
        public String toString() {
            return isRoot ? name : "." + name;
        }
    }

    private static class ArrayElement extends PathElement {
        private int idx;

        public ArrayElement(int idx) {
            this.idx = idx;
        }

        static ArrayElement create(int idx) {
            return new ArrayElement(idx);
        }

        void index(int idx) {
            this.idx = idx;
        }

        @Override
        public String toString() {
            return "[" + idx + "]";
        }
    }


}

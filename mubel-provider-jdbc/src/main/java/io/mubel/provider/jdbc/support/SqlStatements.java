package io.mubel.provider.jdbc.support;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class SqlStatements implements Iterable<String> {

    @SafeVarargs
    public static SqlStatements of(List<String>... statements) {
        int totalSize = 0;
        for (var stmts : statements) {
            totalSize += stmts.size();
        }
        var allStatements = new ArrayList<String>(totalSize);
        for (var stmts : statements) {
            allStatements.addAll(stmts);
        }
        return new SqlStatements(allStatements);
    }

    private final List<String> statements;

    private SqlStatements(List<String> statements) {
        this.statements = statements;
    }

    @Override
    public Iterator<String> iterator() {
        return statements.iterator();
    }
}

package io.mubel.provider.jdbc.support;

public interface RepositoryStatements {

    String upsert();

    String selectAll();

    String select();

    String delete();

    String exists();
}

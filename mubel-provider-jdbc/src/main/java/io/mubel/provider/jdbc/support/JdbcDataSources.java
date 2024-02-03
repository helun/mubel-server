package io.mubel.provider.jdbc.support;

import java.util.HashMap;
import java.util.Map;

public class JdbcDataSources {

    private final Map<String, MubelDataSource> dataSources = new HashMap<>();

    public void add(String name, MubelDataSource dataSource) {
        dataSources.put(name, dataSource);
    }

    public MubelDataSource get(String name) {
        return dataSources.get(name);
    }

    public Map<String, MubelDataSource> getAll() {
        return dataSources;
    }
}

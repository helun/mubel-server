package io.mubel.provider.jdbc.support;

import io.mubel.server.spi.model.BackendType;

import javax.sql.DataSource;

public record MubelDataSource(
        DataSource dataSource,
        BackendType backendType
) {

    public static MubelDataSource of(DataSource dataSource, String jdbcUrl) {
        return new MubelDataSource(dataSource, backendType(jdbcUrl));
    }

    public static BackendType backendType(String jdbcUrl) {
        if (jdbcUrl.startsWith("jdbc:postgresql")) {
            return BackendType.PG;
        } else if (jdbcUrl.startsWith("jdbc:mysql")) {
            return BackendType.MYSQL;
        } else {
            throw new IllegalArgumentException("Unsupported data source type URL: " + jdbcUrl);
        }
    }
}

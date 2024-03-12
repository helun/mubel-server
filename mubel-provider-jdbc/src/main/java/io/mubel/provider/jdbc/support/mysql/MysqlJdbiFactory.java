package io.mubel.provider.jdbc.support.mysql;

import org.jdbi.v3.core.Jdbi;

import javax.sql.DataSource;

public final class MysqlJdbiFactory {

    private MysqlJdbiFactory() {
    }

    public static Jdbi create(DataSource dataSource) {
        return Jdbi.create(dataSource)
                .registerArgument(new MysqlUuidArgumentFactory());
    }

}

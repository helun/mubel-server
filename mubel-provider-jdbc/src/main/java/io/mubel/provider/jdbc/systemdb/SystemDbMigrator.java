/*
 * mubel-provider-jdbc - mubel-provider-jdbc
 * Copyright © 2025 Henrik Barratt-Due (henrikbd@hey.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.mubel.provider.jdbc.systemdb;

import io.mubel.provider.jdbc.configuration.JdbcProviderProperties;
import io.mubel.server.spi.exceptions.MubelConfigurationException;
import org.flywaydb.core.Flyway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;

import static java.util.Objects.requireNonNull;

public class SystemDbMigrator implements InitializingBean {

    private static final Logger LOG = LoggerFactory.getLogger(SystemDbMigrator.class);
    public static final String PG_PATH = "systemdb/pg";
    public static final String MYSQL_PATH = "systemdb/mysql";
    private final String url;
    private final String user;
    private final String pwd;
    private final String path;

    public SystemDbMigrator(JdbcProviderProperties properties) {
        var systemDB = properties.getSystemdb();
        var dataSourceName = systemDB.getDataSource();
        var dbProps = properties.getDatasources().stream()
                .filter(dataSourceProperties -> dataSourceProperties.getName().equals(dataSourceName))
                .findFirst()
                .orElseThrow(() -> new MubelConfigurationException("No datasource with name " + dataSourceName + " found"));
        url = dbProps.getUrl();
        user = dbProps.getUsername();
        pwd = dbProps.getPassword();
        path = resolvePath(url);
    }

    private SystemDbMigrator(String jdbcUrl, String user, String pwd, String path) {
        this.url = requireNonNull(jdbcUrl);
        this.user = requireNonNull(user);
        this.pwd = requireNonNull(pwd);
        this.path = requireNonNull(path);
    }

    public static SystemDbMigrator migrator(String jdbcUrl, String user, String pwd) {
        return new SystemDbMigrator(
                jdbcUrl,
                user,
                pwd,
                resolvePath(jdbcUrl)
        );
    }

    private static String resolvePath(String jdbcUrl) {
        if (jdbcUrl.contains("postgresql")) {
            return PG_PATH;
        } else if (jdbcUrl.contains("mysql")) {
            return MYSQL_PATH;
        } else {
            throw new IllegalArgumentException("Unsupported jdbc url: " + jdbcUrl);
        }
    }

    public void migrate() {
        LOG.info("Migrating systemdb at {} with user {}", url, user);
        Flyway flyway = Flyway.configure()
                .loggers("slf4j")
                .dataSource(url, user, pwd)
                .locations(path)
                .load();

        flyway.migrate();
        LOG.info("Migration complete");
    }

    @Override
    public void afterPropertiesSet() {
        migrate();
    }
}

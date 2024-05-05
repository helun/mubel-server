package io.mubel.provider.jdbc.configuration;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@ConfigurationProperties(prefix = "mubel.provider.jdbc")
public class JdbcProviderProperties {

    private boolean enabled = true;
    private List<DataSourceProperties> datasources = new ArrayList<>();

    private List<BackendProperties> backends;

    private SystemDbProperties systemdb;

    public SystemDbProperties getSystemdb() {
        return systemdb;
    }

    public void setSystemdb(SystemDbProperties systemdb) {
        this.systemdb = systemdb;
    }

    public List<BackendProperties> getBackends() {
        return backends;
    }

    public void setBackends(List<BackendProperties> backends) {
        this.backends = backends;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public void setDatasources(List<DataSourceProperties> datasources) {
        this.datasources = datasources;
    }

    public List<DataSourceProperties> getDatasources() {
        return datasources;
    }

    public Optional<DataSourceProperties> findDataSource(String name) {
        return datasources.stream()
                .filter(dataSourceProperties -> dataSourceProperties.getName().equals(name))
                .findFirst();
    }

    public Optional<BackendProperties> findBackend(String storageBackendName) {
        return backends.stream()
                .filter(backendProperties -> backendProperties.getName().equals(storageBackendName))
                .findFirst();
    }

    public static class DataSourceProperties extends org.springframework.boot.autoconfigure.jdbc.DataSourceProperties {
        private int maximumPoolSize;
        private long connectionTimeout = 1000;
        private long idleTimeout;
        private long maxLifetime;

        public DataSourceProperties() {
            setGenerateUniqueName(false);
        }

        public int getMaximumPoolSize() {
            return maximumPoolSize;
        }

        public void setMaximumPoolSize(int maximumPoolSize) {
            this.maximumPoolSize = maximumPoolSize;
        }

        public long getConnectionTimeout() {
            return connectionTimeout;
        }

        public void setConnectionTimeout(long connectionTimeout) {
            this.connectionTimeout = connectionTimeout;
        }

        public long getIdleTimeout() {
            return idleTimeout;
        }

        public void setIdleTimeout(long idleTimeout) {
            this.idleTimeout = idleTimeout;
        }

        public long getMaxLifetime() {
            return maxLifetime;
        }

        public void setMaxLifetime(long maxLifetime) {
            this.maxLifetime = maxLifetime;
        }
    }

    public static class BackendProperties {
        private String name;
        private String dataSource;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getDataSource() {
            return dataSource;
        }

        public void setDataSource(String dataSource) {
            this.dataSource = dataSource;
        }
    }

    public static class SystemDbProperties {
        private String dataSource;

        public String getDataSource() {
            return dataSource;
        }

        public void setDataSource(String dataSource) {
            this.dataSource = dataSource;
        }
    }
}

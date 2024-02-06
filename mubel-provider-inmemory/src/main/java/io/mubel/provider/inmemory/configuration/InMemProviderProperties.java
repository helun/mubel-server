package io.mubel.provider.inmemory.configuration;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.List;

@ConfigurationProperties(prefix = "mubel.provider.inmemory")
public class InMemProviderProperties {

    private boolean enabled = true;

    private boolean systemDb = false;

    private List<String> backends = new ArrayList<>();

    public List<String> getBackends() {
        return backends;
    }

    public void setBackends(List<String> backends) {
        this.backends = backends;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public boolean isSystemDb() {
        return systemDb;
    }

    public void setSystemDb(Boolean systemDb) {
        this.systemDb = systemDb;
    }
}

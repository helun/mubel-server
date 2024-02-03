package io.mubel.server;

import io.mubel.server.spi.Provider;
import io.mubel.server.spi.exceptions.ResourceNotFoundException;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

@Component
public class Providers {
    private final List<Provider> providers;

    public Providers(List<Provider> providers) {
        this.providers = providers;
    }

    public List<Provider> all() {
        return List.copyOf(providers);
    }

    public Provider get(String providerName) {
        return providers.stream()
                .filter(provider -> provider.name().equals(providerName))
                .findFirst()
                .orElseThrow(() -> new ResourceNotFoundException("No provider found for name: " + providerName));
    }

    public boolean backendExists(String backendName) {
        return findBackend(backendName).isPresent();
    }

    public Optional<Provider> findBackend(String backendName) {
        return find(provider -> hasBackend(provider, backendName));
    }

    private boolean hasBackend(Provider provider, String storageBackendName) {
        return provider.storageBackends()
                .stream()
                .anyMatch(backend -> backend.name().equals(storageBackendName));
    }

    public Optional<Provider> find(Predicate<Provider> predicate) {
        return providers.stream()
                .filter(predicate)
                .findFirst();

    }
}

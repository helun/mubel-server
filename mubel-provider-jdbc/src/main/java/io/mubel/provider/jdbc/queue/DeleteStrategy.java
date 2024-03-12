package io.mubel.provider.jdbc.queue;

import org.jdbi.v3.core.Jdbi;

import java.util.Collection;
import java.util.UUID;

public interface DeleteStrategy {

    void delete(Jdbi jdbi, Collection<UUID> uuids);

}

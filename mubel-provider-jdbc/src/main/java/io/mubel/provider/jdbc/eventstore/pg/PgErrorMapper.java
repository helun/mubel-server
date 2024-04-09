package io.mubel.provider.jdbc.eventstore.pg;

import com.google.common.base.Throwables;
import io.mubel.provider.jdbc.eventstore.ErrorMapper;
import io.mubel.server.spi.exceptions.EventRevisionConflictException;
import io.mubel.server.spi.exceptions.StorageBackendException;
import org.postgresql.util.PSQLException;

public class PgErrorMapper implements ErrorMapper {

    @Override
    public RuntimeException map(Exception e) {
        var root = Throwables.getRootCause(e);
        if (root instanceof PSQLException sqle) {
            if (PgErrorCode.UNIQUE_VIOLATION.equals(sqle.getSQLState())) {
                if (PgEventStoreStatements.isVersionConflictError(sqle.getMessage())) {
                    return new EventRevisionConflictException(
                            PgErrorMessageParser.parseVersionConflictError(sqle.getMessage())
                    );
                }
            }
        }
        return new StorageBackendException(e.getMessage(), e);
    }
}

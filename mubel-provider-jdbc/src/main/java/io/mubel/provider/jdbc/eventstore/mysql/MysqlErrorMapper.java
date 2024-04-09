package io.mubel.provider.jdbc.eventstore.mysql;


import com.google.common.base.Throwables;
import io.mubel.provider.jdbc.eventstore.ErrorMapper;
import io.mubel.server.spi.exceptions.EventRevisionConflictException;
import io.mubel.server.spi.exceptions.StorageBackendException;

import java.sql.SQLException;

public class MysqlErrorMapper implements ErrorMapper {

    @Override
    public RuntimeException map(Exception e) {
        var root = Throwables.getRootCause(e);
        if (root instanceof SQLException mse) {
            if (mse.getErrorCode() == MysqlErrorCode.DUPLICATE_ENTRY) {
                if (mse.getMessage().contains("sid_ver")) {
                    return new EventRevisionConflictException(
                            MysqlErrorMessageParser.parseVersionConflictError(mse.getMessage())
                    );
                }
            }
        }
        return new StorageBackendException(e.getMessage(), e);
    }


}

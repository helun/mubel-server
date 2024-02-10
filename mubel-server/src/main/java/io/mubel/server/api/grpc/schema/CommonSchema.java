package io.mubel.server.api.grpc.schema;

import io.mubel.schema.IntSchema;
import io.mubel.schema.Schema;
import io.mubel.schema.StringSchema;

import static io.mubel.schema.Constrains.ESID_PTRN;
import static io.mubel.schema.Constrains.SAFE_STRING_PTRN;

public class CommonSchema {

    public final static StringSchema.Builder safeString = Schema.string("proto")
            .pattern("alpha-numeric", SAFE_STRING_PTRN);
    public final static StringSchema.Builder esidSchema = Schema.string("esid")
            .pattern("esid", ESID_PTRN);
    public final static StringSchema.Builder streamIdSchema = Schema.string("streamId").uuid();
    public final static IntSchema versionSchema = Schema.integer("version").positive().build();
    public final static IntSchema fromVersionSchema = Schema.integer("fromVersion").positive().build();
    public final static IntSchema sizeSchema = Schema.integer("size")
            .defaultValue(999)
            .maxExclusive(1000)
            .minInclusive(1)
            .build();
}

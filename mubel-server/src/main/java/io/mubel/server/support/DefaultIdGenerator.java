package io.mubel.server.support;

import com.fasterxml.uuid.Generators;
import com.fasterxml.uuid.NoArgGenerator;
import io.mubel.server.spi.support.IdGenerator;

import java.util.UUID;

public class DefaultIdGenerator implements IdGenerator {

    private static final DefaultIdGenerator DEFAULT = createDefaultGenerator();

    private static DefaultIdGenerator createDefaultGenerator() {
        final var defaultType = System.getProperty("mubel.id.generator", "ordered");
        return switch (defaultType) {
            case "ordered" -> timebasedGenerator();
            case "random" -> randomGenerator();
            default -> throw new IllegalArgumentException("Unknown id generator type: " + defaultType);
        };
    }

    private final NoArgGenerator generator;


    private DefaultIdGenerator(NoArgGenerator generator) {
        this.generator = generator;
    }

    public static DefaultIdGenerator timebasedGenerator() {
        return new DefaultIdGenerator(Generators.timeBasedReorderedGenerator());
    }

    public static DefaultIdGenerator randomGenerator() {
        return new DefaultIdGenerator(Generators.timeBasedEpochGenerator());
    }

    @Override
    public String generateStringId() {
        return generator.generate().toString();
    }

    @Override
    public UUID generate() {
        return generator.generate();
    }

    public static DefaultIdGenerator defaultGenerator() {
        return DEFAULT;
    }
}

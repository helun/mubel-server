package io.mubel.provider.jdbc.queue;

import io.mubel.server.spi.queue.*;
import io.mubel.server.spi.support.IdGenerator;
import io.mubel.server.spi.support.TimeBudget;
import org.jdbi.v3.core.Jdbi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.Collection;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class JdbcMessageQueueService implements MessageQueueService {

    private final static Logger LOG = LoggerFactory.getLogger(JdbcMessageQueueService.class);
    private static final long MINIMUM_SLEEP_TIME = 100;

    private final Jdbi jdbi;
    private final MessageQueueStatements statements;
    private final IdGenerator idGenerator;
    private final WaitStrategy waitStrategy;
    private final PollStrategy pollStrategy;
    private final Duration visibilityTimeout;
    private final DeleteStrategy deleteStrategy;
    private final int delayOffsetMs;
    private final AtomicBoolean shouldRun = new AtomicBoolean(true);
    private Disposable messageTimeoutEnforcer;

    public static JdbcMessageQueueService.Builder builder() {
        return new JdbcMessageQueueService.Builder();
    }

    private JdbcMessageQueueService(Builder b) {
        this.jdbi = b.jdbi;
        this.statements = b.statements;
        this.idGenerator = b.idGenerator;
        this.waitStrategy = b.waitStrategy;
        this.visibilityTimeout = b.visibilityTimeout;
        this.pollStrategy = b.pollStrategy;
        this.deleteStrategy = b.deleteStrategy;
        this.delayOffsetMs = b.delayOffsetMs;
    }

    public void start() {
        final var sleepTime = Math.max(visibilityTimeout.toMillis() / 4, MINIMUM_SLEEP_TIME);
        this.messageTimeoutEnforcer = Flux.interval(Duration.ofMillis(sleepTime))
                .subscribeOn(Schedulers.fromExecutorService(Executors.newVirtualThreadPerTaskExecutor()))
                .doOnError(e -> LOG.error("Error enforcing visibility timeout", e))
                .subscribe(i -> enforceVisibilityTimeout());
    }

    private void enforceVisibilityTimeout() {
        jdbi.useTransaction(h -> {
            var rows = h.createUpdate(statements.enforceVisibilityTimeout())
                    .execute();
            if (rows > 0) {
                LOG.debug("Enforced visibility timeout on {} messages", rows);
            }
        });
    }

    @Override
    public void send(SendRequest request) {
        LOG.debug("sending message: {}", request);
        jdbi.useTransaction(h -> {
            long effectiveDelay = effectiveDelay(request.delayMillis());
            h.createUpdate(statements.insert())
                    .bind(0, idGenerator.generate())
                    .bind(1, request.queueName())
                    .bind(2, request.type())
                    .bind(3, request.payload())
                    .bind(4, effectiveDelay)
                    .bind(5, effectiveDelay)
                    .execute();
        });
    }

    @Override
    public void send(BatchSendRequest request) {
        LOG.debug("sending batch to {}", request.queueName());
        jdbi.useTransaction(h -> {
            var batch = h.prepareBatch(statements.insert());
            for (var entry : request.entries()) {
                long effectiveDelay = effectiveDelay(entry.delayMillis());
                batch.bind(0, idGenerator.generate())
                        .bind(1, request.queueName())
                        .bind(2, entry.type())
                        .bind(3, entry.payload())
                        .bind(4, effectiveDelay)
                        .bind(5, effectiveDelay)
                        .add();
            }
            batch.execute();
        });
    }

    private long effectiveDelay(long delay) {
        return delay + this.delayOffsetMs;
    }

    @Override
    public Flux<Message> receive(ReceiveRequest request) {
        LOG.debug("got receive request: {}", request);
        return Flux.create(sink -> {
            try {
                final var timeBudget = new TimeBudget(request.timeout());
                final var pollContext = new QueuePollContext(
                        jdbi,
                        request.queueName(),
                        request.maxMessages(),
                        visibilityTimeout,
                        sink,
                        shouldRun
                );
                while (timeBudget.hasTimeRemaining()
                        && pollContext.shouldContinue()
                ) {
                    pollStrategy.poll(pollContext);
                    if (pollContext.shouldContinue()) {
                        waitStrategy.wait(timeBudget);
                    }
                }
            } catch (Exception e) {
                sink.error(e);
            }
            sink.complete();
            LOG.debug("receive request completed");
        });
    }

    @Override
    public void delete(Collection<UUID> uuids) {
        deleteStrategy.delete(jdbi, uuids);
    }

    @Override
    public void stop() {
        shouldRun.set(false);
        Optional.ofNullable(messageTimeoutEnforcer).ifPresent(Disposable::dispose);
    }

    public static class Builder {
        private Jdbi jdbi;
        private MessageQueueStatements statements;
        private IdGenerator idGenerator;
        private WaitStrategy waitStrategy;
        private PollStrategy pollStrategy;
        private Duration visibilityTimeout = Duration.ofSeconds(30);
        private DeleteStrategy deleteStrategy;
        private int delayOffsetMs = 0;

        public Builder jdbi(Jdbi jdbi) {
            this.jdbi = jdbi;
            return this;
        }

        public Builder statements(MessageQueueStatements statements) {
            this.statements = statements;
            return this;
        }

        public Builder idGenerator(IdGenerator idGenerator) {
            this.idGenerator = idGenerator;
            return this;
        }

        public Builder waitStrategy(WaitStrategy waitStrategy) {
            this.waitStrategy = waitStrategy;
            return this;
        }

        public Builder pollStrategy(PollStrategy pollStrategy) {
            this.pollStrategy = pollStrategy;
            return this;
        }

        public Builder visibilityTimeout(Duration visibilityTimeout) {
            this.visibilityTimeout = visibilityTimeout;
            return this;
        }

        public Builder deleteStrategy(DeleteStrategy deleteStrategy) {
            this.deleteStrategy = deleteStrategy;
            return this;
        }

        public Builder delayOffsetMs(int delayOffsetMs) {
            this.delayOffsetMs = delayOffsetMs;
            return this;
        }

        public JdbcMessageQueueService build() {
            if (deleteStrategy == null) {
                deleteStrategy = new DefaultDeleteStrategy(statements);
            }
            return new JdbcMessageQueueService(this);
        }
    }
}

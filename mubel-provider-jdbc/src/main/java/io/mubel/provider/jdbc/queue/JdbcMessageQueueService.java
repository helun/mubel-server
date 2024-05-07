package io.mubel.provider.jdbc.queue;

import io.mubel.server.spi.queue.*;
import io.mubel.server.spi.support.IdGenerator;
import io.mubel.server.spi.support.TimeBudget;
import org.jdbi.v3.core.Jdbi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.Executors;

public class JdbcMessageQueueService implements MessageQueueService {

    private final static Logger LOG = LoggerFactory.getLogger(JdbcMessageQueueService.class);

    private final Jdbi jdbi;
    private final MessageQueueStatements statements;
    private final IdGenerator idGenerator;
    private final WaitStrategy waitStrategy;
    private final PollStrategy pollStrategy;
    private final Duration visibilityTimeout;
    private final DeleteStrategy deleteStrategy;

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
    }

    public void start() {
        try (var es = Executors.newVirtualThreadPerTaskExecutor()) {
            es.execute(() -> {
                try {
                    final var sleepTime = Duration.ofMillis(500);
                    while (!Thread.currentThread().isInterrupted()) {
                        enforceVisibilityTimeout();
                        Thread.sleep(sleepTime);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (Exception e) {
                    LOG.error("Error enforcing visibility timeout", e);
                }
            });
        }
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
        jdbi.useTransaction(h -> h.createUpdate(statements.insert())
                .bind(0, idGenerator.generate())
                .bind(1, request.queueName())
                .bind(2, request.type())
                .bind(3, request.payload())
                .bind(4, request.delayMillis())
                .bind(5, request.delayMillis())
                .execute());
    }

    @Override
    public void send(BatchSendRequest request) {
        LOG.debug("sending batch to {}", request.queueName());
        jdbi.useTransaction(h -> {
            var batch = h.prepareBatch(statements.insert());
            for (var entry : request.entries()) {
                batch.bind(0, idGenerator.generate())
                        .bind(1, request.queueName())
                        .bind(2, entry.type())
                        .bind(3, entry.payload())
                        .bind(4, entry.delayMillis())
                        .bind(5, entry.delayMillis())
                        .add();
            }
            batch.execute();
        });
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
                        sink
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

    public static class Builder {
        private Jdbi jdbi;
        private MessageQueueStatements statements;
        private IdGenerator idGenerator;
        private WaitStrategy waitStrategy;
        private PollStrategy pollStrategy;
        private Duration visibilityTimeout = Duration.ofSeconds(30);
        private DeleteStrategy deleteStrategy;

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

        public JdbcMessageQueueService build() {
            if (deleteStrategy == null) {
                deleteStrategy = new DefaultDeleteStrategy(statements);
            }
            return new JdbcMessageQueueService(this);
        }
    }
}

package io.mubel.provider.jdbc.queue;

import io.mubel.server.spi.queue.*;
import io.mubel.server.spi.support.IdGenerator;
import io.mubel.server.spi.support.TimeBudget;
import org.jdbi.v3.core.Jdbi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.Executors;

public class JdbcMessageQueueService implements MessageQueueService {

    private final static Logger LOG = LoggerFactory.getLogger(JdbcMessageQueueService.class);

    private final Jdbi jdbi;
    private final MessageQueueStatements statements;
    private final IdGenerator idGenerator;
    private final WaitStrategy waitStrategy;
    private final Duration visibilityTimeout;

    public static JdbcMessageQueueService.Builder builder() {
        return new JdbcMessageQueueService.Builder();
    }

    private JdbcMessageQueueService(Builder b) {
        this.jdbi = b.jdbi;
        this.statements = b.statements;
        this.idGenerator = b.idGenerator;
        this.waitStrategy = b.waitStrategy;
        this.visibilityTimeout = b.visibilityTimeout;
    }

    public void start() {
        Executors.newVirtualThreadPerTaskExecutor().execute(() -> {
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    enforceVisibilityTimeout();
                    Thread.sleep(visibilityTimeout);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                LOG.error("Error enforcing visibility timeout", e);
            }
        });
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
        var visibleAt = new java.sql.Timestamp(System.currentTimeMillis() + request.delayMillis());
        jdbi.useHandle(h -> h.createUpdate(statements.insert())
                .bind(0, idGenerator.generate())
                .bind(1, request.queueName())
                .bind(2, request.type())
                .bind(3, request.payload())
                .bind(4, request.delayMillis())
                .bind(5, visibleAt)
                .execute());
    }

    @Override
    public void send(BatchSendRequest request) {
        jdbi.useTransaction(h -> {
            var batch = h.prepareBatch(statements.insert());
            for (var entry : request.entries()) {
                var visibleAt = new java.sql.Timestamp(System.currentTimeMillis() + entry.delayMillis());
                batch.bind(0, idGenerator.generate())
                        .bind(1, request.queueName())
                        .bind(2, entry.type())
                        .bind(3, entry.payload())
                        .bind(4, entry.delayMillis())
                        .bind(5, visibleAt)
                        .add();
            }
            batch.execute();
        });
    }

    @Override
    public Flux<Message> receive(ReceiveRequest request) {
        return Flux.create(sink -> {
            try {
                var timeBudget = new TimeBudget(request.timeout());
                int messageLimit = request.maxMessages();
                while (timeBudget.hasTimeRemaining()
                        && !sink.isCancelled()
                        && messageLimit > 0
                ) {
                    messageLimit -= poll(request, messageLimit, sink);
                    if (messageLimit > 0) {
                        waitStrategy.wait(timeBudget);
                    }
                }
            } catch (Exception e) {
                sink.error(e);
            }
            sink.complete();
        });
    }

    private int poll(ReceiveRequest request, int messageLimit, FluxSink<Message> sink) {
        var expiresAt = new java.sql.Timestamp(System.currentTimeMillis() + visibilityTimeout.toMillis());
        return jdbi.withHandle(h -> h.createQuery(statements.poll())
                .bind(0, request.queueName())
                .bind(1, messageLimit)
                .bind(2, expiresAt)
                .map(view -> new Message(
                        view.getColumn(1, UUID.class),
                        view.getColumn(2, String.class),
                        view.getColumn(3, String.class),
                        view.getColumn(4, byte[].class)
                ))
                .stream()
                .map(msg -> {
                    sink.next(msg);
                    return 1;
                }).count()
        ).intValue();
    }

    @Override
    public void delete(Iterable<UUID> uuids) {
        jdbi.useHandle(h -> {
            h.createUpdate(statements.delete())
                    .bindList("IDS", uuids)
                    .execute();
        });
    }

    public static class Builder {
        private Jdbi jdbi;
        private MessageQueueStatements statements;
        private IdGenerator idGenerator;
        private WaitStrategy waitStrategy;
        private Duration visibilityTimeout = Duration.ofSeconds(30);

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

        public Builder visibilityTimeout(Duration visibilityTimeout) {
            this.visibilityTimeout = visibilityTimeout;
            return this;
        }

        public JdbcMessageQueueService build() {
            return new JdbcMessageQueueService(this);
        }
    }
}

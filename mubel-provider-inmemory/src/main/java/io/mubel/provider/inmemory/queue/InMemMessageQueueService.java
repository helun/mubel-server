package io.mubel.provider.inmemory.queue;

import io.mubel.server.spi.queue.*;
import io.mubel.server.spi.support.IdGenerator;
import io.mubel.server.spi.support.TimeBudget;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class InMemMessageQueueService implements MessageQueueService {

    private static final Logger LOG = LoggerFactory.getLogger(InMemMessageQueueService.class);
    private static final String DEFAULT_DEADLINE_QUEUE_CONFIG_NAME = "deadlines";

    private final Map<String, DelayQueue<DelayedMessage>> queues = new ConcurrentHashMap<>();
    private final Map<UUID, ScheduledFuture<?>> messagesInFlight = new ConcurrentHashMap<>();

    private final IdGenerator idGenerator;
    private final QueueConfigurations config;
    private final ScheduledExecutorService inflightExecutor = Executors.newScheduledThreadPool(0, Thread.ofVirtual().factory());
    private final AtomicBoolean shouldRun = new AtomicBoolean(true);

    public InMemMessageQueueService(IdGenerator idGenerator, QueueConfigurations config) {
        this.idGenerator = idGenerator;
        this.config = config;
    }

    @Override
    public void send(SendRequest request) {
        addMessageToQueue(request.queueName(), request.type(), request.payload(), request.delayMillis());
    }

    @Override
    public void send(BatchSendRequest request) {
        for (var entry : request.entries()) {
            addMessageToQueue(request.queueName(), entry.type(), entry.payload(), entry.delayMillis());
        }
    }

    private void addMessageToQueue(String queueName, String type, byte[] payload, long delayMillis) {
        var queue = resolveQueue(queueName);
        LOG.debug("adding message to queue {}", queueName);
        queue.put(new DelayedMessage(new Message(idGenerator.generate(), queueName, type, payload), delayMillis));
    }

    private DelayQueue<DelayedMessage> resolveQueue(String queueName) {
        return queues.computeIfAbsent(queueName, k -> new DelayQueue<>());
    }

    @Override
    public Flux<Message> receive(ReceiveRequest request) {
        return Flux.<Message>create(sink -> {
                    var timeBudget = new TimeBudget(request.timeout());
                    var queue = resolveQueue(request.queueName());
                    var visibilityTimeout = this.config.getQueue(request.queueName(), DEFAULT_DEADLINE_QUEUE_CONFIG_NAME).visibilityTimeout().toMillis();
                    try {
                        LOG.debug("polling queue {}", request.queueName());
                        int receivedCount = 0;
                        while (timeBudget.hasTimeRemaining()
                                && receivedCount < request.maxMessages()
                                && !sink.isCancelled()
                                && shouldRun.get()
                        ) {
                            LOG.debug("polling queue {}, time remaining: {}", request.queueName(), timeBudget.remainingTimeMs());
                            var delayedMessage = queue.poll(timeBudget.remainingTimeMs(), TimeUnit.MILLISECONDS);
                            if (delayedMessage != null) {
                                LOG.debug("polled message {}", delayedMessage.message().messageId());
                                sink.next(delayedMessage.message());
                                scheduleInFlightTimeout(delayedMessage.message(), visibilityTimeout);
                                receivedCount++;
                            }
                        }
                        sink.complete();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        sink.complete();
                    }
                }).doOnComplete(() -> LOG.debug("Receive request completed"))
                .doOnError(e -> LOG.error("Error during receive", e))
                .doOnSubscribe(subscription -> LOG.debug("Receive request started"));
    }

    private void scheduleInFlightTimeout(Message message, long visibilityTimeout) {
        ScheduledFuture<?> future = inflightExecutor.schedule(() -> {
            resolveQueue(message.queueName()).put(new DelayedMessage(message, 0));
            messagesInFlight.remove(message.messageId());
            LOG.debug("Message {} has timed out", message.messageId());
        }, visibilityTimeout, TimeUnit.MILLISECONDS);
        messagesInFlight.put(message.messageId(), future);
        LOG.debug("In flight message {} scheduled", message.messageId());
    }

    @Override
    public void delete(Collection<UUID> uuids) {
        for (var uuid : uuids) {
            var future = messagesInFlight.remove(uuid);
            if (future != null) {
                future.cancel(false);
                LOG.debug("In flight message {} deleted / acknowledged", uuid);
            } else {
                queues.values()
                        .stream()
                        .filter(queue -> queue.removeIf(dm -> dm.message().messageId().equals(uuid)))
                        .findFirst();
            }
        }
    }

    public void reset() {
        messagesInFlight.forEach((key, value) -> value.cancel(false));
        messagesInFlight.clear();
        queues.clear();
    }

    @Override
    public void stop() {
        shouldRun.set(false);
        inflightExecutor.shutdownNow();
    }

    private static class DelayedMessage implements Delayed {
        private final Message message;

        private final long startTime;

        public DelayedMessage(Message message, long delay) {
            this.message = message;
            startTime = System.currentTimeMillis() + delay;
        }

        public Message message() {
            return message;
        }

        @Override
        public long getDelay(TimeUnit unit) {
            long diff = startTime - System.currentTimeMillis();
            return unit.convert(diff, TimeUnit.MILLISECONDS);
        }


        @Override
        public int compareTo(Delayed o) {
            return Long.compare(startTime - System.currentTimeMillis(), o.getDelay(TimeUnit.MILLISECONDS));
        }
    }
}
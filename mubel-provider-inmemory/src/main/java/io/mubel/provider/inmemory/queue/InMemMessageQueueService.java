package io.mubel.provider.inmemory.queue;

import io.mubel.server.spi.queue.*;
import io.mubel.server.spi.support.IdGenerator;
import reactor.core.publisher.Flux;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;

public class InMemMessageQueueService implements MessageQueueService {
    private final Map<String, DelayQueue<DelayedMessage>> queues = new ConcurrentHashMap<>();
    private final Map<UUID, ScheduledFuture<?>> messagesInFlight = new ConcurrentHashMap<>();

    private final IdGenerator idGenerator;
    private final QueueConfigurations config;
    private final ScheduledExecutorService inflightExecutor = Executors.newScheduledThreadPool(0, Thread.ofVirtual().factory());

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
        queue.put(new DelayedMessage(new Message(idGenerator.generate(), queueName, type, payload), delayMillis));
    }

    private DelayQueue<DelayedMessage> resolveQueue(String queueName) {
        return queues.computeIfAbsent(queueName, k -> new DelayQueue<>());
    }

    @Override
    public Flux<Message> receive(ReceiveRequest request) {
        return Flux.create(sink -> {
            long timeBudget = request.timeout().toMillis();
            var queue = resolveQueue(request.queueName());
            var visibilityTimeout = this.config.getQueue(request.queueName()).visibilityTimeout().toMillis();
            try {
                while (timeBudget > 0) {
                    var start = System.currentTimeMillis();
                    var delayedMessage = queue.poll(timeBudget, TimeUnit.MILLISECONDS);
                    if (delayedMessage != null) {
                        sink.next(delayedMessage.message());
                        scheduleInFlightTimeout(delayedMessage.message(), visibilityTimeout);
                    }
                    timeBudget -= System.currentTimeMillis() - start;
                }
                sink.complete();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                sink.complete();
            }
        });
    }

    private void scheduleInFlightTimeout(Message message, long visibilityTimeout) {
        ScheduledFuture<?> future = inflightExecutor.schedule(() -> {
            resolveQueue(message.queueName()).put(new DelayedMessage(message, 0));
            messagesInFlight.remove(message.messageId());
        }, visibilityTimeout, TimeUnit.MILLISECONDS);
        messagesInFlight.put(message.messageId(), future);
    }

    @Override
    public void delete(Iterable<UUID> uuids) {
        for (var uuid : uuids) {
            var future = messagesInFlight.remove(uuid);
            if (future != null) {
                future.cancel(false);
            } else {
                queues.values().stream()
                        .filter(queue -> queue.removeIf(dm -> dm.message().messageId().equals(uuid)))
                        .findFirst();
            }
        }
    }

    private static class DelayedMessage implements Delayed {
        private final Message message;
        private final long delay;

        public DelayedMessage(Message message, long delay) {
            this.message = message;
            this.delay = delay;
        }

        public Message message() {
            return message;
        }

        @Override
        public long getDelay(TimeUnit unit) {
            return unit.convert(delay, TimeUnit.MILLISECONDS);
        }

        @Override
        public int compareTo(Delayed o) {
            return Long.compare(delay, ((DelayedMessage) o).delay);
        }
    }
}
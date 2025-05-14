package com.github.deepend0.reactivestomp.simplebroker;

import io.vertx.core.impl.ConcurrentHashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.concurrent.*;

public class QueueProcessor implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(QueueProcessor.class);

    public static int ROUND_ROBIN_MESSAGE_BATCH_SIZE = 10;
    public static int NUM_WORKERS = 3;
    public static int NUM_THREADS = 3;

    private final ExecutorService executorService = Executors.newFixedThreadPool(NUM_THREADS);
    private final QueueWorker [] queueWorkers = new QueueWorker[NUM_WORKERS];

    private final QueueRegistry queueRegistry;
    private final ConcurrentHashSet<TopicSubscription> topicSubscriptions = new ConcurrentHashSet<>();
    private final BlockingDeque<TopicSubscription> topicSubscriptionsQueue = new LinkedBlockingDeque<>();

    public QueueProcessor(QueueRegistry queueRegistry) {
        this.queueRegistry = queueRegistry;
        for (int i = 0; i < NUM_WORKERS; i++) {
            queueWorkers[i] = new QueueWorker();
        }
    }

    private boolean stop = false;

    public void addTopicSubscription(TopicSubscription topicSubscription) {
        topicSubscriptions.add(topicSubscription);
        topicSubscriptionsQueue.add(topicSubscription);
    }

    public void removeTopicSubscription(TopicSubscription topicSubscription) {
        topicSubscriptions.remove(topicSubscription);
    }

    public void removeAllTopicSubscriptions(Subscriber subscriber) {
        topicSubscriptions.removeIf(topicSubscription -> topicSubscription.getSubscriber().equals(subscriber));
    }

    @Override
    public void run() {
        Arrays.stream(queueWorkers).forEach(
                QueueWorker::run
        );
    }

    public void stop() {
        stop = true;
    }

    public class QueueWorker {

        private Future<?> future;

        public void processSubscribers() {
            while (!stop) {
                TopicSubscription topicSubscription;
                try {
                    topicSubscription = topicSubscriptionsQueue.take();
                    if(topicSubscriptions.contains(topicSubscription)) {
                        TopicQueue topicQueue = queueRegistry.getQueue(topicSubscription.getTopic());
                        for (int i = 0; i < ROUND_ROBIN_MESSAGE_BATCH_SIZE
                                && topicSubscription.getOffset() + 1 < topicQueue.queueSize(); i++) {
                            topicSubscription.incOffset();
                            Object message = topicQueue.get(topicSubscription.getOffset());
                            topicSubscription.getEmitter().emit(message);
                        }
                        topicSubscriptionsQueue.add(topicSubscription);
                    } else {
                        topicSubscription.getEmitter().complete();
                    }
                } catch (InterruptedException e) {
                    LOGGER.error("Error while processing queue", e);
                }
            }
        }

        public void run() {
            future = executorService.submit(this::processSubscribers);
        }
    }

}

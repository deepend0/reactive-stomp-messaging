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
    private final ConcurrentHashMap<String, ConcurrentHashSet<TopicSubscription>> topicSubscriptionsMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ConcurrentHashSet<TopicSubscription>> subscriberSubscriptionsMap = new ConcurrentHashMap<>();
    private final BlockingDeque<TopicSubscription> topicSubscriptionsQueue = new LinkedBlockingDeque<>();

    public QueueProcessor(QueueRegistry queueRegistry) {
        this.queueRegistry = queueRegistry;
        for (int i = 0; i < NUM_WORKERS; i++) {
            queueWorkers[i] = new QueueWorker();
        }
    }

    private boolean stop = false;

    public void addTopicSubscription(TopicSubscription topicSubscription) {
        topicSubscriptionsMap
                .computeIfAbsent(topicSubscription.getTopic(), t->new ConcurrentHashSet<>())
                    .add(topicSubscription);
        subscriberSubscriptionsMap
                .computeIfAbsent(topicSubscription.getSubscriber().getId(), t->new ConcurrentHashSet<>())
                    .add(topicSubscription);
        topicSubscriptionsQueue.add(topicSubscription);
    }

    public void removeTopicSubscription(TopicSubscription topicSubscription) {
        topicSubscriptionsMap.get(topicSubscription.getTopic()).remove(topicSubscription);
        subscriberSubscriptionsMap.get(topicSubscription.getSubscriber().getId()).remove(topicSubscription);
    }

    public void removeSubscriptionsOfSubscriber(Subscriber subscriber) {
        subscriberSubscriptionsMap.get(subscriber.getId())
                .forEach(topicSubscription ->
                        topicSubscriptionsMap.get(topicSubscription.getTopic())
                                .remove(topicSubscription));
        subscriberSubscriptionsMap.remove(subscriber.getId());
    }

    public void updateTopicSubscriptionsQueue(String topic) {
        topicSubscriptionsMap.get(topic).forEach(topicSubscription -> {
            if(!topicSubscriptionsQueue.contains(topicSubscription)) {
                topicSubscriptionsQueue.add(topicSubscription);
            }
        });
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

    public void reset() {
        if(stop) {
            topicSubscriptionsQueue.clear();
            topicSubscriptionsMap.clear();
            subscriberSubscriptionsMap.clear();
        } else {
            throw new IllegalStateException("QueueProcessor is not stopped.");
        }
    }

    public class QueueWorker {

        private Future<?> future;

        public void processSubscribers() {
            while (!stop) {
                TopicSubscription topicSubscription;
                try {
                    topicSubscription = topicSubscriptionsQueue.take();
                    if(topicSubscriptionsMap.get(topicSubscription.getTopic()).contains(topicSubscription)) {
                        TopicQueue topicQueue = queueRegistry.getQueue(topicSubscription.getTopic());
                        for (int i = 0; i < ROUND_ROBIN_MESSAGE_BATCH_SIZE
                                && topicSubscription.getOffset() + 1 < topicQueue.queueSize(); i++) {
                            topicSubscription.incOffset();
                            Object message = topicQueue.get(topicSubscription.getOffset());
                            topicSubscription.getEmitter().emit(message);
                        }
                        LOGGER.info("Topic Subscription is being processed. Topic {} Subscriber {}", topicSubscription.getTopic(), topicSubscription.getSubscriber().getId());
                        if(topicSubscription.getOffset() + 1 < topicQueue.queueSize()) {
                            topicSubscriptionsQueue.add(topicSubscription);
                        }
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

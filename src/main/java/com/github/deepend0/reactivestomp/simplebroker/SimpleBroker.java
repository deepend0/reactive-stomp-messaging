package com.github.deepend0.reactivestomp.simplebroker;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;

public class SimpleBroker implements MessageBroker {

    private final QueueRegistry queueRegistry;
    private final QueueProcessor queueProcessor;

    public SimpleBroker(QueueRegistry queueRegistry, QueueProcessor queueProcessor) {
        this.queueRegistry = queueRegistry;
        this.queueProcessor = queueProcessor;
    }

    @Override
    public Uni<Void> send(String topic, Object message) {
        queueRegistry.addIntoQueue(topic, message);
        return Uni.createFrom().voidItem();
    }

    @Override
    public Multi<?> subscribe(Subscriber subscriber, String topic) {
        var topicQueue = queueRegistry.getQueue(topic);
        topicQueue.addSubscriber(subscriber);
        TopicSubscription topicSubscription = new TopicSubscription(topic, subscriber);
        queueProcessor.addTopicSubscription(topicSubscription);
        var multi = Multi.createFrom().emitter(multiEmitter -> topicSubscription.setEmitter(multiEmitter));
        return multi;
    }

    @Override
    public Uni<Void> unsubscribe(Subscriber subscriber, String topic) {
        var topicQueue = queueRegistry.getQueue(topic);
        topicQueue.removeSubscriber(subscriber);
        TopicSubscription topicSubscription = new TopicSubscription(topic, subscriber);
        queueProcessor.removeTopicSubscription(topicSubscription);
        return Uni.createFrom().voidItem();
    }

    @Override
    public Uni<Void> unsubscribeAll(Subscriber subscriber) {
        queueProcessor.removeAllTopicSubscriptions(subscriber);
        return Uni.createFrom().voidItem();
    }

    @Override
    public void run() {
        queueProcessor.run();
    }

    @Override
    public void stop() {
        queueProcessor.stop();
    }

    public static SimpleBroker build() {
        QueueRegistry queueRegistry = new QueueRegistry();
        QueueProcessor queueProcessor = new QueueProcessor(queueRegistry);
        return new SimpleBroker(queueRegistry, queueProcessor);
    }
}

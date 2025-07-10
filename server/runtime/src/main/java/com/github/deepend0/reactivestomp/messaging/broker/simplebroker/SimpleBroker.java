package com.github.deepend0.reactivestomp.messaging.broker.simplebroker;

import com.github.deepend0.reactivestomp.messaging.broker.MessageBroker;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;

import java.util.concurrent.CompletableFuture;

public class SimpleBroker implements MessageBroker {

    private final QueueRegistry queueRegistry;
    private final QueueProcessor queueProcessor;

    public SimpleBroker(QueueRegistry queueRegistry, QueueProcessor queueProcessor) {
        this.queueRegistry = queueRegistry;
        this.queueProcessor = queueProcessor;
    }

    @Override
    public Uni<Void> send(String topic, Object message) {
        CompletableFuture<Void> completableFuture = CompletableFuture.runAsync( ()-> {
            queueRegistry.addIntoQueue(topic, message);
            queueProcessor.updateTopicSubscriptionsQueue(topic);
        });
        return Uni.createFrom().completionStage(completableFuture);
    }

    @Override
    public Multi<?> subscribe(Subscriber subscriber, String topic) {
        var topicQueue = queueRegistry.getQueue(topic);
        topicQueue.addSubscriber(subscriber);
        TopicSubscription topicSubscription = new TopicSubscription(topic, subscriber);
        topicSubscription.setOffset(topicQueue.queueSize() - 1);
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
        queueProcessor.removeSubscriptionsOfSubscriber(subscriber);
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

    @Override
    public void reset() {
        queueProcessor.reset();
        queueRegistry.reset();
    }

    public static SimpleBroker build() {
        QueueRegistry queueRegistry = new QueueRegistry();
        QueueProcessor queueProcessor = new QueueProcessor(queueRegistry);
        return new SimpleBroker(queueRegistry, queueProcessor);
    }
}

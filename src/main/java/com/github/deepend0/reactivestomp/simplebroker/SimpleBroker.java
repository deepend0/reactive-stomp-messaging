package com.github.deepend0.reactivestomp.simplebroker;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SimpleBroker implements MessageBroker {

    private final QueueRegistry queueRegistry;
    private final QueueProcessor queueProcessor;
    private final Map<String, Multi<Object>> multiMap = new ConcurrentHashMap<>();

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
    public Multi<Object> subscribe(Subscriber subscriber, String topic) {
        var topicQueue = queueRegistry.getQueue(topic);
        topicQueue.addSubscriber(subscriber);
        TopicSubscription topicSubscription = new TopicSubscription(topic, subscriber);
        queueProcessor.addTopicSubscription(topicSubscription);
        var multi = Multi.createFrom().emitter(multiEmitter -> topicSubscription.setEmitter(multiEmitter));
        multiMap.put(topic, multi);
        return multi;
    }

    @Override
    public Uni<Void> unsubscribe(Subscriber subscriber, String topic) {
        var topicQueue = queueRegistry.getQueue(topic);
        topicQueue.removeSubscriber(subscriber);
        TopicSubscription topicSubscription = new TopicSubscription(topic, subscriber);
        queueProcessor.removeTopicSubscription(topicSubscription);
        multiMap.remove(topic);
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
}

package com.github.deepend0.reactivestomp.messaging.broker.simplebroker;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class TopicQueue {

    private final String topic;
    private final List<Object> queue = new ArrayList<>();
    private final Set<Subscriber> subscribers = new HashSet<>();

    public TopicQueue(String topic) {
        this.topic = topic;
    }

    public void addSubscriber(Subscriber subscriber) {
        this.subscribers.add(subscriber);
    }

    public void removeSubscriber(Subscriber subscriber) {
        subscribers.remove(subscriber);
    }
    
    public void appendQueue(Object o) {
        queue.add(o);
    }

    public int queueSize() {
        return queue.size();
    }

    public Object get(int index) {
        return queue.get(index);
    }
}

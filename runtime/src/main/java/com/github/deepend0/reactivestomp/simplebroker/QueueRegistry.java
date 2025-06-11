package com.github.deepend0.reactivestomp.simplebroker;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class QueueRegistry {
    private final Map<String, TopicQueue> topicQueueMap = new ConcurrentHashMap<>();

    public void addIntoQueue(String topic, Object message) {
        var topicQueue = topicQueueMap.computeIfAbsent(topic, TopicQueue::new);
        topicQueue.appendQueue(message);
    }

    public TopicQueue getQueue(String topic) {
        return topicQueueMap.computeIfAbsent(topic, TopicQueue::new);
    }

    public void reset(){
        topicQueueMap.clear();
    }
}

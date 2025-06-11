package com.github.deepend0.reactivestomp.simplebroker;

import io.smallrye.mutiny.subscription.MultiEmitter;

import java.util.Objects;

public class TopicSubscription {
    private String topic;
    private Subscriber subscriber;
    private int offset = -1;
    private MultiEmitter<Object> emitter;

    public TopicSubscription(String topic, Subscriber subscriber) {
        this.topic = topic;
        this.subscriber = subscriber;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public Subscriber getSubscriber() {
        return subscriber;
    }

    public void setSubscriber(Subscriber subscriber) {
        this.subscriber = subscriber;
    }

    public int getOffset() {
        return offset;
    }

    public void incOffset() {
        offset++;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    public MultiEmitter<Object> getEmitter() {
        return emitter;
    }

    public void setEmitter(MultiEmitter<Object> emitter) {
        this.emitter = emitter;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TopicSubscription that)) return false;
        return Objects.equals(topic, that.topic) && Objects.equals(subscriber, that.subscriber);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topic, subscriber);
    }
}

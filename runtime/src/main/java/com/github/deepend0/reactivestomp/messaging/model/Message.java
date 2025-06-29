package com.github.deepend0.reactivestomp.messaging.model;

public abstract class Message {
    private final String subscriberId;

    public Message(String subscriberId) {
        this.subscriberId = subscriberId;
    }

    public String getSubscriberId() {
        return subscriberId;
    }
}

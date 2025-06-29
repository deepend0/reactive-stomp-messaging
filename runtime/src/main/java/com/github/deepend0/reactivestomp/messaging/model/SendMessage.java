package com.github.deepend0.reactivestomp.messaging.model;

public class SendMessage extends Message {
    private final String destination;
    private final byte [] payload;

    public SendMessage(String subscriberId, String destination, byte[] payload) {
        super(subscriberId);
        this.destination = destination;
        this.payload = payload;
    }

    public String getDestination() {
        return destination;
    }

    public byte[] getPayload() {
        return payload;
    }
}

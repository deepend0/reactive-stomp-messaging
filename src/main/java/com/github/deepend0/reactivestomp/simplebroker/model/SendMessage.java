package com.github.deepend0.reactivestomp.simplebroker.model;

public class SendMessage extends BrokerMessage {
    private final String destination;
    private final byte [] payload;

    public SendMessage(String sessionId, String destination, byte[] payload) {
        super(sessionId);
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

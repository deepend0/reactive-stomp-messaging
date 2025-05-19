package com.github.deepend0.reactivestomp.simplebroker.messagehandler;

public class SendMessage extends BrokerMessage {
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

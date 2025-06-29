package com.github.deepend0.reactivestomp.messaging.model;

public class DisconnectMessage extends Message {
    public DisconnectMessage(String sessionId) {
        super(sessionId);
    }
}

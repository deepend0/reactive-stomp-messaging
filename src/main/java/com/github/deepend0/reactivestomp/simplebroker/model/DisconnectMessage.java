package com.github.deepend0.reactivestomp.simplebroker.model;

public class DisconnectMessage extends BrokerMessage {
    public DisconnectMessage(String sessionId) {
        super(sessionId);
    }
}

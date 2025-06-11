package com.github.deepend0.reactivestomp.simplebroker.messagehandler;

public class DisconnectMessage extends BrokerMessage {
    public DisconnectMessage(String sessionId) {
        super(sessionId);
    }
}

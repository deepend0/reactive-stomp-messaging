package com.github.deepend0.reactivestomp.websocket;

public record ExternalMessage (String sessionId, byte [] message) {
}

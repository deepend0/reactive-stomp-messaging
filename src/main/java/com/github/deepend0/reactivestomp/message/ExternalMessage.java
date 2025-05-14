package com.github.deepend0.reactivestomp.message;

public record ExternalMessage (String sessionId, byte [] message) {
}

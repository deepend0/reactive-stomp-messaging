package com.github.deepend0.reactivestomp.external;

public record ExternalMessage (String sessionId, byte [] message) {
}

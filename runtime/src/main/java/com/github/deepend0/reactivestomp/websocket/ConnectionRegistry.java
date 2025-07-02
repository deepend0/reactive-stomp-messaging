package com.github.deepend0.reactivestomp.websocket;

import io.quarkus.websockets.next.WebSocketConnection;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.reactive.messaging.Incoming;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@ApplicationScoped
public class ConnectionRegistry {
    Map<String, WebSocketConnection> registry = new ConcurrentHashMap<>();

    public void add(String id, WebSocketConnection connection) {
        registry.put(id, connection);
    }

    public void remove(String id) {
        registry.remove(id);
    }

    public WebSocketConnection get(String id) {
        return registry.get(id);
    }

    @Incoming("serverOutbound")
    public Uni<Void> sendOutgoingMessage(ExternalMessage externalMessage) {
        return registry.get(externalMessage.sessionId()).sendBinary(externalMessage.message());
    }
}

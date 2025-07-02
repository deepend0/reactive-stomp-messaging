package com.github.deepend0.reactivestomp.websocket;

import io.quarkus.websockets.next.OpenConnections;
import io.quarkus.websockets.next.WebSocketConnection;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.reactive.messaging.Incoming;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@ApplicationScoped
public class ConnectionRegistry {

    @Inject
    OpenConnections openConnections;

    Map<String, WebSocketConnection> registry = new ConcurrentHashMap<>();

    public void add(String id) {
        registry.put(id, openConnections.findByConnectionId(id).get());
    }

    public void remove(String id) {
        registry.remove(id);
    }

    public WebSocketConnection get(String id) {
        return registry.get(id);
    }

    @Incoming("serverOutbound")
    public Uni<Void> sendOutgoingMessage(ExternalMessage externalMessage) {
        if (Arrays.equals(externalMessage.message(), new byte[]{'\0'})) {
            return registry.get(externalMessage.sessionId()).close();
        }
        return registry.get(externalMessage.sessionId()).sendBinary(externalMessage.message());
    }
}

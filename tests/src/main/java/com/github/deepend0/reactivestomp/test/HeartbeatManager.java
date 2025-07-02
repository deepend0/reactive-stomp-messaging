package com.github.deepend0.reactivestomp.test;

import io.quarkus.scheduler.Scheduled;
import io.quarkus.websockets.next.WebSocketClientConnection;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.stomp.impl.FrameParser;
import jakarta.enterprise.context.ApplicationScoped;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@ApplicationScoped
public class HeartbeatManager {

    private final Set<WebSocketClientConnection> connections = ConcurrentHashMap.newKeySet();

    public void register(WebSocketClientConnection connection) {
        connections.add(connection);
    }

    public void unregister(WebSocketClientConnection connection) {
        connections.remove(connection);
    }

    @Scheduled(every = "1s")
    void sendHeartbeats() {
        byte[] heartbeat = Buffer.buffer(FrameParser.EOL).getBytes();
        for (WebSocketClientConnection connection : connections) {
            connection.sendBinaryAndAwait(heartbeat);
        }
    }
}
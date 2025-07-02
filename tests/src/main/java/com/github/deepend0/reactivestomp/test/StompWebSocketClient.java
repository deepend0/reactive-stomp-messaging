package com.github.deepend0.reactivestomp.test;


import io.quarkus.websockets.next.BasicWebSocketConnector;
import io.quarkus.websockets.next.WebSocketClientConnection;
import io.vertx.core.buffer.Buffer;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import java.net.URI;
import java.util.function.BiConsumer;

@ApplicationScoped
public class StompWebSocketClient {

    @Inject
    Instance<BasicWebSocketConnector> connector;

    public WebSocketClientConnection openAndConsume(String clientId, BiConsumer<WebSocketClientConnection, Buffer> consumer) {
        URI uri = URI.create("ws://localhost:8081/ws/");
        return connector
                .get()
                .baseUri(uri)
                .path("/stomp")
                .executionModel(BasicWebSocketConnector.ExecutionModel.NON_BLOCKING)
                .onBinaryMessage(consumer)
                .connectAndAwait();
    }
}

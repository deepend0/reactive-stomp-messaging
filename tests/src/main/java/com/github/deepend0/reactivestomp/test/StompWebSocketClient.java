package com.github.deepend0.reactivestomp.test;


import io.quarkus.websockets.next.BasicWebSocketConnector;
import io.quarkus.websockets.next.OnTextMessage;
import io.quarkus.websockets.next.WebSocketClient;
import io.quarkus.websockets.next.WebSocketClientConnection;
import io.quarkus.websockets.next.WebSocketConnector;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.net.URI;
import java.util.function.BiConsumer;

@ApplicationScoped
public class StompWebSocketClient {

    @Inject
    BasicWebSocketConnector connector;

    public WebSocketClientConnection openAndConsume(BiConsumer<WebSocketClientConnection, String> consumer) {
        return connector
                .baseUri("localhost")
                .path("/stomp")
                .executionModel(BasicWebSocketConnector.ExecutionModel.NON_BLOCKING)
                .onTextMessage(consumer)
                .connectAndAwait();
    }
}

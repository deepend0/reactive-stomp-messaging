package com.github.deepend0.reactivestomp.websocket;

import io.quarkus.websockets.next.*;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.MutinyEmitter;
import io.vertx.core.buffer.Buffer;
import org.eclipse.microprofile.reactive.messaging.Channel;

@WebSocket(path = "/ws/stomp")
public class StompWebSocketServer {

    private final WebSocketConnection webSocketConnection;

    private final ConnectionRegistry connectionRegistry;

    private final MutinyEmitter<ExternalMessage> serverInboundEmitter;

    public StompWebSocketServer(WebSocketConnection webSocketConnection,
                                ConnectionRegistry connectionRegistry,
                                @Channel("serverInbound") MutinyEmitter<ExternalMessage> serverInboundEmitter) {
        this.webSocketConnection = webSocketConnection;
        this.connectionRegistry = connectionRegistry;
        this.serverInboundEmitter = serverInboundEmitter;
    }

    @OnOpen
    public void onOpen() {
        connectionRegistry.add(webSocketConnection.id());
    }

    @OnClose
    public void onClose() {
        connectionRegistry.remove(webSocketConnection.id());
    }

    @OnBinaryMessage
    Uni<Void> consumeAsync(Buffer message) {
        ExternalMessage externalMessage = new ExternalMessage(webSocketConnection.id(), message.getBytes());
        return serverInboundEmitter.send(externalMessage);
    }

    @OnTextMessage
    Uni<Void> consumeAsync(String message) {
        ExternalMessage externalMessage = new ExternalMessage(webSocketConnection.id(), message.getBytes());
        return serverInboundEmitter.send(externalMessage);
    }
}
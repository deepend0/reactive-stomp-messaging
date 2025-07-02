package com.github.deepend0.reactivestomp.websocket;

import io.quarkus.websockets.next.*;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.MutinyEmitter;
import jakarta.enterprise.context.SessionScoped;
import org.eclipse.microprofile.reactive.messaging.Channel;

@WebSocket(path = "/stomp")
@SessionScoped
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
        connectionRegistry.add(webSocketConnection.id(), webSocketConnection);
    }

    @OnClose
    public void onClose() {
        connectionRegistry.remove(webSocketConnection.id());
    }

    @OnTextMessage
    Uni<Void> consumeAsync(byte [] message) {
        ExternalMessage externalMessage = new ExternalMessage(webSocketConnection.id(), message);
        return serverInboundEmitter.send(externalMessage);
    }
}
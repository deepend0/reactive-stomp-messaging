package com.github.deepend0.reactivestomp.test.integrationtest;

import com.github.deepend0.reactivestomp.test.StompWebSocketClient;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.websockets.next.WebSocketClientConnection;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.stomp.impl.FrameParser;
import jakarta.inject.Inject;

import java.util.Arrays;
import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;

@QuarkusTest
public class WebSocketBrokerIntegrationTest {
    @Inject
    private StompWebSocketClient stompWebSocketClient;

    private Deque<byte[]> receivedMessages = new ConcurrentLinkedDeque<>();
    private Deque<byte[]> receivedHeartbeats = new ConcurrentLinkedDeque<>();

    private WebSocketClientConnection createConnection() {
        return stompWebSocketClient.openAndConsume((connection, message) -> {
            var messageBytes = message.getBytes();
            if (Arrays.equals(messageBytes, Buffer.buffer(FrameParser.EOL).getBytes())
                    || Arrays.equals(messageBytes, new byte[]{'\0'})) {
                receivedHeartbeats.add(messageBytes);
            } else {
                receivedMessages.add(messageBytes);
            }
        });
    }
}

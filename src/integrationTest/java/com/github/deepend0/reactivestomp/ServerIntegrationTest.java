package com.github.deepend0.reactivestomp;

import com.github.deepend0.reactivestomp.external.ExternalMessage;
import com.github.deepend0.reactivestomp.stompprocessor.MessageIdGenerator;
import com.github.deepend0.reactivestomp.stompprocessor.StompFrameUtils;
import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.MutinyEmitter;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.stomp.impl.FrameParser;
import jakarta.inject.Inject;
import org.awaitility.Awaitility;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.mockito.Mockito;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;

@QuarkusTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ServerIntegrationTest {
    @Inject
    private Vertx vertx;
    @Inject
    @Channel("serverInbound")
    private MutinyEmitter<ExternalMessage> serverInboundEmitter;
    @Inject
    @Channel("serverOutbound")
    private Multi<ExternalMessage> serverOutboundReceiver;
    @InjectMock
    private MessageIdGenerator messageIdGenerator;

    private Deque<ExternalMessage> serverOutboundList = new ConcurrentLinkedDeque<>();
    private Deque<ExternalMessage> serverOutboundHeartbeats = new ConcurrentLinkedDeque<>();

    @BeforeAll
    public void init() {
        serverOutboundReceiver.subscribe().with(externalMessage -> {
            if (Arrays.equals(externalMessage.message(), Buffer.buffer(FrameParser.EOL).getBytes())
                || Arrays.equals(externalMessage.message(), new byte[]{'\0'})) {
                serverOutboundHeartbeats.add(externalMessage);
            } else {
                serverOutboundList.add(externalMessage);
            }
        });
    }

    @Test
    public void shouldSendMessageToMultipleSubscribers() {
        Mockito.when(messageIdGenerator.generate()).thenReturn("abcde");

        String session1 = "session1";
        String session2 = "session2";
        String session3 = "session3";

        String subscription1 = "sub1";
        String subscription2 = "sub2";
        String subscription3 = "sub3";

        String topic = "/topic/chat";
        String message = "Hello World!";

        long timer1 = connectClient(session1);
        long timer2 = connectClient(session2);
        long timer3 = connectClient(session3);

        subscribeClient(session1, subscription1, topic);
        subscribeClient(session2, subscription2, topic);
        subscribeClient(session3, subscription3, topic);

        sendMessage(session1, topic, message);

        receiveMessage(session1, subscription1, topic, message);
        receiveMessage(session2, subscription2, topic, message);
        receiveMessage(session3, subscription3, topic, message);

        disconnectClient(session1, timer1);
        disconnectClient(session2, timer2);
        disconnectClient(session3, timer3);
    }

    private long connectClient(String sessionId) {
        final byte[] connectFrame = StompFrameUtils.connectFrame("www.example.com", "500,500");
        final byte[] connectedFrame = StompFrameUtils.connectedFrame(sessionId, "1000,1000");
        ExternalMessage externalMessage = new ExternalMessage(sessionId, connectFrame);
        serverInboundEmitter.sendAndForget(externalMessage);
        long timerId = vertx.setPeriodic(100, l -> serverInboundEmitter.sendAndForget(new ExternalMessage(sessionId, Buffer.buffer(FrameParser.EOL).getBytes())));
        Awaitility.await().atMost(Duration.ofMillis(3000)).pollInterval(Duration.ofMillis(300)).until(() -> !serverOutboundList.isEmpty());
        ExternalMessage first = serverOutboundList.removeFirst();
        Assertions.assertEquals(sessionId, first.sessionId());
        Assertions.assertArrayEquals(connectedFrame, first.message());
        Awaitility.await().atMost(Duration.ofMillis(3000)).pollInterval(Duration.ofMillis(300)).until(() -> serverOutboundHeartbeats.size() > 2);
        serverOutboundList.clear();
        return timerId;
    }
    
    private void subscribeClient(String sessionId, String subId, String destination) {
        final byte [] subscribeFrame = StompFrameUtils.subscribeFrame(subId,  destination, "12345");
        final byte [] receiptFrame = StompFrameUtils.receiptFrame("12345");
        ExternalMessage externalMessage = new ExternalMessage(sessionId, subscribeFrame);
        serverInboundEmitter.sendAndForget(externalMessage);
        Awaitility.await().atMost(Duration.ofMillis(3000)).pollInterval(Duration.ofMillis(1000)).until(() -> !serverOutboundList.isEmpty());
        ExternalMessage first = serverOutboundList.removeFirst();
        Assertions.assertEquals(sessionId, first.sessionId());
        Assertions.assertArrayEquals(receiptFrame, first.message());
    }

    private void sendMessage(String sessionId, String destination, String message) {
        final byte [] sendFrame = StompFrameUtils.sendFrame(destination, "text/plain", "12346", message);
        final byte [] receiptFrame = StompFrameUtils.receiptFrame("12346");
        ExternalMessage externalMessage = new ExternalMessage(sessionId, sendFrame);
        serverInboundEmitter.sendAndForget(externalMessage);
        Awaitility.await().atMost(Duration.ofMillis(3000)).pollInterval(Duration.ofMillis(1000)).until(() -> !serverOutboundList.isEmpty());
        ExternalMessage first = serverOutboundList.removeFirst();
        Assertions.assertEquals(sessionId, first.sessionId());
        Assertions.assertArrayEquals(receiptFrame, first.message());
    }

    private void receiveMessage(String sessionId, String subId, String destination, String message) {
        final byte [] messageFrame = StompFrameUtils.messageFrame(destination, "abcde", subId, message);
        Awaitility.await().atMost(Duration.ofMillis(3000)).pollInterval(Duration.ofMillis(1000)).until(() -> !serverOutboundList.isEmpty());
        ExternalMessage externalMessage = serverOutboundList.removeFirst();
        Assertions.assertEquals(sessionId, externalMessage.sessionId());
        Assertions.assertArrayEquals(messageFrame, externalMessage.message());
    }

    private void disconnectClient(String sessionId, long timerId) {
        final byte [] disconnectFrame = StompFrameUtils.disconnectFrame("12347");
        final byte [] receiptFrame = StompFrameUtils.receiptFrame("12347");
        ExternalMessage externalMessage = new ExternalMessage(sessionId, disconnectFrame);
        serverInboundEmitter.sendAndForget(externalMessage);
        Awaitility.await().atMost(Duration.ofMillis(3000)).pollInterval(Duration.ofMillis(1000)).until(() -> !serverOutboundList.isEmpty());
        ExternalMessage first = serverOutboundList.removeFirst();
        Assertions.assertEquals(sessionId, first.sessionId());
        Assertions.assertArrayEquals(receiptFrame, first.message());
        vertx.cancelTimer(timerId);
    }
}

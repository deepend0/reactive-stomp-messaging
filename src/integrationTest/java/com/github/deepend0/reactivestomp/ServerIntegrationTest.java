package com.github.deepend0.reactivestomp;

import com.github.deepend0.reactivestomp.external.ExternalMessage;
import com.github.deepend0.reactivestomp.stompprocessor.StompFrameUtils;
import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.MutinyEmitter;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.stomp.impl.FrameParser;
import jakarta.inject.Inject;
import org.awaitility.Awaitility;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Deque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@QuarkusTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ServerIntegrationTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(ServerIntegrationTest.class);

    @Inject
    private Vertx vertx;
    @Inject
    @Channel("serverInbound")
    private MutinyEmitter<ExternalMessage> serverInboundEmitter;
    @Inject
    @Channel("serverOutbound")
    private Multi<ExternalMessage> serverOutboundReceiver;

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
        String session1 = "session1";
        String session2 = "session2";
        String session3 = "session3";

        String subscription1 = "sub1";
        String subscription2 = "sub2";
        String subscription3 = "sub3";

        String destination = "/topic/chat";
        String message = "Hello World!";

        long timer1 = connectClient(session1);
        long timer2 = connectClient(session2);
        long timer3 = connectClient(session3);

        subscribeClient(session1, subscription1, destination, "12345");
        subscribeClient(session2, subscription2, destination, "12346");
        subscribeClient(session3, subscription3, destination, "12347");

        sendMessage(session1, destination, message, "22345");

        CompletableFuture<Void> cf1 = CompletableFuture.runAsync(()->receiveMessage(session1, subscription1, destination, message));
        CompletableFuture<Void> cf2 = CompletableFuture.runAsync(()->receiveMessage(session2, subscription2, destination, message));
        CompletableFuture<Void> cf3 = CompletableFuture.runAsync(()->receiveMessage(session3, subscription3, destination, message));
        CompletableFuture.allOf(cf1, cf2, cf3).join();

        disconnectClient(session1, timer1, "32345");
        disconnectClient(session2, timer2, "32346");
        disconnectClient(session3, timer3, "32347");
    }

    private long connectClient(String sessionId) {
        final byte[] connectFrame = StompFrameUtils.connectFrame("www.example.com", "500,500");
        final byte[] connectedFrame = StompFrameUtils.connectedFrame(sessionId, "1000,1000");
        ExternalMessage externalMessage = new ExternalMessage(sessionId, connectFrame);
        serverInboundEmitter.sendAndForget(externalMessage);
        long timerId = vertx.setPeriodic(500, l ->  {
            serverInboundEmitter.sendAndForget(new ExternalMessage(sessionId, Buffer.buffer(FrameParser.EOL).getBytes()));
            LOGGER.info("Sending client heartbeat for session {}", sessionId);
        });
        Awaitility.await().atMost(Duration.ofMillis(3000)).pollInterval(Duration.ofMillis(300)).until(() -> !serverOutboundList.isEmpty());
        ExternalMessage first = serverOutboundList.removeFirst();
        Assertions.assertEquals(sessionId, first.sessionId());
        Assertions.assertArrayEquals(connectedFrame, first.message());
        Awaitility.await().atMost(Duration.ofMillis(3000)).pollInterval(Duration.ofMillis(300)).until(() -> serverOutboundHeartbeats.size() > 2);
        serverOutboundList.clear();
        return timerId;
    }
    
    private void subscribeClient(String sessionId, String subId, String destination, String receipt) {
        final byte [] subscribeFrame = StompFrameUtils.subscribeFrame(subId,  destination, receipt);
        final byte [] receiptFrame = StompFrameUtils.receiptFrame(receipt);
        ExternalMessage externalMessage = new ExternalMessage(sessionId, subscribeFrame);
        serverInboundEmitter.sendAndForget(externalMessage);
        Awaitility.await().atMost(Duration.ofMillis(3000)).pollInterval(Duration.ofMillis(300)).until(() -> !serverOutboundList.isEmpty());
        ExternalMessage first = serverOutboundList.removeFirst();
        Assertions.assertEquals(sessionId, first.sessionId());
        Assertions.assertArrayEquals(receiptFrame, first.message());
    }

    private void sendMessage(String sessionId, String destination, String message, String receipt) {
        final byte [] sendFrame = StompFrameUtils.sendFrame(destination, "text/plain", receipt, message);
        final byte [] receiptFrame = StompFrameUtils.receiptFrame(receipt);
        ExternalMessage externalMessage = new ExternalMessage(sessionId, sendFrame);
        serverInboundEmitter.sendAndForget(externalMessage);
        Awaitility.await().atMost(Duration.ofMillis(3000)).pollInterval(Duration.ofMillis(300)).until(() -> !serverOutboundList.isEmpty());
        ExternalMessage first = serverOutboundList.removeFirst();
        Assertions.assertEquals(sessionId, first.sessionId());
        Assertions.assertArrayEquals(receiptFrame, first.message());
    }

    private void receiveMessage(String sessionId, String subId, String destination, String message) {
        final byte [] messageFrame = StompFrameUtils.messageFrame(destination, ".*", subId, message);
        Awaitility.await().atMost(Duration.ofMillis(5000)).pollInterval(Duration.ofMillis(300)).until(() -> !serverOutboundList.isEmpty()
                && serverOutboundList.peek().sessionId().equals(sessionId));
        ExternalMessage externalMessage = serverOutboundList.removeFirst();
        Assertions.assertEquals(sessionId, externalMessage.sessionId());
        Matcher matcher = Pattern.compile(new String(messageFrame)).matcher(new String(externalMessage.message()));
        Assertions.assertTrue(matcher.matches());
    }

    private void disconnectClient(String sessionId, long timerId, String receipt) {
        final byte [] disconnectFrame = StompFrameUtils.disconnectFrame(receipt);
        final byte [] receiptFrame = StompFrameUtils.receiptFrame(receipt);
        ExternalMessage externalMessage = new ExternalMessage(sessionId, disconnectFrame);
        serverInboundEmitter.sendAndForget(externalMessage);
        Awaitility.await().atMost(Duration.ofMillis(3000)).pollInterval(Duration.ofMillis(300)).until(() -> !serverOutboundList.isEmpty());
        ExternalMessage first = serverOutboundList.removeFirst();
        Assertions.assertEquals(sessionId, first.sessionId());
        Assertions.assertArrayEquals(receiptFrame, first.message());
        vertx.cancelTimer(timerId);
    }
}

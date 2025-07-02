package com.github.deepend0.reactivestomp.test.broker;

import com.github.deepend0.reactivestomp.test.stompprocessor.FrameTestUtils;
import com.github.deepend0.reactivestomp.websocket.ExternalMessage;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.MutinyEmitter;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.stomp.impl.FrameParser;
import jakarta.inject.Inject;
import org.awaitility.Awaitility;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
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
@TestProfile(BrokerComponentTestProfile.class)
public class BrokerComponentTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(BrokerComponentTest.class);

    private final Vertx vertx = Vertx.vertx();

    @Inject
    @Channel("serverInbound")
    private MutinyEmitter<ExternalMessage> serverInboundEmitter;
    @Inject
    @Channel("serverOutbound")
    private Multi<ExternalMessage> serverOutboundReceiver;

    private Deque<ExternalMessage> serverOutboundList = new ConcurrentLinkedDeque<>();
    private Deque<ExternalMessage> serverOutboundHeartbeats = new ConcurrentLinkedDeque<>();

    @BeforeEach
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

        subscribeClient(session1, subscription1, destination, "1001");
        subscribeClient(session2, subscription2, destination, "1002");
        subscribeClient(session3, subscription3, destination, "1003");

        CompletableFuture<Void> cf1 = CompletableFuture.runAsync(()-> sendMessage(session1, destination, message, "1004"));
        CompletableFuture<Void> cf2 = CompletableFuture.runAsync(()->receiveMessage(session1, subscription1, destination, message));
        CompletableFuture<Void> cf3 = CompletableFuture.runAsync(()->receiveMessage(session2, subscription2, destination, message));
        CompletableFuture<Void> cf4 = CompletableFuture.runAsync(()->receiveMessage(session3, subscription3, destination, message));
        CompletableFuture.allOf(cf1, cf2, cf3, cf4).join();

        disconnectClient(session1, timer1, "1005");
        disconnectClient(session2, timer2, "1006");
        disconnectClient(session3, timer3, "1007");
    }

    @Test
    public void shouldSendMessageToSubscribedSubscribers() {
        String session1 = "session4";
        String session2 = "session5";
        String session3 = "session6";

        String subscription1 = "sub4";
        String subscription2 = "sub5";
        String subscription3 = "sub6";
        String subscription4 = "sub7";

        String destination1 = "/topic/chat2";
        String destination2 = "/topic/chat3";

        String message1 = "Hello World!";
        String message2 = "Hello Mars!";

        long timer1 = connectClient(session1);
        long timer2 = connectClient(session2);
        long timer3 = connectClient(session3);

        subscribeClient(session1, subscription1, destination1, "2001");
        subscribeClient(session2, subscription2, destination1, "2002");
        subscribeClient(session3, subscription3, destination2, "2003");

        CompletableFuture<Void> cf1 = CompletableFuture.runAsync(()->sendMessage(session1, destination1, message1, "2004"));
        CompletableFuture<Void> cf2 = CompletableFuture.runAsync(()->receiveMessage(session1, subscription1, destination1, message1));
        CompletableFuture<Void> cf3 = CompletableFuture.runAsync(()->receiveMessage(session2, subscription2, destination1, message1));
        CompletableFuture<Void> cf4 = CompletableFuture.runAsync(()-> receiveMessageNot(session3));
        CompletableFuture.allOf(cf1, cf2, cf3, cf4).join();

        unsubscribeClient(session2, subscription2, "2005");
        subscribeClient(session3, subscription4, destination1, "2006");

        CompletableFuture<Void> cf5 = CompletableFuture.runAsync(()->sendMessage(session1, destination1, message2, "2007"));
        CompletableFuture<Void> cf6 = CompletableFuture.runAsync(()->receiveMessage(session1, subscription1, destination1, message2));
        CompletableFuture<Void> cf7 = CompletableFuture.runAsync(()-> receiveMessageNot(session2));
        CompletableFuture<Void> cf8 = CompletableFuture.runAsync(()->receiveMessage(session3, subscription4, destination1, message2));
        CompletableFuture.allOf(cf5, cf6, cf7, cf8).join();

        disconnectClient(session1, timer1, "2008");
        disconnectClient(session2, timer2, "2009");
        disconnectClient(session3, timer3, "2010");
    }

    @Test
    public void shouldSendMessageToMultipleDestinations() {
        String session1 = "session7";
        String session2 = "session8";
        String session3 = "session9";

        String subscription1 = "sub8";
        String subscription2 = "sub9";
        String subscription3 = "sub10";

        String destination1 = "/topic/chat4";
        String destination2 = "/topic/chat5";
        String destination3 = "/topic/chat6";

        String message1 = "Hello World!";
        String message2 = "Hello Mars!";
        String message3 = "Hello Jupiter!";

        long timer1 = connectClient(session1);
        long timer2 = connectClient(session2);
        long timer3 = connectClient(session3);

        subscribeClient(session1, subscription1, destination1, "3001");
        subscribeClient(session2, subscription2, destination2, "3002");
        subscribeClient(session3, subscription3, destination3, "3003");

        CompletableFuture<Void> cf1 = CompletableFuture.runAsync(()->sendMessage(session1, destination2, message1, "3004"));
        CompletableFuture<Void> cf2 = CompletableFuture.runAsync(()->receiveMessage(session2, subscription2, destination2, message1));
        CompletableFuture.allOf(cf1, cf2).join();
        CompletableFuture<Void> cf3 = CompletableFuture.runAsync(()->sendMessage(session2, destination3, message2, "3005"));
        CompletableFuture<Void> cf4 = CompletableFuture.runAsync(()->receiveMessage(session3, subscription3, destination3, message2));
        CompletableFuture.allOf(cf3, cf4).join();
        CompletableFuture<Void> cf5 = CompletableFuture.runAsync(()->sendMessage(session3, destination1, message3, "3006"));
        CompletableFuture<Void> cf6 = CompletableFuture.runAsync(()->receiveMessage(session1, subscription1, destination1, message3));
        CompletableFuture.allOf(cf5, cf6).join();

        disconnectClient(session1, timer1, "3007");
        disconnectClient(session2, timer2, "3008");
        disconnectClient(session3, timer3, "3009");
    }

    @Test
    public void shouldDisconnectAndDontReceivesMessagesAndHeartbeatAnymore() throws InterruptedException {
        String session1 = "session10";
        String session2 = "session11";
        String session3 = "session12";

        String subscription2 = "sub12";
        String subscription3 = "sub13";

        String destination = "/topic/chat";
        String message = "Hello World!";

        long timer1 = connectClient(session1);
        long timer2 = connectClient(session2);
        long timer3 = connectClient(session3);

        subscribeClient(session2, subscription2, destination, "4002");
        subscribeClient(session3, subscription3, destination, "4003");

        CompletableFuture<Void> cf1 = CompletableFuture.runAsync(() -> sendMessage(session1, destination, message, "4004"));
        CompletableFuture<Void> cf2 = CompletableFuture.runAsync(() -> receiveMessage(session2, subscription2, destination, message));
        CompletableFuture<Void> cf3 = CompletableFuture.runAsync(() -> receiveMessage(session3, subscription3, destination, message));
        CompletableFuture.allOf(cf1, cf2, cf3).join();

        disconnectClient(session2, timer2, "4006");
        disconnectClient(session3, timer3, "4007");

        Thread.sleep(1000);
        serverOutboundHeartbeats.clear();

        CompletableFuture<Void> cf5 = CompletableFuture.runAsync(() -> sendMessage(session1, destination, message, "4005"));
        CompletableFuture<Void> cf7 = CompletableFuture.runAsync(() -> receiveFrameNot(session2));
        CompletableFuture<Void> cf8 = CompletableFuture.runAsync(() -> receiveFrameNot(session3));
        CompletableFuture.allOf(cf5, cf7, cf8).join();

        disconnectClient(session1, timer1, "4008");
    }

    private long connectClient(String sessionId) {
        final byte[] connectFrame = FrameTestUtils.connectFrame("www.example.com", "500,500");
        final byte[] connectedFrame = FrameTestUtils.connectedFrame(sessionId, "1000,1000");
        ExternalMessage externalMessage = new ExternalMessage(sessionId, connectFrame);
        serverInboundEmitter.sendAndForget(externalMessage);
        long timerId = vertx.setPeriodic(500, l ->  {
            serverInboundEmitter.sendAndForget(new ExternalMessage(sessionId, Buffer.buffer(FrameParser.EOL).getBytes()));
            LOGGER.info("Sending client heartbeat for session {}", sessionId);
        });
        Awaitility.await().atMost(Duration.ofMillis(3000)).pollInterval(Duration.ofMillis(300)).until(() -> !serverOutboundList.isEmpty()
                && serverOutboundList.peek().sessionId().equals(sessionId));
        ExternalMessage first = serverOutboundList.removeFirst();
        Assertions.assertEquals(sessionId, first.sessionId());
        Assertions.assertArrayEquals(connectedFrame, first.message());
        Awaitility.await().atMost(Duration.ofMillis(3000)).pollInterval(Duration.ofMillis(300)).until(() -> serverOutboundHeartbeats.size() > 2);
        return timerId;
    }
    
    private void subscribeClient(String sessionId, String subId, String destination, String receipt) {
        final byte [] subscribeFrame = FrameTestUtils.subscribeFrame(subId,  destination, receipt);
        final byte [] receiptFrame = FrameTestUtils.receiptFrame(receipt);
        ExternalMessage externalMessage = new ExternalMessage(sessionId, subscribeFrame);
        serverInboundEmitter.sendAndForget(externalMessage);
        Awaitility.await().atMost(Duration.ofMillis(3000)).pollInterval(Duration.ofMillis(300)).until(() -> !serverOutboundList.isEmpty()
                && serverOutboundList.peek().sessionId().equals(sessionId));
        ExternalMessage first = serverOutboundList.removeFirst();
        Assertions.assertEquals(sessionId, first.sessionId());
        Assertions.assertArrayEquals(receiptFrame, first.message());
    }

    private void unsubscribeClient(String sessionId, String subId, String receipt) {
        final byte [] unsubscribeFrame = FrameTestUtils.unsubscribeFrame(subId, receipt);
        final byte [] receiptFrame = FrameTestUtils.receiptFrame(receipt);
        ExternalMessage externalMessage = new ExternalMessage(sessionId, unsubscribeFrame);
        serverInboundEmitter.sendAndForget(externalMessage);
        Awaitility.await().atMost(Duration.ofMillis(3000)).pollInterval(Duration.ofMillis(300)).until(() -> !serverOutboundList.isEmpty()
                && serverOutboundList.peek().sessionId().equals(sessionId));
        ExternalMessage first = serverOutboundList.removeFirst();
        Assertions.assertEquals(sessionId, first.sessionId());
        Assertions.assertArrayEquals(receiptFrame, first.message());
    }

    private void sendMessage(String sessionId, String destination, String message, String receipt) {
        final byte [] sendFrame = FrameTestUtils.sendFrame(destination, "text/plain", receipt, message);
        final byte [] receiptFrame = FrameTestUtils.receiptFrame(receipt);
        ExternalMessage externalMessage = new ExternalMessage(sessionId, sendFrame);
        serverInboundEmitter.sendAndForget(externalMessage);
        Awaitility.await().atMost(Duration.ofMillis(3000)).pollInterval(Duration.ofMillis(300)).until(() -> !serverOutboundList.isEmpty()
                        && serverOutboundList.peek().sessionId().equals(sessionId));
        ExternalMessage first = serverOutboundList.removeFirst();
        Assertions.assertEquals(sessionId, first.sessionId());
        Assertions.assertArrayEquals(receiptFrame, first.message());
    }

    private void receiveMessage(String sessionId, String subId, String destination, String message) {
        final byte [] messageFrame = FrameTestUtils.messageFrame(destination, ".*", subId, message);
        Awaitility.await().atMost(Duration.ofMillis(5000)).pollInterval(Duration.ofMillis(1000)).until(() -> !serverOutboundList.isEmpty()
                && serverOutboundList.peek().sessionId().equals(sessionId));
        ExternalMessage externalMessage = serverOutboundList.removeFirst();
        Assertions.assertEquals(sessionId, externalMessage.sessionId());
        Matcher matcher = Pattern.compile(new String(messageFrame)).matcher(new String(externalMessage.message()));
        Assertions.assertTrue(matcher.matches());
    }

    private void receiveMessageNot(String sessionId) {
        Awaitility.await().timeout(Duration.ofMillis(3000)).until(() -> serverOutboundList.isEmpty()
                || !serverOutboundList.peek().sessionId().equals(sessionId));
    }

    private void receiveFrameNot(String sessionId) {
        Awaitility.await().timeout(Duration.ofMillis(3000)).until(() -> (serverOutboundList.isEmpty()
                || !serverOutboundList.peek().sessionId().equals(sessionId)) && (serverOutboundHeartbeats.isEmpty()
                || !serverOutboundHeartbeats.peek().sessionId().equals(sessionId)));
    }

    private void disconnectClient(String sessionId, long timerId, String receipt) {
        final byte [] disconnectFrame = FrameTestUtils.disconnectFrame(receipt);
        final byte [] receiptFrame = FrameTestUtils.receiptFrame(receipt);
        vertx.cancelTimer(timerId);
        ExternalMessage externalMessage = new ExternalMessage(sessionId, disconnectFrame);
        serverInboundEmitter.sendAndForget(externalMessage);
        Awaitility.await().atMost(Duration.ofMillis(3000)).pollInterval(Duration.ofMillis(300)).until(() -> !serverOutboundList.isEmpty()
                && serverOutboundList.peek().sessionId().equals(sessionId));
        ExternalMessage first = serverOutboundList.removeFirst();
        Assertions.assertEquals(sessionId, first.sessionId());
        Assertions.assertArrayEquals(receiptFrame, first.message());
    }
}

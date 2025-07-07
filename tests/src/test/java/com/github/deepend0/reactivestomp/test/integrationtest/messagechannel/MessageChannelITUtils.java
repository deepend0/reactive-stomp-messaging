package com.github.deepend0.reactivestomp.test.integrationtest.messagechannel;

import com.github.deepend0.reactivestomp.test.stompprocessor.FrameTestUtils;
import com.github.deepend0.reactivestomp.websocket.ExternalMessage;
import io.smallrye.reactive.messaging.MutinyEmitter;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.stomp.impl.FrameParser;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Deque;
import java.util.regex.Pattern;

public class MessageChannelITUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(MessageChannelITUtils.class);

    public static final Duration AWAIT_AT_MOST = Duration.ofMillis(3000);
    public static final Duration AWAIT_POLL_INTERVAL = Duration.ofMillis(100);

    private final Vertx vertx = Vertx.vertx();

    private MutinyEmitter<ExternalMessage> serverInboundEmitter;
    private Deque<ExternalMessage> serverOutboundList;
    private Deque<ExternalMessage> serverOutboundHeartbeats;

    public MessageChannelITUtils(MutinyEmitter<ExternalMessage> serverInboundEmitter, Deque<ExternalMessage> serverOutboundList, Deque<ExternalMessage> serverOutboundHeartbeats) {
        this.serverInboundEmitter = serverInboundEmitter;
        this.serverOutboundList = serverOutboundList;
        this.serverOutboundHeartbeats = serverOutboundHeartbeats;
    }

    public long connectClient(String sessionId) {
        final byte[] connectFrame = FrameTestUtils.connectFrame("www.example.com", "1000,1000");
        final byte[] connectedFrame = FrameTestUtils.connectedFrame(sessionId, "1000,1000");
        ExternalMessage externalMessage = new ExternalMessage(sessionId, connectFrame);
        serverInboundEmitter.sendAndForget(externalMessage);
        long timerId = vertx.setPeriodic(500, l ->  {
            serverInboundEmitter.sendAndForget(new ExternalMessage(sessionId, Buffer.buffer(FrameParser.EOL).getBytes()));
            LOGGER.info("Sending client heartbeat for session {}", sessionId);
        });
        Awaitility.await().atMost(AWAIT_AT_MOST).pollInterval(AWAIT_POLL_INTERVAL).until(() -> !serverOutboundList.isEmpty()
                && serverOutboundList.peek().sessionId().equals(sessionId));
        ExternalMessage first = serverOutboundList.removeFirst();
        Assertions.assertEquals(sessionId, first.sessionId());
        Assertions.assertArrayEquals(connectedFrame, first.message());
        Awaitility.await().atMost(AWAIT_AT_MOST).pollInterval(AWAIT_POLL_INTERVAL).until(() -> serverOutboundHeartbeats.size() > 1);
        return timerId;
    }

    public void subscribeClient(String sessionId, String subId, String destination, String receipt) {
        final byte [] subscribeFrame = FrameTestUtils.subscribeFrame(subId,  destination, receipt);
        final byte [] receiptFrame = FrameTestUtils.receiptFrame(receipt);
        ExternalMessage externalMessage = new ExternalMessage(sessionId, subscribeFrame);
        serverInboundEmitter.sendAndForget(externalMessage);
        Awaitility.await().atMost(AWAIT_AT_MOST).pollInterval(AWAIT_POLL_INTERVAL).until(() -> !serverOutboundList.isEmpty()
                && serverOutboundList.peek().sessionId().equals(sessionId));
        ExternalMessage first = serverOutboundList.removeFirst();
        Assertions.assertEquals(sessionId, first.sessionId());
        Assertions.assertArrayEquals(receiptFrame, first.message());
    }

    public void unsubscribeClient(String sessionId, String subId, String receipt) {
        final byte [] unsubscribeFrame = FrameTestUtils.unsubscribeFrame(subId, receipt);
        final byte [] receiptFrame = FrameTestUtils.receiptFrame(receipt);
        ExternalMessage externalMessage = new ExternalMessage(sessionId, unsubscribeFrame);
        serverInboundEmitter.sendAndForget(externalMessage);
        Awaitility.await().atMost(AWAIT_AT_MOST).pollInterval(AWAIT_POLL_INTERVAL).until(() -> !serverOutboundList.isEmpty()
                && serverOutboundList.peek().sessionId().equals(sessionId));
        ExternalMessage first = serverOutboundList.removeFirst();
        Assertions.assertEquals(sessionId, first.sessionId());
        Assertions.assertArrayEquals(receiptFrame, first.message());
    }

    public void sendMessage(String sessionId, String destination, String message, String receipt) {
        final byte [] sendFrame = FrameTestUtils.sendFrame(destination, "text/plain", receipt, message);
        final byte [] receiptFrame = FrameTestUtils.receiptFrame(receipt);
        ExternalMessage externalMessage = new ExternalMessage(sessionId, sendFrame);
        serverInboundEmitter.sendAndForget(externalMessage);
        Awaitility.await().atMost(AWAIT_AT_MOST).pollInterval(AWAIT_POLL_INTERVAL).until(() -> !serverOutboundList.isEmpty()
                && serverOutboundList.peek().sessionId().equals(sessionId) && Arrays.equals(receiptFrame, serverOutboundList.peek().message()));
        serverOutboundList.removeFirst();
    }

    public void receiveMessage(String sessionId, String subId, String destination, String message) {
        final byte [] messageFrame = FrameTestUtils.messageFrame(destination, ".*", subId, message);
        Awaitility.await().atMost(AWAIT_AT_MOST).pollInterval(AWAIT_POLL_INTERVAL).until(() -> !serverOutboundList.isEmpty()
                && serverOutboundList.peek().sessionId().equals(sessionId) && Pattern.compile(new String(messageFrame)).matcher(new String(serverOutboundList.peek().message())).matches());
        serverOutboundList.removeFirst();
    }

    public void receiveMessageNot(String sessionId) {
        Awaitility.await().timeout(AWAIT_AT_MOST).until(() -> serverOutboundList.isEmpty()
                || !serverOutboundList.peek().sessionId().equals(sessionId));
    }

    public void receiveFrameNot(String sessionId) {
        Awaitility.await().timeout(AWAIT_AT_MOST).until(() -> (serverOutboundList.isEmpty()
                || !serverOutboundList.peek().sessionId().equals(sessionId)) && (serverOutboundHeartbeats.isEmpty()
                || !serverOutboundHeartbeats.peek().sessionId().equals(sessionId)));
    }

    public void disconnectClient(String sessionId, long timerId, String receipt) {
        final byte [] disconnectFrame = FrameTestUtils.disconnectFrame(receipt);
        final byte [] receiptFrame = FrameTestUtils.receiptFrame(receipt);
        vertx.cancelTimer(timerId);
        ExternalMessage externalMessage = new ExternalMessage(sessionId, disconnectFrame);
        serverInboundEmitter.sendAndForget(externalMessage);
        Awaitility.await().atMost(AWAIT_AT_MOST).pollInterval(Duration.ofMillis(300)).until(() -> !serverOutboundList.isEmpty()
                && serverOutboundList.peek().sessionId().equals(sessionId));
        ExternalMessage first = serverOutboundList.removeFirst();
        Assertions.assertEquals(sessionId, first.sessionId());
        Assertions.assertArrayEquals(receiptFrame, first.message());
    }
}

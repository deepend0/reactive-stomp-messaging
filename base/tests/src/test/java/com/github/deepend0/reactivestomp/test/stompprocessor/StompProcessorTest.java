package com.github.deepend0.reactivestomp.test.stompprocessor;

import com.github.deepend0.reactivestomp.test.FrameTestUtils;
import com.github.deepend0.reactivestomp.websocket.ExternalMessage;
import com.github.deepend0.reactivestomp.messaging.model.*;
import com.github.deepend0.reactivestomp.stompprocessor.MessageIdGenerator;
import io.quarkus.test.InjectMock;
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
import org.junit.jupiter.api.*;
import org.mockito.Mockito;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@QuarkusTest
@TestProfile(StompProcessorTestProfile.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class StompProcessorTest {
    public static final Duration AWAIT_AT_MOST = Duration.ofMillis(3000);;
    public static final Duration AWAIT_POLL_INTERVAL = Duration.ofMillis(100);
    @Inject
    private Vertx vertx;
    @Inject
    @Channel("serverInbound")
    private MutinyEmitter<ExternalMessage> serverInboundEmitter;
    @Inject
    @Channel("serverOutbound")
    private Multi<ExternalMessage> serverOutboundReceiver;
    @Inject
    @Channel("messagingInbound")
    private Multi<Message> messagingInboundReceiver;
    @Inject
    @Channel("brokerOutbound")
    private MutinyEmitter<Message> brokerOutboundEmitter;
    @InjectMock
    private MessageIdGenerator messageIdGenerator;

    private static final String sessionId = "session1";

    private List<ExternalMessage> serverOutboundList = new ArrayList<>();
    private List<ExternalMessage> serverOutboundHeartbeats = new ArrayList<>();
    private List<Message> messagingInboundList = new ArrayList<>();
    private long timerId;

    @BeforeAll
    public void init() {
        serverOutboundReceiver.subscribe().with(externalMessage -> {
            if (Arrays.equals(externalMessage.message(), Buffer.buffer(FrameParser.EOL).getBytes())) {
                serverOutboundHeartbeats.add(externalMessage);
            } else {
                serverOutboundList.add(externalMessage);
            }
        });
        messagingInboundReceiver.subscribe().with(messagingInboundList::add);
    }

    @AfterEach
    public void reset() {
        serverOutboundList.clear();
        messagingInboundList.clear();
    }

    @Test
    @Order(1)
    public void shouldRejectMessagesBeforeConnect() {
        final byte [] subscribeFrame = FrameTestUtils.subscribeFrame("sub-043", "/topic/chat", "12348");
        final byte [] errorFrame = FrameTestUtils.errorFrame("REJECTED", "text/plain", "Active connection doesn't exist.");
        ExternalMessage externalMessage = new ExternalMessage(sessionId, subscribeFrame);
        serverInboundEmitter.sendAndForget(externalMessage);
        Awaitility.await().atMost(AWAIT_AT_MOST).pollInterval(AWAIT_POLL_INTERVAL).until(() -> !serverOutboundList.isEmpty());
        ExternalMessage first = serverOutboundList.getFirst();
        Assertions.assertEquals(sessionId, first.sessionId());
        Assertions.assertArrayEquals(errorFrame, first.message());
    }

    @Test
    @Order(2)
    public void shouldConnectWithConnectedMessage() {
        final byte[] connectFrame = FrameTestUtils.connectFrame("www.example.com", "500,500");
        final byte[] connectedFrame = FrameTestUtils.connectedFrame(sessionId, "1000,1000");
        ExternalMessage externalMessage = new ExternalMessage(sessionId, connectFrame);
        serverInboundEmitter.sendAndForget(externalMessage);
        timerId = vertx.setPeriodic(1000, l -> serverInboundEmitter.sendAndForget(new ExternalMessage(sessionId, "\n".getBytes(StandardCharsets.UTF_8))));
        Awaitility.await().atMost(AWAIT_AT_MOST).pollInterval(AWAIT_POLL_INTERVAL).until(() -> !serverOutboundList.isEmpty());
        ExternalMessage first = serverOutboundList.getFirst();
        Assertions.assertEquals(sessionId, first.sessionId());
        Assertions.assertArrayEquals(connectedFrame, first.message());
        Awaitility.await().atMost(Duration.ofMillis(6000)).pollInterval(AWAIT_POLL_INTERVAL).until(() -> serverOutboundHeartbeats.size() > 2);
    }

    @Test
    @Order(3)
    public void shouldRejectRepeatedConnectMessage() {
        final byte[] connectFrame = FrameTestUtils.connectFrame("www.example.com", "500,500");
        final byte [] errorFrame = FrameTestUtils.errorFrame("REJECTED", "text/plain", "Active connection already exists.");
        ExternalMessage externalMessage = new ExternalMessage(sessionId, connectFrame);
        serverInboundEmitter.sendAndForget(externalMessage);
        Awaitility.await().atMost(AWAIT_AT_MOST).pollInterval(AWAIT_POLL_INTERVAL).until(() -> !serverOutboundList.isEmpty());
        ExternalMessage first = serverOutboundList.getFirst();
        Assertions.assertEquals(sessionId, first.sessionId());
        Assertions.assertArrayEquals(errorFrame, first.message());
    }

    @Test
    @Order(4)
    public void shouldSubscribeWithReceiptAndBrokerSubscription() {
        final byte [] subscribeFrame = FrameTestUtils.subscribeFrame("sub-001", "/topic/chat", "12345");
        final byte [] receiptFrame = FrameTestUtils.receiptFrame("12345");
        ExternalMessage externalMessage = new ExternalMessage(sessionId, subscribeFrame);
        serverInboundEmitter.sendAndForget(externalMessage);
        Awaitility.await().atMost(AWAIT_AT_MOST).pollInterval(AWAIT_POLL_INTERVAL).until(() -> !serverOutboundList.isEmpty());
        ExternalMessage first = serverOutboundList.getFirst();
        Assertions.assertEquals(sessionId, first.sessionId());
        Assertions.assertArrayEquals(receiptFrame, first.message());
        Awaitility.await().atMost(AWAIT_AT_MOST).pollInterval(AWAIT_POLL_INTERVAL).until(() -> !messagingInboundList.isEmpty());
        Message message = messagingInboundList.getFirst();
        Assertions.assertEquals(sessionId, message.getSubscriberId());
        Assertions.assertInstanceOf(SubscribeMessage.class, message);
        Assertions.assertEquals("/topic/chat", ((SubscribeMessage) message).getDestination());
    }

    @Test
    @Order(5)
    public void shouldSendMessageToBrokerWithReceipt() {
        final byte [] sendFrame = FrameTestUtils.sendFrame("/queue/messages", "text/plain", "12346", "Hello, this is a dummy message!");
        final byte [] receiptFrame = FrameTestUtils.receiptFrame("12346");
        ExternalMessage externalMessage = new ExternalMessage(sessionId, sendFrame);
        serverInboundEmitter.sendAndForget(externalMessage);
        Awaitility.await().atMost(AWAIT_AT_MOST).pollInterval(AWAIT_POLL_INTERVAL).until(() -> !serverOutboundList.isEmpty());
        ExternalMessage first = serverOutboundList.getFirst();
        Assertions.assertEquals(sessionId, first.sessionId());
        Assertions.assertArrayEquals(receiptFrame, first.message());
        Awaitility.await().atMost(AWAIT_AT_MOST).pollInterval(AWAIT_POLL_INTERVAL).until(() -> !messagingInboundList.isEmpty());
        Message message = messagingInboundList.getFirst();
        Assertions.assertEquals(sessionId, message.getSubscriberId());
        Assertions.assertInstanceOf(SendMessage.class, message);
        Assertions.assertEquals("/queue/messages", ((SendMessage) message).getDestination());
        Assertions.assertArrayEquals("Hello, this is a dummy message!".getBytes(StandardCharsets.UTF_8), ((SendMessage) message).getPayload());
    }

    @Test
    @Order(6)
    public void shouldNotResendMessageWhenAckReceivedInTime() throws InterruptedException {
        final byte[] subscribeFrame = FrameTestUtils.subscribeWithAckFrame("sub-ack", "/topic/chat2", "client", "r-ack");
        serverInboundEmitter.sendAndForget(new ExternalMessage(sessionId, subscribeFrame));
        Awaitility.await().atMost(AWAIT_AT_MOST).pollInterval(AWAIT_POLL_INTERVAL)
                .until(() -> !messagingInboundList.isEmpty());
        messagingInboundList.clear();
        serverOutboundList.clear();

        Mockito.when(messageIdGenerator.generate()).thenReturn("ack-msg-1");
        final byte[] expectedMessageFrame = FrameTestUtils.messageFrameWithAck("/topic/chat2", "ack-msg-1", "sub-ack", "ack-msg-1", "ACK success payload");
        brokerOutboundEmitter.sendAndForget(new SendMessage(sessionId, "/topic/chat2", "ACK success payload".getBytes(StandardCharsets.UTF_8)));

        Awaitility.await().atMost(AWAIT_AT_MOST).pollInterval(AWAIT_POLL_INTERVAL).until(() -> serverOutboundList.size() == 1);
        Assertions.assertArrayEquals(expectedMessageFrame, serverOutboundList.get(0).message());

        final byte[] ackFrame = FrameTestUtils.ackFrame("ack-msg-1");
        serverInboundEmitter.sendAndForget(new ExternalMessage(sessionId, ackFrame));

        Thread.sleep(2000);
        Assertions.assertEquals(1, serverOutboundList.size(),
                "Message should only be sent once when ACK is received in time");
    }

    @Test
    @Order(7)
    public void shouldResendMessageOnceIfNotAckedInTime() throws InterruptedException {
        // Step 1: Subscribe first so broker can send messages
        final byte[] subscribeFrame = FrameTestUtils.subscribeWithAckFrame("sub-ack2", "/topic/chat3",  "client", "r-ack2");
        serverInboundEmitter.sendAndForget(new ExternalMessage(sessionId, subscribeFrame));
        Awaitility.await().atMost(AWAIT_AT_MOST).pollInterval(AWAIT_POLL_INTERVAL).until(() -> !messagingInboundList.isEmpty());
        messagingInboundList.clear();
        serverOutboundList.clear();

        Mockito.when(messageIdGenerator.generate()).thenReturn("ack-msg-2");
        final byte[] expectedMessageFrame = FrameTestUtils.messageFrameWithAck("/topic/chat3", "ack-msg-2", "sub-ack2", "ack-msg-2","ACK test payload");
        brokerOutboundEmitter.sendAndForget(new SendMessage(sessionId, "/topic/chat3", "ACK test payload".getBytes(StandardCharsets.UTF_8)));

        Awaitility.await().atMost(AWAIT_AT_MOST).pollInterval(AWAIT_POLL_INTERVAL).until(() -> !serverOutboundList.isEmpty());
        Assertions.assertArrayEquals(expectedMessageFrame, serverOutboundList.get(0).message());

        // Step 4: Wait slightly over 5 seconds to allow resend trigger
        Awaitility.await().atMost(Duration.ofMillis(2000)).pollInterval(AWAIT_POLL_INTERVAL).until(() -> serverOutboundList.size() == 2);
        Assertions.assertArrayEquals(expectedMessageFrame, serverOutboundList.get(1).message(),
                "Resent frame should be identical to the first one");

        // Step 5: Send ACK for message-id
        final byte[] ackFrame = FrameTestUtils.ackFrame("ack-msg-2");
        serverInboundEmitter.sendAndForget(new ExternalMessage(sessionId, ackFrame));

        // Step 6: Ensure no third resend happens after ACK
        Thread.sleep(2000); // wait to check no further sends
        Assertions.assertEquals(2, serverOutboundList.size(), "Message should be sent exactly twice");
    }

    @Test
    @Order(8)
    public void shouldSendMessageFromBroker() {
        final byte [] messageFrame = FrameTestUtils.messageFrame("/topic/chat", "abcde", "sub-001", "Hello, this is a dummy message!");
        Mockito.when(messageIdGenerator.generate()).thenReturn("abcde");
        SendMessage sendMessage = new SendMessage(sessionId, "/topic/chat", "Hello, this is a dummy message!".getBytes(StandardCharsets.UTF_8));
        brokerOutboundEmitter.sendAndForget(sendMessage);
        Awaitility.await().atMost(AWAIT_AT_MOST).pollInterval(AWAIT_POLL_INTERVAL).until(() -> !serverOutboundList.isEmpty());
        ExternalMessage first = serverOutboundList.getFirst();
        Assertions.assertEquals(sessionId, first.sessionId());
        Assertions.assertArrayEquals(messageFrame, first.message());
    }

    @Test
    @Order(9)
    public void shouldUnsubscribeWithReceiptAndBrokerSubscription() {
        final byte [] unsubscribeFrame = FrameTestUtils.unsubscribeFrame("sub-001", "54321");
        final byte [] receiptFrame = FrameTestUtils.receiptFrame("54321");
        ExternalMessage externalMessage = new ExternalMessage(sessionId, unsubscribeFrame);
        serverInboundEmitter.sendAndForget(externalMessage);
        Awaitility.await().atMost(AWAIT_AT_MOST).pollInterval(AWAIT_POLL_INTERVAL).until(() -> !serverOutboundList.isEmpty());
        ExternalMessage first = serverOutboundList.getFirst();
        Assertions.assertEquals(sessionId, first.sessionId());
        Assertions.assertArrayEquals(receiptFrame, first.message());
        Awaitility.await().atMost(AWAIT_AT_MOST).pollInterval(AWAIT_POLL_INTERVAL).until(() -> !messagingInboundList.isEmpty());
        Message message = messagingInboundList.getFirst();
        Assertions.assertEquals(sessionId, message.getSubscriberId());
        Assertions.assertInstanceOf(UnsubscribeMessage.class, message);
        Assertions.assertEquals("/topic/chat", ((UnsubscribeMessage) message).getDestination());
        Awaitility.await().atMost(AWAIT_AT_MOST).pollInterval(AWAIT_POLL_INTERVAL).until(() -> messagingInboundList.getFirst() instanceof UnsubscribeMessage);
    }

    @Test
    @Order(10)
    public void shouldDisconnectWithReceiptAndBrokerDisconnect() {
        final byte [] disconnectFrame = FrameTestUtils.disconnectFrame("12347");
        final byte [] receiptFrame = FrameTestUtils.receiptFrame("12347");
        ExternalMessage externalMessage = new ExternalMessage(sessionId, disconnectFrame);
        serverInboundEmitter.sendAndForget(externalMessage);
        Awaitility.await().atMost(AWAIT_AT_MOST).pollInterval(AWAIT_POLL_INTERVAL).until(() -> !serverOutboundList.isEmpty());
        ExternalMessage first = serverOutboundList.getFirst();
        Assertions.assertEquals(sessionId, first.sessionId());
        Assertions.assertArrayEquals(receiptFrame, first.message());
        Awaitility.await().atMost(AWAIT_AT_MOST).pollInterval(AWAIT_POLL_INTERVAL).until(() -> !messagingInboundList.isEmpty());
        Message message = messagingInboundList.getFirst();
        Assertions.assertEquals(sessionId, message.getSubscriberId());
        Assertions.assertInstanceOf(DisconnectMessage.class, message);
        vertx.cancelTimer(timerId);
    }

    @Test
    @Order(11)
    public void shouldRejectMessagesAfterDisconnect() {
        final byte [] subscribeFrame = FrameTestUtils.subscribeFrame("sub-05", "/topic/chat", "12348");
        final byte [] errorFrame = FrameTestUtils.errorFrame("REJECTED", "text/plain", "Active connection doesn't exist.");
        ExternalMessage externalMessage = new ExternalMessage(sessionId, subscribeFrame);
        serverInboundEmitter.sendAndForget(externalMessage);
        Awaitility.await().atMost(AWAIT_AT_MOST).pollInterval(AWAIT_POLL_INTERVAL).until(() -> !serverOutboundList.isEmpty());
        ExternalMessage first = serverOutboundList.getFirst();
        Assertions.assertEquals(sessionId, first.sessionId());
        Assertions.assertArrayEquals(errorFrame, first.message());
    }
}

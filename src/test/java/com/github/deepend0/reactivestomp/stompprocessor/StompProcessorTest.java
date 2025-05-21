package com.github.deepend0.reactivestomp.stompprocessor;

import com.github.deepend0.reactivestomp.external.ExternalMessage;
import com.github.deepend0.reactivestomp.simplebroker.messagehandler.BrokerMessage;
import com.github.deepend0.reactivestomp.simplebroker.messagehandler.DisconnectMessage;
import com.github.deepend0.reactivestomp.simplebroker.messagehandler.SendMessage;
import com.github.deepend0.reactivestomp.simplebroker.messagehandler.SubscribeMessage;
import com.github.deepend0.reactivestomp.simplebroker.messagehandler.UnsubscribeMessage;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
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
    @Inject
    private Vertx vertx;
    @Inject
    @Channel("serverInbound")
    private MutinyEmitter<ExternalMessage> serverInboundEmitter;
    @Inject
    @Channel("serverOutbound")
    private Multi<ExternalMessage> serverOutboundReceiver;
    @Inject
    @Channel("brokerInbound")
    private Multi<BrokerMessage> brokerInboundReceiver;
    @Inject
    @Channel("brokerOutbound")
    private MutinyEmitter<BrokerMessage> brokerOutboundEmitter;
    @InjectMock
    private MessageIdGenerator messageIdGenerator;
    @Inject
    private StompProcessor stompProcessor;

    private static final String sessionId = "session1";

    private List<ExternalMessage> serverOutboundList = new ArrayList<>();
    private List<ExternalMessage> serverOutboundHeartbeats = new ArrayList<>();
    private List<BrokerMessage> brokerInboundList = new ArrayList<>();
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
        brokerInboundReceiver.subscribe().with(brokerInboundList::add);
    }

    @AfterEach
    public void reset() {
        serverOutboundList.clear();
        brokerInboundList.clear();
    }

    @Test
    @Order(1)
    public void shouldConnectWithConnectedMessage() {
        final byte[] connectFrame = StompFrameUtils.connectFrame("www.example.com", "500,500");
        final byte[] connectedFrame = StompFrameUtils.connectedFrame(sessionId, "1000,1000");
        ExternalMessage externalMessage = new ExternalMessage(sessionId, connectFrame);
        serverInboundEmitter.sendAndForget(externalMessage);
        timerId = vertx.setPeriodic(1000, l -> serverInboundEmitter.sendAndForget(new ExternalMessage(sessionId, "\n".getBytes(StandardCharsets.UTF_8))));
        Awaitility.await().atMost(Duration.ofMillis(5000)).pollInterval(Duration.ofMillis(1000)).until(() -> !serverOutboundList.isEmpty());
        ExternalMessage first = serverOutboundList.getFirst();
        Assertions.assertEquals(sessionId, first.sessionId());
        Assertions.assertArrayEquals(connectedFrame, first.message());
        Awaitility.await().atMost(Duration.ofMillis(6000)).pollInterval(Duration.ofMillis(1000)).until(() -> serverOutboundHeartbeats.size() > 2);
    }

    @Test
    @Order(2)
    public void shouldSubscribeWithReceiptAndBrokerSubscription() {
        final byte [] subscribeFrame = StompFrameUtils.subscribeFrame("sub-001", "/topic/chat", "12345");
        final byte [] receiptFrame = StompFrameUtils.receiptFrame("12345");
        ExternalMessage externalMessage = new ExternalMessage(sessionId, subscribeFrame);
        serverInboundEmitter.sendAndForget(externalMessage);
        Awaitility.await().atMost(Duration.ofMillis(5000)).pollInterval(Duration.ofMillis(1000)).until(() -> !serverOutboundList.isEmpty());
        ExternalMessage first = serverOutboundList.getFirst();
        Assertions.assertEquals(sessionId, first.sessionId());
        Assertions.assertArrayEquals(receiptFrame, first.message());
        Awaitility.await().atMost(Duration.ofMillis(5000)).pollInterval(Duration.ofMillis(1000)).until(() -> !brokerInboundList.isEmpty());
        BrokerMessage brokerMessage = brokerInboundList.getFirst();
        Assertions.assertEquals(sessionId, brokerMessage.getSubscriberId());
        Assertions.assertInstanceOf(SubscribeMessage.class, brokerMessage);
        Assertions.assertEquals("/topic/chat", ((SubscribeMessage) brokerMessage).getDestination());
    }

    @Test
    @Order(3)
    public void shouldSendMessageToBrokerWithReceipt() {
        final byte [] sendFrame = StompFrameUtils.sendFrame("/queue/messages", "text/plain", "12346", "Hello, this is a dummy message!");
        final byte [] receiptFrame = StompFrameUtils.receiptFrame("12346");
        ExternalMessage externalMessage = new ExternalMessage(sessionId, sendFrame);
        serverInboundEmitter.sendAndForget(externalMessage);
        Awaitility.await().atMost(Duration.ofMillis(5000)).pollInterval(Duration.ofMillis(1000)).until(() -> !serverOutboundList.isEmpty());
        ExternalMessage first = serverOutboundList.getFirst();
        Assertions.assertEquals(sessionId, first.sessionId());
        Assertions.assertArrayEquals(receiptFrame, first.message());
        Awaitility.await().atMost(Duration.ofMillis(5000)).pollInterval(Duration.ofMillis(1000)).until(() -> !brokerInboundList.isEmpty());
        BrokerMessage brokerMessage = brokerInboundList.getFirst();
        Assertions.assertEquals(sessionId, brokerMessage.getSubscriberId());
        Assertions.assertInstanceOf(SendMessage.class, brokerMessage);
        Assertions.assertEquals("/queue/messages", ((SendMessage) brokerMessage).getDestination());
        Assertions.assertArrayEquals("Hello, this is a dummy message!\n".getBytes(StandardCharsets.UTF_8), ((SendMessage) brokerMessage).getPayload());
    }

    @Test
    @Order(4)
    public void shouldSendMessageFromBroker() {
        final byte [] messageFrame = StompFrameUtils.messageFrame("/topic/chat", "abcde", "sub-001", "Hello, this is a dummy message!");
        Mockito.when(messageIdGenerator.generate()).thenReturn("abcde");
        SendMessage sendMessage = new SendMessage(sessionId, "/topic/chat", "Hello, this is a dummy message!".getBytes(StandardCharsets.UTF_8));
        brokerOutboundEmitter.sendAndForget(sendMessage);
        Awaitility.await().atMost(Duration.ofMillis(5000)).pollInterval(Duration.ofMillis(1000)).until(() -> !serverOutboundList.isEmpty());
        ExternalMessage first = serverOutboundList.getFirst();
        Assertions.assertEquals(sessionId, first.sessionId());
        Assertions.assertArrayEquals(messageFrame, first.message());
    }

    @Test
    @Order(5)
    public void shouldUnsubscribeWithReceiptAndBrokerSubscription() {
        final byte [] unsubscribeFrame = StompFrameUtils.unsubscribeFrame("sub-001", "54321");
        final byte [] receiptFrame = StompFrameUtils.receiptFrame("54321");
        ExternalMessage externalMessage = new ExternalMessage(sessionId, unsubscribeFrame);
        serverInboundEmitter.sendAndForget(externalMessage);
        Awaitility.await().atMost(Duration.ofMillis(5000)).pollInterval(Duration.ofMillis(1000)).until(() -> !serverOutboundList.isEmpty());
        ExternalMessage first = serverOutboundList.getFirst();
        Assertions.assertEquals(sessionId, first.sessionId());
        Assertions.assertArrayEquals(receiptFrame, first.message());
        Awaitility.await().atMost(Duration.ofMillis(5000)).pollInterval(Duration.ofMillis(1000)).until(() -> !brokerInboundList.isEmpty());
        BrokerMessage brokerMessage = brokerInboundList.getFirst();
        Assertions.assertEquals(sessionId, brokerMessage.getSubscriberId());
        Assertions.assertInstanceOf(UnsubscribeMessage.class, brokerMessage);
        Assertions.assertEquals("/topic/chat", ((UnsubscribeMessage) brokerMessage).getDestination());
        Awaitility.await().atMost(Duration.ofMillis(5000)).pollInterval(Duration.ofMillis(1000)).until(() -> brokerInboundList.getFirst() instanceof UnsubscribeMessage);
    }

    @Test
    @Order(6)
    public void shouldDisconnectWithReceiptAndBrokerDisconnect() {
        final byte [] disconnectFrame = StompFrameUtils.disconnectFrame("12347");
        final byte [] receiptFrame = StompFrameUtils.receiptFrame("12347");
        ExternalMessage externalMessage = new ExternalMessage(sessionId, disconnectFrame);
        serverInboundEmitter.sendAndForget(externalMessage);
        Awaitility.await().atMost(Duration.ofMillis(5000)).pollInterval(Duration.ofMillis(1000)).until(() -> !serverOutboundList.isEmpty());
        ExternalMessage first = serverOutboundList.getFirst();
        Assertions.assertEquals(sessionId, first.sessionId());
        Assertions.assertArrayEquals(receiptFrame, first.message());
        Awaitility.await().atMost(Duration.ofMillis(5000)).pollInterval(Duration.ofMillis(1000)).until(() -> !brokerInboundList.isEmpty());
        BrokerMessage brokerMessage = brokerInboundList.getFirst();
        Assertions.assertEquals(sessionId, brokerMessage.getSubscriberId());
        Assertions.assertInstanceOf(DisconnectMessage.class, brokerMessage);
        vertx.cancelTimer(timerId);
    }
}

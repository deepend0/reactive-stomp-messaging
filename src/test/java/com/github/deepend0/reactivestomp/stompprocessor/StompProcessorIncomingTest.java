package com.github.deepend0.reactivestomp.stompprocessor;

import com.github.deepend0.reactivestomp.message.ExternalMessage;
import com.github.deepend0.reactivestomp.simplebroker.model.*;
import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.MutinyEmitter;
import io.vertx.core.Vertx;
import jakarta.inject.Inject;
import org.apache.commons.lang3.function.Consumers;
import org.awaitility.Awaitility;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.junit.jupiter.api.*;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@QuarkusTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class StompProcessorIncomingTest {
    @Inject
    private Vertx vertx;
    @Inject
    @Channel("serverInbound")
    private MutinyEmitter<ExternalMessage> serverInboundEmitter;
    @Inject
    @Channel("serverInboundStatus")
    private Multi<Void> serverInboundStatusReceiver;
    @Inject
    @Channel("serverOutbound")
    private Multi<ExternalMessage> serverOutboundReceiver;
    @Inject
    @Channel("brokerInbound")
    private Multi<BrokerMessage> brokerInboundReceiver;

    private static final String sessionId = "session1";

    private static final byte[] CONNECT_MESSAGE = ("""
            CONNECT
            accept-version:1.2
            host:example.com
            heart-beat:500,500
            
            \u0000"""
    ).getBytes(StandardCharsets.UTF_8);

    private static final byte[] CONNECTED_MESSAGE = ("""
            CONNECTED
            server:vertx-stomp/4.5.13
            heart-beat:1000,1000
            session:session1
            version:1.2
            
            \u0000"""
    ).getBytes(StandardCharsets.UTF_8);

    private static final byte[] SUBSCRIBE_FRAME = ("""
            SUBSCRIBE
            id:sub-001
            destination:/topic/chat
            receipt:12345
            
            \u0000"""
    ).getBytes(StandardCharsets.UTF_8);

    private static final byte[] SUBSCRIBE_RECEIPT_FRAME = ("""
            RECEIPT
            receipt-id:12345
            
            \u0000"""
    ).getBytes(StandardCharsets.UTF_8);

    private static final byte[] UNSUBSCRIBE_FRAME = ("""
            UNSUBSCRIBE
            id:sub-001
            receipt:54321
            
            \u0000"""
    ).getBytes(StandardCharsets.UTF_8);

    private static final byte[] UNSUBSCRIBE_RECEIPT_FRAME = ("""
            RECEIPT
            receipt-id:54321
            
            \u0000"""
    ).getBytes(StandardCharsets.UTF_8);

    private static final byte[] SEND_FRAME = ("""
            SEND
            destination:/queue/messages
            content-type:text/plain
            receipt:12346
            
            Hello, this is a dummy message!
            \u0000"""
    ).getBytes(StandardCharsets.UTF_8);

    private static final byte[] SEND_RECEIPT_FRAME = ("""
            RECEIPT
            receipt-id:12346
            
            \u0000"""
    ).getBytes(StandardCharsets.UTF_8);

    private static final byte[] DISCONNECT_FRAME = ("""
            DISCONNECT
            receipt:12347
            
            \u0000"""
    ).getBytes(StandardCharsets.UTF_8);

    private static final byte[] DISCONNECT_RECEIPT_FRAME = ("""
            RECEIPT
            receipt-id:12347
            
            \u0000"""
    ).getBytes(StandardCharsets.UTF_8);

    private List<ExternalMessage> serverOutboundList = new ArrayList<>();
    private List<ExternalMessage> serverOutboundHeartbeats = new ArrayList<>();
    private List<BrokerMessage> brokerInboundList = new ArrayList<>();
    private long timerId;

    @BeforeAll
    public void init() {
        serverOutboundReceiver.subscribe().with(externalMessage -> {
            if (Arrays.equals(externalMessage.message(), "\n".getBytes(StandardCharsets.UTF_8))) {
                serverOutboundHeartbeats.add(externalMessage);
            } else {
                serverOutboundList.add(externalMessage);
            }
        });
        serverInboundStatusReceiver.subscribe().with(Consumers.nop());
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
        ExternalMessage externalMessage = new ExternalMessage(sessionId, CONNECT_MESSAGE);
        serverInboundEmitter.sendAndForget(externalMessage);
        timerId = vertx.setPeriodic(1000, l -> serverInboundEmitter.sendAndForget(new ExternalMessage(sessionId, "\n".getBytes(StandardCharsets.UTF_8))));
        Awaitility.await().atMost(Duration.ofMillis(5000)).pollInterval(Duration.ofMillis(1000)).until(() -> !serverOutboundList.isEmpty());
        ExternalMessage first = serverOutboundList.getFirst();
        Assertions.assertEquals(sessionId, first.sessionId());
        Assertions.assertArrayEquals(CONNECTED_MESSAGE, first.message());
        Awaitility.await().atMost(Duration.ofMillis(6000)).pollInterval(Duration.ofMillis(1000)).until(() -> serverOutboundHeartbeats.size() > 2);
    }

    @Test
    @Order(2)
    public void shouldSubscribeWithReceiptAndBrokerSubscription() {
        ExternalMessage externalMessage = new ExternalMessage(sessionId, SUBSCRIBE_FRAME);
        serverInboundEmitter.sendAndForget(externalMessage);
        Awaitility.await().atMost(Duration.ofMillis(5000)).pollInterval(Duration.ofMillis(1000)).until(() -> !serverOutboundList.isEmpty());
        ExternalMessage first = serverOutboundList.getFirst();
        Assertions.assertEquals(sessionId, first.sessionId());
        Assertions.assertArrayEquals(SUBSCRIBE_RECEIPT_FRAME, first.message());
        Awaitility.await().atMost(Duration.ofMillis(5000)).pollInterval(Duration.ofMillis(1000)).until(() -> !brokerInboundList.isEmpty());
        BrokerMessage brokerMessage = brokerInboundList.getFirst();
        Assertions.assertEquals(sessionId, brokerMessage.getSubscriberId());
        Assertions.assertInstanceOf(SubscribeMessage.class, brokerMessage);
        Assertions.assertEquals("/topic/chat", ((SubscribeMessage) brokerMessage).getDestination());
    }

    @Test
    @Order(3)
    public void shouldSendWithReceiptAndBrokerMessage() {
        ExternalMessage externalMessage = new ExternalMessage(sessionId, SEND_FRAME);
        serverInboundEmitter.sendAndForget(externalMessage);
        Awaitility.await().atMost(Duration.ofMillis(5000)).pollInterval(Duration.ofMillis(1000)).until(() -> !serverOutboundList.isEmpty());
        ExternalMessage first = serverOutboundList.getFirst();
        Assertions.assertEquals(sessionId, first.sessionId());
        Assertions.assertArrayEquals(SEND_RECEIPT_FRAME, first.message());
        Awaitility.await().atMost(Duration.ofMillis(5000)).pollInterval(Duration.ofMillis(1000)).until(() -> !brokerInboundList.isEmpty());
        BrokerMessage brokerMessage = brokerInboundList.getFirst();
        Assertions.assertEquals(sessionId, brokerMessage.getSubscriberId());
        Assertions.assertInstanceOf(SendMessage.class, brokerMessage);
        Assertions.assertEquals("/queue/messages", ((SendMessage) brokerMessage).getDestination());
        Assertions.assertArrayEquals("Hello, this is a dummy message!\n".getBytes(StandardCharsets.UTF_8), ((SendMessage) brokerMessage).getPayload());
    }

    @Test
    @Order(4)
    public void shouldUnsubscribeWithReceiptAndBrokerSubscription() {
        ExternalMessage externalMessage = new ExternalMessage(sessionId, UNSUBSCRIBE_FRAME);
        serverInboundEmitter.sendAndForget(externalMessage);
        Awaitility.await().atMost(Duration.ofMillis(5000)).pollInterval(Duration.ofMillis(1000)).until(() -> !serverOutboundList.isEmpty());
        ExternalMessage first = serverOutboundList.getFirst();
        Assertions.assertEquals(sessionId, first.sessionId());
        Assertions.assertArrayEquals(UNSUBSCRIBE_RECEIPT_FRAME, first.message());
        Awaitility.await().atMost(Duration.ofMillis(5000)).pollInterval(Duration.ofMillis(1000)).until(() -> !brokerInboundList.isEmpty());
        BrokerMessage brokerMessage = brokerInboundList.getFirst();
        Assertions.assertEquals(sessionId, brokerMessage.getSubscriberId());
        Assertions.assertInstanceOf(UnsubscribeMessage.class, brokerMessage);
        Assertions.assertEquals("/topic/chat", ((UnsubscribeMessage) brokerMessage).getDestination());
        Awaitility.await().atMost(Duration.ofMillis(5000)).pollInterval(Duration.ofMillis(1000)).until(() -> brokerInboundList.getFirst() instanceof UnsubscribeMessage);
    }

    @Test
    @Order(5)
    public void shouldDisconnectWithReceiptAndBrokerDisconnect() {
        ExternalMessage externalMessage = new ExternalMessage(sessionId, DISCONNECT_FRAME);
        serverInboundEmitter.sendAndForget(externalMessage);
        Awaitility.await().atMost(Duration.ofMillis(5000)).pollInterval(Duration.ofMillis(1000)).until(() -> !serverOutboundList.isEmpty());
        ExternalMessage first = serverOutboundList.getFirst();
        Assertions.assertEquals(sessionId, first.sessionId());
        Assertions.assertArrayEquals(DISCONNECT_RECEIPT_FRAME, first.message());
        Awaitility.await().atMost(Duration.ofMillis(5000)).pollInterval(Duration.ofMillis(1000)).until(() -> !brokerInboundList.isEmpty());
        BrokerMessage brokerMessage = brokerInboundList.getFirst();
        Assertions.assertEquals(sessionId, brokerMessage.getSubscriberId());
        Assertions.assertInstanceOf(DisconnectMessage.class, brokerMessage);
        vertx.cancelTimer(timerId);
    }
}

package com.github.deepend0.reactivestomp.stompprocessor;

import com.github.deepend0.reactivestomp.messaging.model.DisconnectMessage;
import com.github.deepend0.reactivestomp.messaging.model.Message;
import com.github.deepend0.reactivestomp.websocket.ExternalMessage;
import io.quarkus.vertx.ConsumeEvent;
import io.smallrye.mutiny.tuples.Tuple2;
import io.smallrye.mutiny.tuples.Tuples;
import io.smallrye.reactive.messaging.MutinyEmitter;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.stomp.impl.FrameParser;
import jakarta.enterprise.context.ApplicationScoped;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class StompRegistry {
    private static final Logger LOGGER = LoggerFactory.getLogger(StompRegistry.class);
    private final Vertx vertx;

    protected final MutinyEmitter<ExternalMessage> serverOutboundEmitter;

    private final MutinyEmitter<Message> messagingInboundEmitter;

    private final ConcurrentHashMap<String, Long> lastClientActivities = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Tuple2<Long, Integer>> sessionPingTimerIds = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Tuple2<Long, Integer>> sessionPongTimerIds = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Tuple2<String, String>, SessionSubscription> sessionSubscriptionsBySubscription = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Tuple2<String, String>, SessionSubscription> sessionSubscriptionsByDestination = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Tuple2<String, String>> ackIdSubids = new ConcurrentHashMap<>();

    private long ackPeriod;

    public StompRegistry(Vertx vertx,
                         @Channel("serverOutbound") MutinyEmitter<ExternalMessage> serverOutboundEmitter,
                         @Channel("messagingInbound") MutinyEmitter<Message> messagingInboundEmitter,
                         @ConfigProperty(name = "reactive-stomp.stomp.ackPeriod") Long ackPeriod
    ) {
        this.vertx = vertx;
        this.serverOutboundEmitter = serverOutboundEmitter;
        this.messagingInboundEmitter = messagingInboundEmitter;
        this.ackPeriod = ackPeriod;
    }
    
    @ConsumeEvent(value = StompProcessor.CONNECT_EVENT_DESTINATION)
    public void handleConnectEvent(ConnectMessage connectMessage) {
        handleHeartbeat(connectMessage.sessionId(), connectMessage.ping(), connectMessage.pong());
    }

    @ConsumeEvent(value = StompProcessor.DISCONNECT_EVENT_DESTINATION)
    public void handleDisconnectEvent(String sessionId) {
        disconnect(sessionId);
    }

    @ConsumeEvent(value = StompProcessor.SUBSCRIBE_EVENT_DESTINATION)
    public void handleSubscribeEvent(SessionSubscription sessionSubscription) {
        addSessionSubscription(sessionSubscription);
    }
    
    @ConsumeEvent(value = StompProcessor.UNSUBSCRIBE_EVENT_DESTINATION)
    public void handleUnsubscribeEvent(SessionSubscription sessionSubscription) {
        deleteSessionSubscription(sessionSubscription);
    }

    @ConsumeEvent(value = StompProcessor.MESSAGE_SEND_DESTINATION)
    public void handleMessageSendEvent (SendMessage sendMessage){
        SessionSubscription sessionSubscription = getSessionSubscriptionBySubscription(sendMessage.sessionId, sendMessage.subscriptionId);
        if(sessionSubscription.isAckRequired()) {
            sessionSubscription.externalMessagesAdd(new ExternalMessageWithDate(sendMessage.externalMessage, sendMessage.ackId, LocalDateTime.now()));
            ackIdSubids.put(sendMessage.ackId, Tuple2.of(sendMessage.sessionId, sendMessage.subscriptionId));
        }
    }

    @ConsumeEvent(value = StompProcessor.MESSAGE_ACK_DESTINATION)
    public void handleAckEvent (AckMessage ackMessage){
        if(ackMessage.acked) {
            var key = ackIdSubids.get(ackMessage.ackId);
            if(key != null) {
                SessionSubscription sessionSubscription = sessionSubscriptionsBySubscription.get(key);
                if (sessionSubscription.ack().equals("client")) {
                    if (!sessionSubscription.externalMessages.isEmpty() && sessionSubscription.externalMessages.stream().anyMatch(externalMessageWithDate -> ackMessage.ackId().equals(externalMessageWithDate.ackId))) {
                        while (sessionSubscription.externalMessages.peek() != null) {
                            ExternalMessageWithDate externalMessageWithDate = sessionSubscription.externalMessages.remove();
                            if (ackMessage.ackId().equals(externalMessageWithDate.ackId)) {
                                break;
                            }
                        }
                    }
                } else if (sessionSubscription.ack().equals("client-individual")) {
                    Optional<ExternalMessageWithDate> foundExternalMessage = sessionSubscription.externalMessages.stream().filter(externalMessageWithDate -> ackMessage.ackId.equals(externalMessageWithDate.ackId)).findFirst();
                    foundExternalMessage.ifPresent(sessionSubscription.externalMessages::remove);
                }
            }
        }
        ackIdSubids.remove(ackMessage.ackId);
    }

    public void handleHeartbeat(String sessionId, int ping, int pong) {
        if (ping > 0) {
            long timerId = vertx.setPeriodic(ping, l -> {
                serverOutboundEmitter.sendAndForget(new ExternalMessage(sessionId, Buffer.buffer(FrameParser.EOL).getBytes()));
                LOGGER.debug("Sending server heartbeat for session {}", sessionId);
            });
            sessionPingTimerIds.put(sessionId, Tuple2.of(timerId, ping));
        }
        if (pong > 0) {
            long timerId = vertx.setPeriodic(pong, l -> {
                long lastClientActivity = lastClientActivities.get(sessionId);
                LOGGER.debug("Last client activity : {} for session: {}", lastClientActivity, sessionId);
                long delta = Instant.now().toEpochMilli() - lastClientActivity;
                if (delta > pong * 2) {
                    LOGGER.warn("Disconnecting client " + sessionId + " - no client activity in the last " + delta + " ms");
                    serverOutboundEmitter.sendAndForget(new ExternalMessage(sessionId, new byte[]{'\0'}));
                    messagingInboundEmitter.sendAndForget(new DisconnectMessage(sessionId));
                    disconnect(sessionId);
                }
            });
            sessionPongTimerIds.put(sessionId, Tuple2.of(timerId, pong));
        }
    }

    public boolean hasActiveSession(String sessionId) {
        return lastClientActivities.containsKey(sessionId);
    }

    public void updateLastActivity(String sessionId) {
        lastClientActivities.put(sessionId, Instant.now().toEpochMilli());
    }

    public void disconnect(String sessionId) {
        Tuple2<Long, Integer> pingTimer = sessionPingTimerIds.get(sessionId);
        Tuple2<Long, Integer> pongTimer = sessionPongTimerIds.get(sessionId);
        assert vertx.cancelTimer(pingTimer.getItem1());
        assert vertx.cancelTimer(pongTimer.getItem1());
        sessionPingTimerIds.remove(sessionId);
        sessionPongTimerIds.remove(sessionId);
        lastClientActivities.remove(sessionId);

        var sessionIdSubIdKeys = sessionSubscriptionsBySubscription.entrySet().stream().filter(e -> sessionId.equals(e.getKey().getItem1())).map(Map.Entry::getValue).toList();
        sessionIdSubIdKeys.forEach(this::deleteSessionSubscription);
    }

    public void addSessionSubscription(SessionSubscription sessionSubscription) {
        sessionSubscriptionsBySubscription.put(Tuples.tuple2(List.of(sessionSubscription.sessionId(), sessionSubscription.subscriptionId())), sessionSubscription);
        sessionSubscriptionsByDestination.put(Tuples.tuple2(List.of(sessionSubscription.sessionId(), sessionSubscription.destination())), sessionSubscription);

        if(sessionSubscription.isAckRequired()) {
            long ackTimerId = vertx.setPeriodic(ackPeriod, l -> {
                LOGGER.info("Num messages waiting for ack " + sessionSubscription.externalMessages.size());

                while (!sessionSubscription.externalMessages.isEmpty()) {

                    LOGGER.info("Message To Be Acked Sent Time: " + sessionSubscription.externalMessages.peek().sentTime + " Due time " + LocalDateTime.now().minus(ackPeriod, ChronoUnit.MILLIS));
                    if(!sessionSubscription.externalMessages.peek().sentTime.isBefore(LocalDateTime.now().minus(ackPeriod, ChronoUnit.MILLIS))) {
                        ExternalMessageWithDate externalMessageWithDate = sessionSubscription.externalMessages.remove();
                        serverOutboundEmitter.sendAndForget(externalMessageWithDate.externalMessage);
                        ackIdSubids.remove(Tuple2.of(sessionSubscription.sessionId, sessionSubscription.subscriptionId));
                        LOGGER.debug("Resending unacked message for session {}, subscription {]", sessionSubscription.sessionId, sessionSubscription.subscriptionId);
                    }
                }
            });
            sessionSubscription.setAckTimerId(ackTimerId);
        }
    }

    public void deleteSessionSubscription(SessionSubscription sessionSubscription) {
        SessionSubscription sessionSubscriptionExisting = getSessionSubscriptionBySubscription(sessionSubscription.sessionId, sessionSubscription.subscriptionId);
        if(sessionSubscriptionExisting.ackTimerId != null) {
            vertx.cancelTimer(sessionSubscription.ackTimerId);
        }
        sessionSubscriptionsBySubscription.remove(Tuples.tuple2(List.of(sessionSubscription.sessionId(), sessionSubscription.subscriptionId())));
        sessionSubscriptionsByDestination.remove(Tuples.tuple2(List.of(sessionSubscription.sessionId(), sessionSubscription.destination())));
    }

    public SessionSubscription getSessionSubscriptionBySubscription(String sessionId, String subscriptionId) {
        return sessionSubscriptionsBySubscription.get(Tuples.tuple2(List.of(sessionId, subscriptionId)));
    }

    public SessionSubscription getSessionSubscriptionByDestination(String sessionId, String destination) {
        return sessionSubscriptionsByDestination.get(Tuples.tuple2(List.of(sessionId, destination)));
    }

    public record ConnectMessage(String sessionId, int ping, int pong) {
    }

    public record SendMessage(String sessionId, String subscriptionId, ExternalMessage externalMessage, String ackId) {
    }


    public record AckMessage(String sessionId, String ackId, boolean acked) {
    }

    public record ExternalMessageWithDate(ExternalMessage externalMessage, String ackId, LocalDateTime sentTime) {
    }

    public static class SessionSubscription {
        private final String sessionId;
        private final String subscriptionId;
        private final String destination;
        private final String ack;
        private final Queue<ExternalMessageWithDate> externalMessages = new LinkedList<>();
        private Long ackTimerId;

        public SessionSubscription(String sessionId, String subscriptionId, String destination, String ack) {
            this.sessionId = sessionId;
            this.subscriptionId = subscriptionId;
            this.destination = destination;
            this.ack = ack;
        }

        public String sessionId() {
            return sessionId;
        }

        public String subscriptionId() {
            return subscriptionId;
        }

        public String destination() {
            return destination;
        }

        public String ack() {
            return ack;
        }

        public void externalMessagesAdd(ExternalMessageWithDate externalMessage) {
            externalMessages.add(externalMessage);
        }

        public ExternalMessageWithDate externalMessagesPeek() {
            return externalMessages.peek();
        }

        public ExternalMessageWithDate externalMessagesRemove() {
            return externalMessages.remove();
        }

        public Long getAckTimerId() {
            return ackTimerId;
        }

        public void setAckTimerId(Long ackTimerId) {
            this.ackTimerId = ackTimerId;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof SessionSubscription that)) return false;
            return Objects.equals(sessionId, that.sessionId) && Objects.equals(subscriptionId, that.subscriptionId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(sessionId, subscriptionId);
        }

        public boolean isAckRequired() {
            return List.of("client", "client-individual").contains(ack);
        }
    }
}

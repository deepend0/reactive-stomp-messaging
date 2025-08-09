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
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class StompRegistry {
    private static final Logger LOGGER = LoggerFactory.getLogger(StompRegistry.class);
    private final Vertx vertx;

    protected final MutinyEmitter<ExternalMessage> serverOutboundEmitter;

    private final MutinyEmitter<Message> messagingInboundEmitter;

    private static final Logger LOG = LoggerFactory.getLogger(StompRegistry.class);
    private final ConcurrentHashMap<String, Long> lastClientActivities = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Tuple2<Long, Integer>> sessionPingTimerIds = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Tuple2<Long, Integer>> sessionPongTimerIds = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Tuple2<String, String>, SessionSubscription> sessionSubscriptionsBySubscription = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Tuple2<String, String>, SessionSubscription> sessionSubscriptionsByDestination = new ConcurrentHashMap<>();

    public StompRegistry(Vertx vertx,
                         @Channel("serverOutbound") MutinyEmitter<ExternalMessage> serverOutboundEmitter,
                         @Channel("messagingInbound") MutinyEmitter<Message> messagingInboundEmitter) {
        this.vertx = vertx;
        this.serverOutboundEmitter = serverOutboundEmitter;
        this.messagingInboundEmitter = messagingInboundEmitter;
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
                    LOG.warn("Disconnecting client " + sessionId + " - no client activity in the last " + delta + " ms");
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
        var sessionIdSubIdKeys = sessionSubscriptionsBySubscription.keySet().stream().filter(k -> sessionId.equals(k.getItem1())).toList();
        sessionIdSubIdKeys.forEach(sessionSubscriptionsBySubscription::remove);
        var sessionIdDestKeys = sessionSubscriptionsByDestination.keySet().stream().filter(k -> sessionId.equals(k.getItem1())).toList();
        sessionIdDestKeys.forEach(sessionSubscriptionsByDestination::remove);
    }

    public void addSessionSubscription(SessionSubscription sessionSubscription) {
        sessionSubscriptionsBySubscription.put(Tuples.tuple2(List.of(sessionSubscription.sessionId(), sessionSubscription.subscriptionId())), sessionSubscription);
        sessionSubscriptionsByDestination.put(Tuples.tuple2(List.of(sessionSubscription.sessionId(), sessionSubscription.destination())), sessionSubscription);
    }

    public void deleteSessionSubscription(SessionSubscription sessionSubscription) {
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

    public record SessionSubscription(String sessionId, String subscriptionId, String destination) {

        @Override
            public boolean equals(Object o) {
                if (!(o instanceof SessionSubscription that)) return false;
                return Objects.equals(sessionId, that.sessionId) && Objects.equals(subscriptionId, that.subscriptionId);
            }

            @Override
            public int hashCode() {
                return Objects.hash(sessionId, subscriptionId);
            }
        }
}

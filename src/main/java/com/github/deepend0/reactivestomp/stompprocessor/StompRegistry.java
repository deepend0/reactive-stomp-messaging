package com.github.deepend0.reactivestomp.stompprocessor;

import com.github.deepend0.reactivestomp.external.ExternalMessage;
import com.github.deepend0.reactivestomp.simplebroker.messagehandler.BrokerMessage;
import com.github.deepend0.reactivestomp.simplebroker.messagehandler.DisconnectMessage;
import io.smallrye.mutiny.tuples.Tuple2;
import io.smallrye.mutiny.tuples.Tuples;
import io.smallrye.reactive.messaging.MutinyEmitter;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.stomp.impl.FrameParser;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

@ApplicationScoped
public class StompRegistry {
    private static final Logger LOGGER = LoggerFactory.getLogger(StompRegistry.class);
    private final Vertx vertx;

    protected final MutinyEmitter<ExternalMessage> serverOutboundEmitter;

    private final MutinyEmitter<BrokerMessage> brokerInboundEmitter;

    private static final Logger LOG = LoggerFactory.getLogger(StompRegistry.class);
    private final ConcurrentHashMap<String, Long> lastClientActivities = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Tuple2<Long, Integer>> sessionPingTimerIds = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Tuple2<Long, Integer>> sessionPongTimerIds = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Tuple2<String, String>, SessionSubscription> sessionSubscriptionsBySubscription = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Tuple2<String, String>, SessionSubscription> sessionSubscriptionsByDestination = new ConcurrentHashMap<>();

    public StompRegistry(Vertx vertx,
                         @Channel("serverOutbound") MutinyEmitter<ExternalMessage> serverOutboundEmitter,
                         @Channel("brokerInbound") MutinyEmitter<BrokerMessage> brokerInboundEmitter) {
        this.vertx = vertx;
        this.serverOutboundEmitter = serverOutboundEmitter;
        this.brokerInboundEmitter = brokerInboundEmitter;
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
                    brokerInboundEmitter.sendAndForget(new DisconnectMessage(sessionId));
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
        vertx.cancelTimer(pingTimer.getItem1());
        vertx.cancelTimer(pongTimer.getItem2());
        sessionPingTimerIds.remove(sessionId);
        sessionPongTimerIds.remove(sessionId);
        lastClientActivities.remove(sessionId);
        var sessionIdSubIdKeys = sessionSubscriptionsBySubscription.keySet().stream().filter(k -> sessionId.equals(k.getItem1())).toList();
        sessionIdSubIdKeys.forEach(sessionSubscriptionsBySubscription::remove);
        var sessionIdDestKeys = sessionSubscriptionsByDestination.keySet().stream().filter(k -> sessionId.equals(k.getItem1())).toList();
        sessionIdDestKeys.forEach(sessionSubscriptionsByDestination::remove);
    }

    public void addSessionSubscription(SessionSubscription sessionSubscription) {
        sessionSubscriptionsBySubscription.put(Tuples.tuple2(List.of(sessionSubscription.getSessionId(), sessionSubscription.getSubscriptionId())), sessionSubscription);
        sessionSubscriptionsByDestination.put(Tuples.tuple2(List.of(sessionSubscription.getSessionId(), sessionSubscription.getDestination())), sessionSubscription);
    }

    public void deleteSessionSubscription(SessionSubscription sessionSubscription) {
        sessionSubscriptionsBySubscription.remove(Tuples.tuple2(List.of(sessionSubscription.getSessionId(), sessionSubscription.getSubscriptionId())));
        sessionSubscriptionsByDestination.remove(Tuples.tuple2(List.of(sessionSubscription.getSessionId(), sessionSubscription.getDestination())));
    }

    public SessionSubscription getSessionSubscriptionBySubscription(String sessionId, String subscriptionId) {
        return sessionSubscriptionsBySubscription.get(Tuples.tuple2(List.of(sessionId, subscriptionId)));
    }

    public SessionSubscription getSessionSubscriptionByDestination(String sessionId, String destination) {
        return sessionSubscriptionsByDestination.get(Tuples.tuple2(List.of(sessionId, destination)));
    }


    public static class SessionSubscription {
        private String sessionId;
        private String subscriptionId;
        private String destination;

        public SessionSubscription(String sessionId, String subscriptionId, String destination) {
            this.sessionId = sessionId;
            this.subscriptionId = subscriptionId;
            this.destination = destination;
        }

        public String getSessionId() {
            return sessionId;
        }

        public void setSessionId(String sessionId) {
            this.sessionId = sessionId;
        }

        public String getSubscriptionId() {
            return subscriptionId;
        }

        public void setSubscriptionId(String subscriptionId) {
            this.subscriptionId = subscriptionId;
        }

        public String getDestination() {
            return destination;
        }

        public void setDestination(String destination) {
            this.destination = destination;
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
    }
}

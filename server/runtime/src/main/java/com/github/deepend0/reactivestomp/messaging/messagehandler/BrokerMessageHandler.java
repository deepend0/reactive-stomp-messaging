package com.github.deepend0.reactivestomp.messaging.messagehandler;

import com.github.deepend0.reactivestomp.messaging.broker.MessageBrokerClient;
import com.github.deepend0.reactivestomp.messaging.broker.simplebroker.Subscriber;
import com.github.deepend0.reactivestomp.messaging.model.*;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.subscription.Cancellable;
import io.smallrye.reactive.messaging.MutinyEmitter;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

@ApplicationScoped
public class BrokerMessageHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(BrokerMessageHandler.class);
    private final MessageBrokerClient messageBrokerClient;
    private final MutinyEmitter<Message> brokerOutboundEmitter;
    private final Map<String, Map<String, Cancellable>> subscriptions = new ConcurrentHashMap<>();

    public BrokerMessageHandler(MessageBrokerClient messageBrokerClient, @Channel("brokerOutbound") MutinyEmitter<Message> brokerOutboundEmitter) {
        this.messageBrokerClient = messageBrokerClient;
        this.brokerOutboundEmitter = brokerOutboundEmitter;
    }

    @Incoming("brokerInbound")
    public Uni<Void> handleBrokerMessage(Message message) {
        return switch (message) {
            case SubscribeMessage subscribeMessage ->
                //Fire and forget
                Uni.createFrom().voidItem().onItem().invoke(()-> {
                            Cancellable cancellable =
                                    messageBrokerClient.subscribe(
                                                    new Subscriber(subscribeMessage.getSubscriberId()),
                                                    subscribeMessage.getDestination())
                                            .onItem()
                                            .call(payload -> {
                                                SendMessage sendMessage = new SendMessage(subscribeMessage.getSubscriberId(),
                                                        subscribeMessage.getDestination(),
                                                        (byte[]) payload);
                                                return brokerOutboundEmitter.send(sendMessage);
                                            }).subscribe().with(
                                                    ignored -> {
                                                    },
                                                    failure -> LOGGER.error("Broker outbound forwarding failed", failure)
                                            );
                            subscriptions.computeIfAbsent(
                                    subscribeMessage.getSubscriberId(), key -> new ConcurrentHashMap<>())
                                        .put(subscribeMessage.getDestination(), cancellable);
                        }
                );
            case UnsubscribeMessage unsubscribeMessage -> {
                Optional.ofNullable(subscriptions.get(unsubscribeMessage.getSubscriberId()))
                        .ifPresent(map->map.get(unsubscribeMessage.getDestination()).cancel());
                yield Uni.createFrom().voidItem();
            }
            case SendMessage sendMessage -> messageBrokerClient.send(sendMessage.getDestination(), sendMessage.getPayload());
            case DisconnectMessage disconnectMessage ->
                Uni.createFrom().future(CompletableFuture.runAsync(()->
                        Optional.ofNullable(subscriptions.get(disconnectMessage.getSubscriberId()))
                                .ifPresent(map->map.values().forEach(Cancellable::cancel))));

            default -> Uni.createFrom().failure(new IllegalStateException("Unexpected value: " + message));
        };
    }
}

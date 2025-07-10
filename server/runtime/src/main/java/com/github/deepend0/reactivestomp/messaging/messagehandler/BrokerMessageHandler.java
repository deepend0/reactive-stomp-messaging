package com.github.deepend0.reactivestomp.messaging.messagehandler;

import com.github.deepend0.reactivestomp.messaging.broker.MessageBroker;
import com.github.deepend0.reactivestomp.messaging.broker.simplebroker.SimpleBroker;
import com.github.deepend0.reactivestomp.messaging.broker.simplebroker.Subscriber;
import com.github.deepend0.reactivestomp.messaging.model.*;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.MutinyEmitter;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class BrokerMessageHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(BrokerMessageHandler.class);
    private final MessageBroker messageBroker;
    private final MutinyEmitter<Message> brokerOutboundEmitter;

    public BrokerMessageHandler(SimpleBroker messageBroker, @Channel("brokerOutbound") MutinyEmitter<Message> brokerOutboundEmitter) {
        this.messageBroker = messageBroker;
        this.brokerOutboundEmitter = brokerOutboundEmitter;
    }

    @Incoming("brokerInbound")
    public Uni<Void> handleBrokerMessage(Message message) {
        return switch (message) {
            case SubscribeMessage subscribeMessage ->
                //Fire and forget
                Uni.createFrom().voidItem().onItem().invoke(()->
                    messageBroker.subscribe(
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
                    )
                );
            case UnsubscribeMessage unsubscribeMessage ->
                    messageBroker.unsubscribe(new Subscriber(unsubscribeMessage.getSubscriberId()), unsubscribeMessage.getDestination());
            case SendMessage sendMessage -> messageBroker.send(sendMessage.getDestination(), sendMessage.getPayload());
            case DisconnectMessage disconnectMessage ->
                    messageBroker.unsubscribeAll(new Subscriber(disconnectMessage.getSubscriberId()));
            default -> Uni.createFrom().failure(new IllegalStateException("Unexpected value: " + message));
        };
    }
}

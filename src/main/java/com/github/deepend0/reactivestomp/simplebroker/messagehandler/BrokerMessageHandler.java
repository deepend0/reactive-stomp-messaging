package com.github.deepend0.reactivestomp.simplebroker.messagehandler;

import com.github.deepend0.reactivestomp.simplebroker.SimpleBroker;
import com.github.deepend0.reactivestomp.simplebroker.Subscriber;
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
    private final SimpleBroker simpleBroker;
    private final MutinyEmitter<BrokerMessage> brokerOutboundEmitter;

    public BrokerMessageHandler(SimpleBroker simpleBroker, @Channel("brokerOutbound") MutinyEmitter<BrokerMessage> brokerOutboundEmitter) {
        this.simpleBroker = simpleBroker;
        this.brokerOutboundEmitter = brokerOutboundEmitter;
    }

    @Incoming("brokerInbound")
    public Uni<Void> handleBrokerMessage(BrokerMessage brokerMessage) {
        return switch (brokerMessage) {
            case SubscribeMessage subscribeMessage ->
                //Fire and forget
                Uni.createFrom().voidItem().onItem().invoke(()->
                    simpleBroker.subscribe(
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
                    simpleBroker.unsubscribe(new Subscriber(unsubscribeMessage.getSubscriberId()), unsubscribeMessage.getDestination());
            case SendMessage sendMessage -> simpleBroker.send(sendMessage.getDestination(), sendMessage.getPayload());
            case DisconnectMessage disconnectMessage ->
                    simpleBroker.unsubscribeAll(new Subscriber(disconnectMessage.getSubscriberId()));
            default -> Uni.createFrom().failure(new IllegalStateException("Unexpected value: " + brokerMessage));
        };
    }
}

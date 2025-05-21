package com.github.deepend0.reactivestomp.simplebroker.messagehandler;

import com.github.deepend0.reactivestomp.simplebroker.SimpleBroker;
import com.github.deepend0.reactivestomp.simplebroker.Subscriber;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.MutinyEmitter;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Incoming;

@ApplicationScoped
public class BrokerMessageHandler {

    private final SimpleBroker simpleBroker;
    private final MutinyEmitter<BrokerMessage> brokerOutboundEmitter;

    public BrokerMessageHandler(SimpleBroker simpleBroker, @Channel("brokerOutbound") MutinyEmitter<BrokerMessage> brokerOutboundEmitter) {
        this.simpleBroker = simpleBroker;
        this.brokerOutboundEmitter = brokerOutboundEmitter;
    }

    @Incoming("brokerInbound")
    public Uni<Void> handleBrokerMessage(BrokerMessage brokerMessage) {
        return switch (brokerMessage) {
            case SubscribeMessage subscribeMessage -> subscribe(subscribeMessage).toUni().replaceWithVoid();
            case UnsubscribeMessage unsubscribeMessage ->
                    simpleBroker.unsubscribe(new Subscriber(unsubscribeMessage.getSubscriberId()), unsubscribeMessage.getDestination());
            case SendMessage sendMessage -> simpleBroker.send(sendMessage.getDestination(), sendMessage.getPayload());
            case DisconnectMessage disconnectMessage ->
                    simpleBroker.unsubscribeAll(new Subscriber(disconnectMessage.getSubscriberId()));
            default -> Uni.createFrom().failure(new IllegalStateException("Unexpected value: " + brokerMessage));
        };
    }

    public Multi<SendMessage> subscribe(SubscribeMessage subscribeMessage) {
        return simpleBroker.subscribe(
                        new Subscriber(subscribeMessage.getSubscriberId()),
                        subscribeMessage.getDestination())
                .map(payload -> new SendMessage(subscribeMessage.getSubscriberId(),
                        subscribeMessage.getDestination(),
                        (byte[]) payload))
                .broadcast().withCancellationAfterLastSubscriberDeparture().toAtLeast(1)
                .call(brokerOutboundEmitter::send);
    }
}

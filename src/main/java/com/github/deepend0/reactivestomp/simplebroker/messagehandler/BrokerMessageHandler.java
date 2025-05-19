package com.github.deepend0.reactivestomp.simplebroker.messagehandler;

import com.github.deepend0.reactivestomp.simplebroker.SimpleBroker;
import com.github.deepend0.reactivestomp.simplebroker.Subscriber;
import io.smallrye.mutiny.Multi;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

@ApplicationScoped
public class BrokerMessageHandler {

    private final SimpleBroker simpleBroker;

    public BrokerMessageHandler(SimpleBroker simpleBroker) {
        this.simpleBroker = simpleBroker;
    }

    @Incoming("brokerInbound")
    public void handleBrokerMessage(BrokerMessage brokerMessage) {
        switch (brokerMessage) {
            case SubscribeMessage subscribeMessage:
                subscribe(subscribeMessage);
                break;
            case UnsubscribeMessage unsubscribeMessage:
                simpleBroker.unsubscribe(new Subscriber(unsubscribeMessage.getSubscriberId()), unsubscribeMessage.getDestination());
                break;
            case SendMessage sendMessage:
                simpleBroker.send(sendMessage.getDestination(), sendMessage.getPayload());
                break;
            case DisconnectMessage disconnectMessage:
                simpleBroker.unsubscribeAll(new Subscriber(disconnectMessage.getSubscriberId()));
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + brokerMessage);
        };
    }

    @Outgoing("brokerOutbound")
    public Multi<SendMessage> subscribe(SubscribeMessage subscribeMessage) {
        return simpleBroker.subscribe(
                        new Subscriber(subscribeMessage.getSubscriberId()),
                        subscribeMessage.getDestination())
                .map(payload -> new SendMessage(subscribeMessage.getSubscriberId(),
                                subscribeMessage.getDestination(),
                                (byte[]) payload));
    }
}

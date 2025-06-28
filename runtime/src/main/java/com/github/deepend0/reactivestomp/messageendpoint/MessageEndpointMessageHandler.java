package com.github.deepend0.reactivestomp.messageendpoint;

import com.github.deepend0.reactivestomp.simplebroker.messagehandler.BrokerMessage;
import com.github.deepend0.reactivestomp.simplebroker.messagehandler.SendMessage;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.MutinyEmitter;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class MessageEndpointMessageHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(MessageEndpointMessageHandler.class);
    private final MessageEndpointRegistry messageEndpointRegistry;
    private final Serde serde;
    private final MutinyEmitter<BrokerMessage> brokerInboundEmitter;

    public MessageEndpointMessageHandler(
            MessageEndpointRegistry messageEndpointRegistry,
            Serde serde,
            @Channel("brokerInbound") MutinyEmitter<BrokerMessage> brokerInboundEmitter) {
        this.messageEndpointRegistry = messageEndpointRegistry;
        this.serde = serde;
        this.brokerInboundEmitter = brokerInboundEmitter;
    }

    @Incoming("messageEndpointInbound")
    public Uni<Void> handle(SendMessage sendMessage) {
        return Uni.join().all(messageEndpointRegistry.getMessageEndpoints(sendMessage.getDestination())
                .stream().
                map(messageEndpointMethodWrapper ->
                    {
                        Multi<byte[]> result = messageEndpointMethodWrapper.call(serde, sendMessage.getPayload());

                        if(messageEndpointMethodWrapper.getOutboundDestination() != null) {
                            // Fire and forget
                            return Uni.createFrom().voidItem()
                                    .onItem().invoke(() -> result
                                            .map(payload -> new SendMessage(
                                                    "server",
                                                    messageEndpointMethodWrapper.getOutboundDestination(),
                                                    payload))
                                            .onItem()
                                            .transformToUni(brokerInboundEmitter::send)
                                            .merge()
                                            .subscribe().with(
                                                    ignored -> {},
                                                    error -> LOGGER.error("Processing message endpoint failed", error)
                                            )
                                    );
                        } else {
                            return Uni.createFrom().voidItem();
                        }

                })
                .toList())
                .andFailFast()
                .replaceWithVoid();
    }
}

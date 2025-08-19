package com.github.deepend0.reactivestomp.messaging.messagehandler;

import com.github.deepend0.reactivestomp.messaging.messageendpoint.MessageEndpointResponse;
import com.github.deepend0.reactivestomp.messaging.model.Message;
import com.github.deepend0.reactivestomp.messaging.model.SendMessage;
import com.github.deepend0.reactivestomp.messaging.messageendpoint.MessageEndpointRegistry;
import com.github.deepend0.reactivestomp.messaging.messageendpoint.Serde;
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
    private final MutinyEmitter<Message> brokerInboundEmitter;

    public MessageEndpointMessageHandler(
            MessageEndpointRegistry messageEndpointRegistry,
            Serde serde,
            @Channel("brokerInbound") MutinyEmitter<Message> brokerInboundEmitter) {
        this.messageEndpointRegistry = messageEndpointRegistry;
        this.serde = serde;
        this.brokerInboundEmitter = brokerInboundEmitter;
    }

    @Incoming("messageEndpointInbound")
    public Uni<Void> handle(SendMessage sendMessage) {
        return Uni.join().all(messageEndpointRegistry.getMessageEndpoints(sendMessage.getDestination())
                .stream()
                .map(messageEndpointMethodWrapper ->
                    {
                        Uni<MessageEndpointResponse<Multi<byte[]>>> responseUni = messageEndpointMethodWrapper.call(serde, sendMessage.getPayload());

                        return responseUni.flatMap(response-> {
                            if (messageEndpointMethodWrapper.getOutboundDestination() != null ||
                                    response.outboundDestination() != null) {
                                String outboundDestination = messageEndpointMethodWrapper.getOutboundDestination() != null ?
                                        messageEndpointMethodWrapper.getOutboundDestination() :
                                        response.outboundDestination();
                                // Fire and forget
                                return Uni.createFrom().voidItem()
                                        .onItem().invoke(() -> response.value()
                                                .map(payload -> new SendMessage(
                                                        "server",
                                                        outboundDestination,
                                                        payload))
                                                .onItem()
                                                .transformToUni(brokerInboundEmitter::send)
                                                .merge()
                                                .subscribe().with(
                                                        ignored -> {
                                                        },
                                                        error -> LOGGER.error("Processing message endpoint failed", error)
                                                )
                                        );
                            } else {
                                return Uni.createFrom().voidItem().onItem().invoke(() -> response.value()
                                        .subscribe()
                                        .with(
                                                ignored -> {
                                                },
                                                error -> LOGGER.error("Processing message endpoint failed", error)
                                        ));
                            }
                        });
                })
                .toList())
                .andFailFast()
                .replaceWithVoid();
    }
}

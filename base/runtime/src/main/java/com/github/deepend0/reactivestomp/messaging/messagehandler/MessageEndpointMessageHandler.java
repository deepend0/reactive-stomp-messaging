package com.github.deepend0.reactivestomp.messaging.messagehandler;

import com.github.deepend0.reactivestomp.messaging.messageendpoint.MessageEndpointMethodWrapper;
import com.github.deepend0.reactivestomp.messaging.messageendpoint.MessageEndpointRegistry;
import com.github.deepend0.reactivestomp.messaging.messageendpoint.MessageEndpointResponse;
import com.github.deepend0.reactivestomp.messaging.messageendpoint.Serde;
import com.github.deepend0.reactivestomp.messaging.model.Message;
import com.github.deepend0.reactivestomp.messaging.model.SendMessage;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.MutinyEmitter;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

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
        // Fire and forget
        return Uni.createFrom().voidItem().onItem().invoke(()->
                Multi.createFrom().item(messageEndpointRegistry.getMessageEndpoints(sendMessage.getDestination()))
                .flatMap(matchResult ->
                    {
                        MessageEndpointMethodWrapper<?, ?> messageEndpointMethodWrapper = matchResult.getHandler();
                        Map<String, String> inboundPathParams = matchResult.getParams();
                        Multi<MessageEndpointResponse<Multi<byte[]>>> responseMulti = messageEndpointMethodWrapper.call(serde, inboundPathParams, sendMessage.getPayload());

                        return responseMulti
                            .onFailure().invoke(t -> LOGGER.error("Endpoint call failed", t))
                            .flatMap(response -> {
                                String outboundDestination = messageEndpointMethodWrapper.getOutboundDestination() != null ?
                                        messageEndpointMethodWrapper.getOutboundDestination() :
                                        response.outboundDestination();

                                Multi<byte []> payloads = response.value();
                                if (outboundDestination != null) {
                                    try{
                                        String outboundPath = messageEndpointMethodWrapper.evaluateOutboundPath(outboundDestination, serde, inboundPathParams, sendMessage.getPayload());
                                        return payloads
                                                .map(payload -> new SendMessage(
                                                        "server",
                                                        outboundPath,
                                                        payload))
                                                .onItem()
                                                .transformToUni(brokerInboundEmitter::send)
                                                .merge();
                                    } catch (IOException e) {
                                        LOGGER.error("Failed to evaluate outbound path", e);
                                        return Multi.createFrom().failure(e);
                                    }
                                } else {
                                    return payloads;
                                }
                        });
                }).subscribe()
                    .with(ignored->{}, error -> LOGGER.error("Processing message endpoint failed", error))
        );
    }
}

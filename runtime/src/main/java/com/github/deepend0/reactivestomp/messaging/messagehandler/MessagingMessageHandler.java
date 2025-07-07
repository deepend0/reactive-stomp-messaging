package com.github.deepend0.reactivestomp.messaging.messagehandler;

import com.github.deepend0.reactivestomp.messaging.model.Message;
import com.github.deepend0.reactivestomp.messaging.model.SendMessage;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.MutinyEmitter;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Incoming;

import java.util.List;
import java.util.Optional;

@ApplicationScoped
public class MessagingMessageHandler {
    private final Optional<List<String>> messageEndpointPrefixes;

    private final MutinyEmitter<Message> brokerInboundEmitter;

    private final MutinyEmitter<SendMessage> messageEndpointInboundEmitter;

    public MessagingMessageHandler(@ConfigProperty(name = "reactive-stomp.messaging.message-endpoint.paths") Optional<List<String>> messageEndpointPrefixes,
                                   @Channel("brokerInbound") MutinyEmitter<Message> brokerInboundEmitter,
                                   @Channel("messageEndpointInbound") MutinyEmitter<SendMessage> messageEndpointInboundEmitter) {
        this.messageEndpointPrefixes = messageEndpointPrefixes;
        this.brokerInboundEmitter = brokerInboundEmitter;
        this.messageEndpointInboundEmitter = messageEndpointInboundEmitter;
    }

    @Incoming("messagingInbound")
    public Uni<Void> handle(Message message) {
        if (message instanceof SendMessage sendMessage) {
            if (messageEndpointPrefixes.stream().flatMap(List::stream).anyMatch(prefix -> sendMessage.getDestination().startsWith(prefix))) {
                return messageEndpointInboundEmitter.send(sendMessage);
            }
        }
        return brokerInboundEmitter.send(message);
    }
}

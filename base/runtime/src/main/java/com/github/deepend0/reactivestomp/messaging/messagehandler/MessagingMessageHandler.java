package com.github.deepend0.reactivestomp.messaging.messagehandler;

import com.github.deepend0.reactivestomp.messaging.model.Message;
import com.github.deepend0.reactivestomp.messaging.model.SendMessage;
import com.github.deepend0.reactivestomp.messaging.model.SubscribeMessage;
import com.github.deepend0.reactivestomp.messaging.model.UnsubscribeMessage;
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
    private final Optional<List<String>> brokerRegexes;

    private final Optional<List<String>> messageEndpointRegexes;

    private final MutinyEmitter<Message> brokerInboundEmitter;

    private final MutinyEmitter<SendMessage> messageEndpointInboundEmitter;

    public MessagingMessageHandler(@ConfigProperty(name = "reactive-stomp.messaging.broker.regex") Optional<List<String>> brokerRegexes,
                                   @ConfigProperty(name = "reactive-stomp.messaging.message-endpoint.regex") Optional<List<String>> messageEndpointRegexes,
                                   @Channel("brokerInbound") MutinyEmitter<Message> brokerInboundEmitter,
                                   @Channel("messageEndpointInbound") MutinyEmitter<SendMessage> messageEndpointInboundEmitter) {
        this.brokerRegexes = brokerRegexes;
        this.messageEndpointRegexes = messageEndpointRegexes;
        this.brokerInboundEmitter = brokerInboundEmitter;
        this.messageEndpointInboundEmitter = messageEndpointInboundEmitter;
    }

    @Incoming("messagingInbound")
    public Uni<Void> handle(Message message) {
        if (message instanceof SendMessage sendMessage) {
            if (messageEndpointRegexes.stream().flatMap(List::stream).anyMatch(regex -> sendMessage.getDestination().matches(regex))) {
                return messageEndpointInboundEmitter.send(sendMessage);
            } else if (brokerRegexes.stream().flatMap(List::stream).anyMatch(regex -> sendMessage.getDestination().matches(regex))) {
                return brokerInboundEmitter.send(message);
            } else {
                return Uni.createFrom().voidItem();
            }
        } else if (message instanceof SubscribeMessage subscribeMessage) {
            if (brokerRegexes.stream().flatMap(List::stream).anyMatch(regex -> subscribeMessage.getDestination().matches(regex))) {
                return brokerInboundEmitter.send(message);
            } else {
                return Uni.createFrom().voidItem();
            }
        } else if (message instanceof UnsubscribeMessage unsubscribeMessage) {
            if (brokerRegexes.stream().flatMap(List::stream).anyMatch(regex -> unsubscribeMessage.getDestination().matches(regex))) {
                return brokerInboundEmitter.send(message);
            } else {
                return Uni.createFrom().voidItem();
            }
        }
        return brokerInboundEmitter.send(message);
    }
}

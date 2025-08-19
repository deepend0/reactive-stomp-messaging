package com.github.deepend0.reactivestomp.messaging.client;

import com.github.deepend0.reactivestomp.messaging.messageendpoint.Serde;
import com.github.deepend0.reactivestomp.messaging.model.Message;
import com.github.deepend0.reactivestomp.messaging.model.SendMessage;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.MutinyEmitter;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.reactive.messaging.Channel;

@ApplicationScoped
public class MessagingClientImpl implements MessagingClient {

    @Inject
    @Channel("messagingInbound")
    private MutinyEmitter<Message> messagingInboundEmitter;

    @Inject
    private Serde serde;

    @Override
    public Uni<Void> send(String destination, Object message) {
        try {
            SendMessage sendMessage = new SendMessage("internal", destination, serde.serialize(message));
            return messagingInboundEmitter.send(sendMessage);
        } catch (Exception e) {
            return Uni.createFrom().failure(new RuntimeException("Failed to serialize message", e));
        }
    }
}

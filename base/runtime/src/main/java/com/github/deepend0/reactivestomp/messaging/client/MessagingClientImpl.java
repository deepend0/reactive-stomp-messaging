package com.github.deepend0.reactivestomp.messaging.client;

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

    @Override
    public Uni<Void> send(String destination, byte[] message) {
        SendMessage sendMessage = new SendMessage("internal", destination, message);
        return messagingInboundEmitter.send(sendMessage);
    }
}

package com.github.deepend0.reactivestomp.messaging.client;

import com.github.deepend0.reactivestomp.messaging.messageendpoint.Serde;
import com.github.deepend0.reactivestomp.messaging.model.Message;
import com.github.deepend0.reactivestomp.messaging.model.SendMessage;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.subscription.Cancellable;
import io.smallrye.reactive.messaging.MutinyEmitter;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

@ApplicationScoped
public class MessagingClientImpl implements MessagingClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(MessagingClientImpl.class);

    @Inject
    @Channel("messagingInbound")
    private MutinyEmitter<Message> messagingInboundEmitter;

    @Inject
    private Serde serde;

    @Override
    public void send(String destination, Object message) {
        try {
            SendMessage sendMessage = new SendMessage("server", destination, serde.serialize(message));
            messagingInboundEmitter.sendAndForget(sendMessage);
        } catch (IOException e) {
            LOGGER.error("Serializing error when sending message", e);
        }
    }
}

package com.github.deepend0.reactivestomp.stompprocessor;

import com.github.deepend0.reactivestomp.messaging.model.SendMessage;
import com.github.deepend0.reactivestomp.websocket.ExternalMessage;
import io.smallrye.mutiny.Uni;

public interface StompProcessor {
    String CONNECT_EVENT_DESTINATION = "stomp-processor.connect";
    String DISCONNECT_EVENT_DESTINATION = "stomp-processor.disconnect";
    String SUBSCRIBE_EVENT_DESTINATION = "stomp-processor.subscribe";
    String UNSUBSCRIBE_EVENT_DESTINATION = "stomp-processor.unsubscribe";
    String MESSAGE_SEND_DESTINATION = "stomp-processor.send";
    String MESSAGE_ACK_DESTINATION = "stomp-processor.ack";
    String MESSAGE_NACK_DESTINATION = "stomp-processor.nack";

    Uni<Void> processFromClient(ExternalMessage externalMessage);

    Uni<Void> processToClient(SendMessage sendMessage);
}

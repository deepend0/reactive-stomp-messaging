package com.github.deepend0.reactivestomp.stompprocessor;

import com.github.deepend0.reactivestomp.messaging.model.SendMessage;
import com.github.deepend0.reactivestomp.websocket.ExternalMessage;
import io.smallrye.mutiny.Uni;

public interface StompProcessor {
    String CONNECT_EVENT_DESTINATION = "stomp-processor.connect";
    String DISCONNECT_EVENT_DESTINATION = "stomp-processor.disconnect";
    String SUBSCRIBE_EVENT_DESTINATION = "stomp-processor.subscribe";
    String UNSUBSCRIBE_EVENT_DESTINATION = "stomp-processor.unsubscribe";

    Uni<Void> processFromClient(ExternalMessage externalMessage);

    Uni<Void> processToClient(SendMessage sendMessage);
}

package com.github.deepend0.reactivestomp.stompprocessor;

import com.github.deepend0.reactivestomp.websocket.ExternalMessage;
import com.github.deepend0.reactivestomp.messaging.model.SendMessage;
import io.smallrye.mutiny.Uni;

public interface StompProcessor {
    Uni<Void> processFromClient(ExternalMessage externalMessage);

    Uni<Void> processToClient(SendMessage sendMessage);
}

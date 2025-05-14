package com.github.deepend0.reactivestomp.stompprocessor;

import com.github.deepend0.reactivestomp.message.ExternalMessage;
import com.github.deepend0.reactivestomp.simplebroker.model.SendMessage;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;

public interface StompProcessor {
    Multi<Void> processFromClient(ExternalMessage externalMessage);

    Uni<Void> processToClient(SendMessage sendMessage);
}

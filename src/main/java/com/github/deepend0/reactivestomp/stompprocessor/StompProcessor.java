package com.github.deepend0.reactivestomp.stompprocessor;

import com.github.deepend0.reactivestomp.external.ExternalMessage;
import com.github.deepend0.reactivestomp.simplebroker.messagehandler.SendMessage;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;

public interface StompProcessor {
    Multi<Void> processFromClient(ExternalMessage externalMessage);

    Uni<Void> processToClient(SendMessage sendMessage);
}

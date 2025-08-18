package com.github.deepend0.reactivestomp.messaging.client;

import io.smallrye.mutiny.Uni;

public interface MessagingClient {

    Uni<Void> send(String destination, byte[] message);
}

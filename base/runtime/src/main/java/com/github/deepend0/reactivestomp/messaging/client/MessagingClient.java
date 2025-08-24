package com.github.deepend0.reactivestomp.messaging.client;

public interface MessagingClient {

    void send(String destination, Object message);
}

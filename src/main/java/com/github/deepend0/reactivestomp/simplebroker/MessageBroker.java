package com.github.deepend0.reactivestomp.simplebroker;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;

public interface MessageBroker extends Runnable {
    Uni<Void> send(String destination, Object message);

    Multi<Object> subscribe(Subscriber subscriber, String topic);

    Uni<Void> unsubscribe(Subscriber subscriber, String topic);

    void stop();
}

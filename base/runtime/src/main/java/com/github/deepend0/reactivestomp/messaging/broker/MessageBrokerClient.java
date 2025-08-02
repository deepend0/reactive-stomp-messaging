package com.github.deepend0.reactivestomp.messaging.broker;

import com.github.deepend0.reactivestomp.messaging.broker.simplebroker.Subscriber;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;

public interface MessageBrokerClient {
    Uni<Void> send(String destination, Object message);

    Multi<?> subscribe(Subscriber subscriber, String destination);
}

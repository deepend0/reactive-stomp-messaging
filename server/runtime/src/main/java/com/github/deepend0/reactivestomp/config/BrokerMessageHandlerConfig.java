package com.github.deepend0.reactivestomp.config;

import com.github.deepend0.reactivestomp.messaging.broker.simplebroker.SimpleBroker;
import io.quarkus.arc.DefaultBean;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;

@ApplicationScoped
public class BrokerMessageHandlerConfig {
    @Produces
    @DefaultBean
    public SimpleBroker simpleBroker() {
        SimpleBroker simpleBroker = SimpleBroker.build();
        simpleBroker.run();
        return simpleBroker;
    }
}

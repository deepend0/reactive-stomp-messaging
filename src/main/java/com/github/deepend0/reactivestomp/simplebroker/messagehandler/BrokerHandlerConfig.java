package com.github.deepend0.reactivestomp.simplebroker.messagehandler;

import com.github.deepend0.reactivestomp.simplebroker.SimpleBroker;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;

@ApplicationScoped
public class BrokerHandlerConfig {
    @Produces
    public SimpleBroker simpleBroker() {
        return SimpleBroker.build();
    }
}

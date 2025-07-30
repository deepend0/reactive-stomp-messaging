package com.github.deepend0.reactivestomp.messaging.broker.kafka;

import io.vertx.amqp.AmqpClientOptions;
import io.vertx.core.Vertx;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.Map;

@ApplicationScoped
public class BrokerMessageHandlerConfig {
    @Produces
    public AmqpClientAdaptor amqpClientAdaptor(Vertx vertx, @ConfigProperty(name = "amqp") Map<String, String> amqpProperties) {
        AmqpClientOptions options = new AmqpClientOptions()
                .setHost(amqpProperties.get("host"))
                .setPort(Integer.valueOf(amqpProperties.get("port")))
                .setUsername(amqpProperties.get("username"))
                .setPassword(amqpProperties.get("password"));
        return new AmqpClientAdaptor(vertx, options);
    }
}

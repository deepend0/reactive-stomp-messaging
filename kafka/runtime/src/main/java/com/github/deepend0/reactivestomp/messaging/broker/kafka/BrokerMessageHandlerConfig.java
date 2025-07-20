package com.github.deepend0.reactivestomp.messaging.broker.kafka;

import io.vertx.core.Vertx;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@ApplicationScoped
public class BrokerMessageHandlerConfig {
    @Produces
    public KafkaClientAdaptor kafkaClientAdaptor(Vertx vertx, @ConfigProperty(name = "kafka.bootstrap-servers") String kafkaBootstrapServer) {
        return new KafkaClientAdaptor(vertx, kafkaBootstrapServer);
    }
}

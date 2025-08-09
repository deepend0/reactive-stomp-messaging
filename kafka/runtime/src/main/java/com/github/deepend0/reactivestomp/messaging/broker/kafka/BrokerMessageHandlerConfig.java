package com.github.deepend0.reactivestomp.messaging.broker.kafka;

import io.vertx.core.Vertx;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@ApplicationScoped
public class BrokerMessageHandlerConfig {
    @Produces
    public KafkaClientAdaptor kafkaClientAdaptor(Vertx vertx,
                                                 @ConfigProperty(name = "kafka.bootstrap-servers") String bootstrapServers,
                                                 @ConfigProperty(name = "kafka.producer") Optional<Map<String, String>> producerProperties,
                                                 @ConfigProperty(name = "kafka.consumer") Optional<Map<String, String>> consumerProperties) {
        return new KafkaClientAdaptor(vertx, bootstrapServers, producerProperties.orElse(new HashMap<>()), consumerProperties.orElse(new HashMap<>()));
    }
}

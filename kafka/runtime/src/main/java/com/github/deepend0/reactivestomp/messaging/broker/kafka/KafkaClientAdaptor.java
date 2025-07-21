package com.github.deepend0.reactivestomp.messaging.broker.kafka;

import com.github.deepend0.reactivestomp.messaging.broker.MessageBrokerClient;
import com.github.deepend0.reactivestomp.messaging.broker.simplebroker.Subscriber;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.vertx.core.Vertx;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class KafkaClientAdaptor implements MessageBrokerClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaClientAdaptor.class);
    private final Vertx vertx;

    private final Map<String, String> producerProperties;

    private final Map<String, String> consumerProperties;

    private final KafkaProducer<String, byte []> kafkaProducer;

    public KafkaClientAdaptor(Vertx vertx, @ConfigProperty(name = "kafka.bootstrap-servers") String kafkaBootstrapServers) {
        this.vertx = vertx;
        this.producerProperties = new HashMap<>();
        producerProperties.put("bootstrap.servers", kafkaBootstrapServers);
        producerProperties.put("key.serializer", StringSerializer.class.getName());
        producerProperties.put("value.serializer", ByteArraySerializer.class.getName());
        this.consumerProperties = new HashMap<>();
        consumerProperties.put("bootstrap.servers", kafkaBootstrapServers);
        consumerProperties.put("key.deserializer", StringDeserializer.class.getName());
        consumerProperties.put("value.deserializer", ByteArrayDeserializer.class.getName());
        consumerProperties.put("auto.offset.reset", "earliest");

        this.kafkaProducer = KafkaProducer.create(vertx, producerProperties);
    }

    @Override
    public Uni<Void> send(String destination, Object message) {
        return Uni.createFrom().item(kafkaProducer.send(KafkaProducerRecord.create(destination.replace("/", "."), (byte []) message)).result()).replaceWithVoid();
    }

    public Multi<?> subscribe(Subscriber subscriber, String destination) {
        consumerProperties.put("group.id", UUID.randomUUID().toString());
        KafkaConsumer<String, byte []> kafkaConsumer = KafkaConsumer.create(vertx, consumerProperties);
        kafkaConsumer.subscribe(Set.of(destination.replace("/", ".")));
        return Multi.createFrom().emitter(multiEmitter -> {
            kafkaConsumer.handler(kafkaConsumerRecord -> {
                LOGGER.debug("Received Kafka Message {}", new String(kafkaConsumerRecord.record().value()));
                multiEmitter.emit(kafkaConsumerRecord.record().value());
            });
            kafkaConsumer.exceptionHandler(multiEmitter::fail);
        }).onCancellation().invoke(kafkaConsumer::unsubscribe);
    }
}

package com.github.deepend0.reactivestomp.messaging.broker.kafka;

import com.github.deepend0.reactivestomp.messaging.broker.MessageBrokerClient;
import com.github.deepend0.reactivestomp.messaging.broker.simplebroker.Subscriber;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.subscription.MultiEmitter;
import io.vertx.core.Vertx;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class KafkaClientAdaptor implements MessageBrokerClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaClientAdaptor.class);

    private final KafkaProducer<String, byte []> kafkaProducer;

    private final KafkaConsumer<String, byte []> kafkaConsumer;

    private final Map<String, Map<String, MultiEmitter<? super byte[]>>> emittersByTopicAndSubscriber = new ConcurrentHashMap<>();


    public KafkaClientAdaptor(Vertx vertx,
                              String bootstrapServers,
                              Map<String, String> producerProperties,
                              Map<String, String> consumerProperties) {
        producerProperties.put("bootstrap.servers", bootstrapServers);
        producerProperties.put("key.serializer", StringSerializer.class.getName());
        producerProperties.put("value.serializer", ByteArraySerializer.class.getName());
        consumerProperties.put("bootstrap.servers", bootstrapServers);
        consumerProperties.put("key.deserializer", StringDeserializer.class.getName());
        consumerProperties.put("value.deserializer", ByteArrayDeserializer.class.getName());
        consumerProperties.put("auto.offset.reset", "earliest");
        consumerProperties.put("group.id", UUID.randomUUID().toString());
        this.kafkaProducer = KafkaProducer.create(vertx, producerProperties);
        this.kafkaConsumer = KafkaConsumer.create(vertx, consumerProperties);
    }

    @Override
    public Uni<Void> send(String destination, Object message) {
        return Uni.createFrom().item(kafkaProducer.send(KafkaProducerRecord.create(destination.replace("/", "."), (byte []) message)).result()).replaceWithVoid();
    }

    public Multi<?> subscribe(Subscriber subscriber, String destination) {
        kafkaConsumer.subscribe(Set.of(destination.replace("/", ".")));
        String topicName = destination.replace("/", ".");

        return Multi.createFrom().<byte[]>emitter( multiEmitter -> {
            emittersByTopicAndSubscriber.computeIfAbsent(topicName, key -> new ConcurrentHashMap<>()).put(subscriber.getId(), multiEmitter);
            subscribeToCurrentTopics();
        }).onCancellation().invoke(()-> {
            emittersByTopicAndSubscriber.get(topicName).remove(subscriber.getId());
            if(emittersByTopicAndSubscriber.get(topicName).isEmpty()) {
                emittersByTopicAndSubscriber.remove(topicName);
            }
            subscribeToCurrentTopics();
        });
    }

    private void subscribeToCurrentTopics() {
        LOGGER.info("Subscribing to topics: {}", emittersByTopicAndSubscriber.keySet().stream().collect(Collectors.joining(",")));
        kafkaConsumer.subscribe(String.join(",", emittersByTopicAndSubscriber.keySet()));
        kafkaConsumer.handler(kafkaConsumerRecord -> {
            LOGGER.info("Received Kafka Message {}", new String(kafkaConsumerRecord.record().value()));
            emittersByTopicAndSubscriber.get(kafkaConsumerRecord.topic()).values().forEach(multiEmitter ->
                multiEmitter.emit(kafkaConsumerRecord.record().value())
            );
        });
    }
}

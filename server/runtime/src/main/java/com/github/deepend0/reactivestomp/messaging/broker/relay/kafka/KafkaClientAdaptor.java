package com.github.deepend0.reactivestomp.messaging.broker.relay.kafka;

import com.github.deepend0.reactivestomp.messaging.broker.MessageBrokerClient;
import com.github.deepend0.reactivestomp.messaging.broker.simplebroker.Subscriber;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.kafka.impl.ReactiveKafkaConsumer;
import io.smallrye.reactive.messaging.kafka.impl.ReactiveKafkaProducer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Set;

public class KafkaClientAdaptor implements MessageBrokerClient {

    ReactiveKafkaConsumer<String, byte[]> reactiveKafkaConsumer;

    ReactiveKafkaProducer<String, byte []> reactiveKafkaProducer;

    @Override
    public Uni<Void> send(String destination, Object message) {
        return reactiveKafkaProducer.send(new ProducerRecord(destination,message)).replaceWithVoid();
    }

    public Multi<?> subscribe(Subscriber subscriber, String destination) {
        return reactiveKafkaConsumer.subscribe(Set.of(destination)).map(ConsumerRecord::value);
    }
}

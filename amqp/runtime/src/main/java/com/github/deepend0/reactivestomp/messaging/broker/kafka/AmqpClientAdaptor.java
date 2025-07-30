package com.github.deepend0.reactivestomp.messaging.broker.kafka;

import com.github.deepend0.reactivestomp.messaging.broker.MessageBrokerClient;
import com.github.deepend0.reactivestomp.messaging.broker.simplebroker.Subscriber;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.subscription.MultiEmitter;
import io.vertx.amqp.*;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;


public class AmqpClientAdaptor implements MessageBrokerClient {

    private final Uni<AmqpConnection> amqpConnectionUni;
    private final Map<String, Uni<AmqpSender>> senders = new ConcurrentHashMap<>();
    private final Map<String, Uni<AmqpReceiver>> receivers = new ConcurrentHashMap<>();
    private final Map<String, List<MultiEmitter<? super byte[]>>> emitters = new ConcurrentHashMap<>();

    public AmqpClientAdaptor(Vertx vertx, AmqpClientOptions amqpClientOptions) {
        AmqpClient amqpClient = AmqpClient.create(vertx, amqpClientOptions);
        amqpConnectionUni = Uni.createFrom().completionStage(amqpClient.connect().toCompletionStage()).memoize().indefinitely();
    }

    @Override
    public Uni<Void> send(String destination, Object message) {
        return amqpConnectionUni.flatMap(amqpConnection ->
            senders.computeIfAbsent(destination, k-> Uni.createFrom().completionStage(amqpConnection.createSender(destination).toCompletionStage())
                            .memoize().indefinitely())
                .flatMap(amqpSender ->
                    Uni.createFrom()
                        .completionStage(
                            amqpSender.sendWithAck(AmqpMessage.create().withBufferAsBody(Buffer.buffer((byte[])message)).build()).toCompletionStage()))

        );
    }

    @Override
    public Multi<?> subscribe(Subscriber subscriber, String destination) {
        Multi<?> multi = Multi.createFrom().<byte[]>emitter(multiEmitter -> {
            emitters.computeIfAbsent(destination, k-> new CopyOnWriteArrayList<>()).add(multiEmitter);
            multiEmitter.onTermination(()->emitters.get(destination).remove(multiEmitter));
        });

        receivers.computeIfAbsent(destination, k -> {
                Uni<AmqpReceiver> amqpReceiverUni =  amqpConnectionUni
                            .flatMap(amqpConnection -> Uni.createFrom().completionStage(amqpConnection.createReceiver(destination).toCompletionStage())
                            .map(amqpReceiver -> amqpReceiver.handler(amqpMessage ->
                                    emitters.get(destination).forEach(multiEmitter -> multiEmitter.emit(amqpMessage.bodyAsBinary().getBytes()))))
                            .memoize().indefinitely());
                amqpReceiverUni.subscribe().with(ignored->{});
                return amqpReceiverUni;
            });

        return multi;
    }
}

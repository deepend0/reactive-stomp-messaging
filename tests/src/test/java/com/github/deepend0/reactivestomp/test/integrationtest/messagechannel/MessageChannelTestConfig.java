package com.github.deepend0.reactivestomp.test.integrationtest.messagechannel;

import com.github.deepend0.reactivestomp.websocket.ExternalMessage;
import io.quarkus.arc.profile.IfBuildProfile;
import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.MutinyEmitter;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.stomp.impl.FrameParser;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Named;
import org.eclipse.microprofile.reactive.messaging.Channel;
import jakarta.inject.Inject;

import java.util.Arrays;
import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;

@IfBuildProfile("test")
@ApplicationScoped
public class MessageChannelTestConfig {

    @Inject
    @Channel("serverInbound")
    MutinyEmitter<ExternalMessage> serverInboundEmitter;

    @Inject
    @Channel("serverOutbound")
    Multi<ExternalMessage> serverOutboundReceiver;

    @Produces
    @Named("serverInboundEmitter")
    public MutinyEmitter<ExternalMessage> produceServerInboundEmitter() {
        return serverInboundEmitter;
    }

    @Produces
    @Named("serverOutboundReceiver")
    public Multi<ExternalMessage> produceServerOutboundReceiver() {
        return serverOutboundReceiver;
    }

    private Deque<ExternalMessage> serverOutboundList = new ConcurrentLinkedDeque<>();
    private Deque<ExternalMessage> serverOutboundHeartbeats = new ConcurrentLinkedDeque<>();

    @PostConstruct
    public void init() {
        serverOutboundReceiver.subscribe().with(externalMessage -> {
            if (Arrays.equals(externalMessage.message(), Buffer.buffer(FrameParser.EOL).getBytes())
                    || Arrays.equals(externalMessage.message(), new byte[]{'\0'})) {
                serverOutboundHeartbeats.add(externalMessage);
            } else {
                serverOutboundList.add(externalMessage);
            }
        });
    }

    public Deque<ExternalMessage> getServerOutboundList() {
        return serverOutboundList;
    }

    public void resetServerOutboundList() {
        this.serverOutboundList.clear();
    }

    public void resetServerOutboundHeartbeats() {
        this.serverOutboundHeartbeats.clear();
    }
    public Deque<ExternalMessage> getServerOutboundHeartbeats() {
        return serverOutboundHeartbeats;
    }
}
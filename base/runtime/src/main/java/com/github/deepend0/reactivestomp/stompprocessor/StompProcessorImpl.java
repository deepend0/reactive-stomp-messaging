package com.github.deepend0.reactivestomp.stompprocessor;

import com.github.deepend0.reactivestomp.messaging.model.SendMessage;
import com.github.deepend0.reactivestomp.stompprocessor.framehandler.*;
import com.github.deepend0.reactivestomp.websocket.ExternalMessage;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.MutinyEmitter;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.EventBus;
import io.vertx.ext.stomp.Command;
import io.vertx.ext.stomp.Frame;
import io.vertx.ext.stomp.Frames;
import io.vertx.ext.stomp.impl.FrameParser;
import io.vertx.ext.stomp.utils.Headers;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@ApplicationScoped
public class StompProcessorImpl implements StompProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(StompProcessorImpl.class);
    public static final List<String> ACCEPTED_VERSIONS = List.of("1.2", "1.1");
    private final FrameParserAdapter frameParserAdapter;
    private final StompRegistry stompRegistry;
    private final MessageIdGenerator messageIdGenerator;

    private final Map<Command, FrameHandler> commandFrameHandlerMap = new HashMap<>();

    private final MutinyEmitter<ExternalMessage> serverOutboundEmitter;
    private EventBus eventBus;

    public StompProcessorImpl(FrameParserAdapter frameParserAdapter,
                              StompRegistry stompRegistry,
                              MessageIdGenerator messageIdGenerator,
                              @Channel("serverOutbound")
                              MutinyEmitter<ExternalMessage> serverOutboundEmitter,
                              ConnectFrameHandler connectFrameHandler,
                              PingFrameHandler pingFrameHandler,
                              DisconnectFrameHandler disconnectFrameHandler,
                              SubscribeFrameHandler subscribeFrameHandler,
                              UnsubscribeFrameHandler unsubscribeFrameHandler,
                              SendFrameHandler sendFrameHandler,
                              AckFrameHandler ackFrameHandler,
                              NackFrameHandler nackFrameHandler,
                              EventBus eventBus) {
        this.frameParserAdapter = frameParserAdapter;
        this.stompRegistry = stompRegistry;
        this.messageIdGenerator = messageIdGenerator;
        commandFrameHandlerMap.put(Command.CONNECT, connectFrameHandler);
        commandFrameHandlerMap.put(Command.PING, pingFrameHandler);
        commandFrameHandlerMap.put(Command.DISCONNECT, disconnectFrameHandler);
        commandFrameHandlerMap.put(Command.SUBSCRIBE, subscribeFrameHandler);
        commandFrameHandlerMap.put(Command.UNSUBSCRIBE, unsubscribeFrameHandler);
        commandFrameHandlerMap.put(Command.SEND, sendFrameHandler);
        commandFrameHandlerMap.put(Command.ACK, ackFrameHandler);
        commandFrameHandlerMap.put(Command.NACK, nackFrameHandler);
        this.serverOutboundEmitter = serverOutboundEmitter;
        this.eventBus = eventBus;
    }

    @Produces
    FrameParser frameParser() {
        return new FrameParser();
    }

    @Override
    @Incoming("serverInbound")
    public Uni<Void> processFromClient(ExternalMessage externalMessage) {
        String sessionId = externalMessage.sessionId();
        LOGGER.debug("Received message from client {}", sessionId);

        List<Frame> messages = frameParserAdapter.parse(externalMessage.message());
        var unis = messages.stream().map(frame -> {
            boolean hasActiveSession = stompRegistry.hasActiveSession(sessionId);
            if(Command.CONNECT.equals(frame.getCommand())) {
                if (hasActiveSession) {
                    return serverOutboundEmitter.send(new ExternalMessage(sessionId, FrameUtils.frameToByteArray(Frames.createErrorFrame(
                            "REJECTED",
                            Headers.create("content-type", "text/plain"), "Active connection already exists."))));
                }
            } else {
                if (!hasActiveSession) {
                    return serverOutboundEmitter.send(new ExternalMessage(sessionId, FrameUtils.frameToByteArray(Frames.createErrorFrame(
                            "REJECTED",
                            Headers.create("content-type", "text/plain"), "Active connection doesn't exist."))));
                }
            }
            stompRegistry.updateLastActivity(sessionId);
            FrameHolder frameHolder =  new FrameHolder(sessionId, frame);
            FrameHandler frameHandler = commandFrameHandlerMap.get(frameHolder.frame().getCommand());
            return frameHandler.handle(frameHolder);
        }).toList();

        return Uni.join().all(unis).andFailFast().replaceWithVoid();
    }

    @Override
    @Incoming("brokerOutbound")
    public Uni<Void> processToClient(SendMessage sendMessage) {
        StompRegistry.SessionSubscription sessionSubscription = stompRegistry.getSessionSubscriptionByDestination(sendMessage.getSubscriberId(), sendMessage.getDestination());
        Map<String, String> headers = new HashMap<>();
        String messageId = messageIdGenerator.generate();
        headers.put("subscription", sessionSubscription.subscriptionId());
        headers.put("messageId", messageId);
        headers.put("destination",sendMessage.getDestination());
        String ackId = null;
        if(sessionSubscription.isAckRequired()) {
            ackId = messageId;
            headers.put("ack", messageId);
        }
        Frame frame = new Frame(Command.MESSAGE, Headers.create(headers), Buffer.buffer(sendMessage.getPayload()));
        ExternalMessage externalMessage = new ExternalMessage(sendMessage.getSubscriberId(), frame.toBuffer().getBytes());
        eventBus.publish(StompProcessor.MESSAGE_SEND_DESTINATION, new StompRegistry.SendMessage(sendMessage.getSubscriberId(), sessionSubscription.subscriptionId(), externalMessage, ackId));
        return serverOutboundEmitter.send(externalMessage);
    }

    @ApplicationScoped
    public static class FrameParserAdapter {
        private final FrameParser frameParser;

        public FrameParserAdapter(FrameParser frameParser) {
            this.frameParser = frameParser;
        }

        public List<Frame> parse(byte[] message) {
            List<Frame> frames = new ArrayList<>();
            frameParser.handler(frames::add);
            frameParser.handle(Buffer.buffer(message));
            return frames;
        }
    }
}

package com.github.deepend0.reactivestomp.stompprocessor;

import com.github.deepend0.reactivestomp.message.ExternalMessage;
import com.github.deepend0.reactivestomp.stompprocessor.framehandler.*;
import com.github.deepend0.reactivestomp.simplebroker.model.BrokerMessage;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.MutinyEmitter;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.stomp.Command;
import io.vertx.ext.stomp.Frame;
import io.vertx.ext.stomp.impl.FrameParser;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.*;

@ApplicationScoped
public class StompProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(StompProcessor.class);
    public static final List<String> ACCEPTED_VERSIONS = List.of("1.0", "1.1", "1.2");
    private final FrameParserAdapter frameParserAdapter;
    private final StompRegistry stompRegistry;

    private final Map<Command, FrameHandler> commandFrameHandlerMap = new HashMap<>();

    public StompProcessor(FrameParserAdapter frameParserAdapter,
                          StompRegistry stompRegistry,
                          ConnectFrameHandler connectFrameHandler,
                          DisconnectFrameHandler disconnectFrameHandler,
                          SubscribeFrameHandler subscribeFrameHandler,
                          UnsubscribeFrameHandler unsubscribeFrameHandler,
                          SendFrameHandler sendFrameHandler,
                          AckFrameHandler ackFrameHandler,
                          NackFrameHandler nackFrameHandler) {
        this.frameParserAdapter = frameParserAdapter;
        this.stompRegistry = stompRegistry;
        commandFrameHandlerMap.put(Command.CONNECT, connectFrameHandler);
        commandFrameHandlerMap.put(Command.DISCONNECT, disconnectFrameHandler);
        commandFrameHandlerMap.put(Command.SUBSCRIBE, subscribeFrameHandler);
        commandFrameHandlerMap.put(Command.UNSUBSCRIBE, unsubscribeFrameHandler);
        commandFrameHandlerMap.put(Command.SEND, sendFrameHandler);
        commandFrameHandlerMap.put(Command.ACK, ackFrameHandler);
        commandFrameHandlerMap.put(Command.NACK, nackFrameHandler);
    }

    @Produces
    FrameParser frameParser() {
        return new FrameParser();
    }

    @Incoming("serverInbound")
    @Outgoing("serverInboundStatus")
    public Multi<Void> processFromClient(ExternalMessage externalMessage) {
        String sessionId = externalMessage.sessionId();
        stompRegistry.updateLastActivity(sessionId);
        if (Arrays.equals(externalMessage.message(), "\n".getBytes(StandardCharsets.UTF_8))) {
            return Multi.createFrom().empty();
        }
        List<Frame> messages = frameParserAdapter.parse(externalMessage.message());
        var unis = messages.stream().map(frame -> new FrameHolder(sessionId, frame)).map(frameHolder -> {
            FrameHandler frameHandler = commandFrameHandlerMap.get(frameHolder.frame().getCommand());
            return frameHandler.handle(frameHolder);
        }).toList();

        return Multi.createFrom().iterable(unis)
                .onItem().transformToMultiAndMerge(Uni::toMulti);
    }

    //@Incoming("brokerOutbound")
    public Multi<Message<byte[]>> processToClient(Message<?> message) {
        return null;
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

    public static StompProcessor build(Vertx vertx, MutinyEmitter<ExternalMessage> serverOutboundEmitter, MutinyEmitter<BrokerMessage> brokerInboundEmitter) {
        FrameParser frameParser = new FrameParser();
        FrameParserAdapter frameParserAdapter = new FrameParserAdapter(frameParser);
        StompRegistry stompRegistry = new StompRegistry(vertx, serverOutboundEmitter, brokerInboundEmitter);
        ConnectFrameHandler connectFrameHandler = new ConnectFrameHandler(serverOutboundEmitter, stompRegistry);
        DisconnectFrameHandler disconnectFrameHandler = new DisconnectFrameHandler(serverOutboundEmitter, brokerInboundEmitter, stompRegistry);
        SubscribeFrameHandler subscribeFrameHandler = new SubscribeFrameHandler(stompRegistry, serverOutboundEmitter, brokerInboundEmitter);
        UnsubscribeFrameHandler unsubscribeFrameHandler = new UnsubscribeFrameHandler(stompRegistry, serverOutboundEmitter, brokerInboundEmitter);
        SendFrameHandler sendFrameHandler = new SendFrameHandler(serverOutboundEmitter, brokerInboundEmitter);
        AckFrameHandler ackFrameHandler = new AckFrameHandler(serverOutboundEmitter);
        NackFrameHandler nackFrameHandler = new NackFrameHandler(serverOutboundEmitter);
        return new StompProcessor(
                frameParserAdapter,
                stompRegistry,
                connectFrameHandler,
                disconnectFrameHandler,
                subscribeFrameHandler,
                unsubscribeFrameHandler,
                sendFrameHandler,
                ackFrameHandler,
                nackFrameHandler
        );
    }
}

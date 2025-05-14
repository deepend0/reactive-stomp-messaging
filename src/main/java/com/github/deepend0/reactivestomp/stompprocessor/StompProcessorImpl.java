package com.github.deepend0.reactivestomp.stompprocessor;

import com.github.deepend0.reactivestomp.message.ExternalMessage;
import com.github.deepend0.reactivestomp.simplebroker.model.SendMessage;
import com.github.deepend0.reactivestomp.stompprocessor.framehandler.*;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.MutinyEmitter;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.stomp.Command;
import io.vertx.ext.stomp.Frame;
import io.vertx.ext.stomp.impl.FrameParser;
import io.vertx.ext.stomp.utils.Headers;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.Supplier;

@ApplicationScoped
public class StompProcessorImpl implements StompProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(StompProcessorImpl.class);
    public static final List<String> ACCEPTED_VERSIONS = List.of("1.0", "1.1", "1.2");
    private final FrameParserAdapter frameParserAdapter;
    private final StompRegistry stompRegistry;
    private final MessageIdGenerator messageIdGenerator;

    private final Map<Command, FrameHandler> commandFrameHandlerMap = new HashMap<>();

    private final MutinyEmitter<ExternalMessage> serverOutboundEmitter;

    public StompProcessorImpl(FrameParserAdapter frameParserAdapter,
                              StompRegistry stompRegistry,
                              MessageIdGenerator messageIdGenerator,
                              @Channel("serverOutbound")
                              MutinyEmitter<ExternalMessage> serverOutboundEmitter,
                              ConnectFrameHandler connectFrameHandler,
                              DisconnectFrameHandler disconnectFrameHandler,
                              SubscribeFrameHandler subscribeFrameHandler,
                              UnsubscribeFrameHandler unsubscribeFrameHandler,
                              SendFrameHandler sendFrameHandler,
                              AckFrameHandler ackFrameHandler,
                              NackFrameHandler nackFrameHandler) {
        this.frameParserAdapter = frameParserAdapter;
        this.stompRegistry = stompRegistry;
        this.messageIdGenerator = messageIdGenerator;
        commandFrameHandlerMap.put(Command.CONNECT, connectFrameHandler);
        commandFrameHandlerMap.put(Command.DISCONNECT, disconnectFrameHandler);
        commandFrameHandlerMap.put(Command.SUBSCRIBE, subscribeFrameHandler);
        commandFrameHandlerMap.put(Command.UNSUBSCRIBE, unsubscribeFrameHandler);
        commandFrameHandlerMap.put(Command.SEND, sendFrameHandler);
        commandFrameHandlerMap.put(Command.ACK, ackFrameHandler);
        commandFrameHandlerMap.put(Command.NACK, nackFrameHandler);
        this.serverOutboundEmitter = serverOutboundEmitter;
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

    @Incoming("brokerOutbound")
    @Outgoing("brokerOutboundStatus")
    public Uni<Void> processToClient(SendMessage sendMessage) {
        String subscriptionId = stompRegistry.getSessionSubscriptionByDestination(sendMessage.getSubscriberId(), sendMessage.getDestination()).getSubscriptionId();
        Map<String, String> headers = new HashMap<>();
        headers.put("subscription",subscriptionId);
        headers.put("messageId", messageIdGenerator.generate());
        headers.put("destination",sendMessage.getDestination());
        Frame frame = new Frame(Command.MESSAGE, Headers.create(headers), Buffer.buffer(sendMessage.getPayload()));
        return serverOutboundEmitter.send(new ExternalMessage(sendMessage.getSubscriberId(), frame.toBuffer().getBytes()));
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

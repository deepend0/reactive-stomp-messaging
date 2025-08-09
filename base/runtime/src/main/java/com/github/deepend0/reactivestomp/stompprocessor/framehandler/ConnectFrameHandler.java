package com.github.deepend0.reactivestomp.stompprocessor.framehandler;

import com.github.deepend0.reactivestomp.stompprocessor.StompProcessor;
import com.github.deepend0.reactivestomp.stompprocessor.StompProcessorImpl;
import com.github.deepend0.reactivestomp.stompprocessor.StompRegistry;
import com.github.deepend0.reactivestomp.websocket.ExternalMessage;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.MutinyEmitter;
import io.vertx.ext.stomp.Command;
import io.vertx.ext.stomp.Frame;
import io.vertx.ext.stomp.Frames;
import io.vertx.ext.stomp.impl.FrameParser;
import io.vertx.ext.stomp.utils.Headers;
import io.vertx.ext.stomp.utils.Server;
import io.vertx.mutiny.core.eventbus.EventBus;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@ApplicationScoped
public class ConnectFrameHandler extends FrameHandler {

    @ConfigProperty(name = "reactive-stomp.heartbeat")
    private int [] heartbeatPeriods;

    @Inject
    private EventBus eventBus;

    private Frame.Heartbeat serverHeartbeat;

    public ConnectFrameHandler() {
    }

    public ConnectFrameHandler(MutinyEmitter<ExternalMessage> serverOutboundEmitter) {
        super(serverOutboundEmitter);
    }

    @PostConstruct
    public void postConstruct(){
        serverHeartbeat = new Frame.Heartbeat(heartbeatPeriods[0], heartbeatPeriods[1]);
    }

    @Override
    public Uni<Void> handle(FrameHolder frameHolder) {
        String sessionId = frameHolder.sessionId();
        Frame frame = frameHolder.frame();
        // Server negotiation
        List<String> accepted = new ArrayList<>();
        String accept = frame.getHeader(Frame.ACCEPT_VERSION);
        if (accept == null) {
            accepted.add("1.0");
        } else {
            accepted.addAll(Arrays.asList(accept.split(FrameParser.COMMA)));
        }

        String version = negotiate(accepted);
        if (version == null) {
            return serverOutboundEmitter.send(new ExternalMessage(sessionId, FrameUtils.frameToByteArray(Frames.createErrorFrame(
                    "Incompatible versions",
                    Headers.create(
                            Frame.VERSION, getSupportedVersionsHeaderLine(),
                            Frame.CONTENT_TYPE, "text/plain"),
                    "Client protocol requirement does not mach versions supported by the server. " +
                            "Supported protocol versions are " + getSupportedVersionsHeaderLine()))
            ));
        }
        int ping = (int)  Frame.Heartbeat.computePingPeriod(
                serverHeartbeat,
                Frame.Heartbeat.parse(frame.getHeader(Frame.HEARTBEAT)));
        int pong = (int) Frame.Heartbeat.computePongPeriod(
                serverHeartbeat,
                Frame.Heartbeat.parse(frame.getHeader(Frame.HEARTBEAT)));

        Uni<Void> uniSend = serverOutboundEmitter.send(new ExternalMessage(sessionId, FrameUtils.frameToByteArray(new Frame(Command.CONNECTED, Headers.create(
                Frame.VERSION, version,
                Frame.SERVER, Server.SERVER_NAME,
                Frame.SESSION, sessionId,
                Frame.HEARTBEAT, new Frame.Heartbeat(ping, pong).toString()), null))));

        StompRegistry.ConnectMessage connectMessage = new StompRegistry.ConnectMessage(sessionId, ping, pong);
        eventBus.publish(StompProcessor.CONNECT_EVENT_DESTINATION, connectMessage);

        return uniSend;
    }

    private String getSupportedVersionsHeaderLine() {
        StringBuilder builder = new StringBuilder();
        StompProcessorImpl.ACCEPTED_VERSIONS.stream().forEach(
                v -> builder.append(builder.isEmpty() ? v : FrameParser.COMMA + v));
        return builder.toString();
    }

    private String negotiate(List<String> accepted) {
        List<String> supported = StompProcessorImpl.ACCEPTED_VERSIONS;
        for (String v : supported) {
            if (accepted.contains(v)) {
                return v;
            }
        }
        return null;
    }
}

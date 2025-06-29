package com.github.deepend0.reactivestomp.stompprocessor.framehandler;

import com.github.deepend0.reactivestomp.external.ExternalMessage;
import com.github.deepend0.reactivestomp.messaging.model.Message;
import com.github.deepend0.reactivestomp.messaging.model.SubscribeMessage;
import com.github.deepend0.reactivestomp.stompprocessor.StompRegistry;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.MutinyEmitter;
import io.vertx.ext.stomp.Frame;
import io.vertx.ext.stomp.Frames;
import io.vertx.ext.stomp.utils.Headers;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.reactive.messaging.Channel;

@ApplicationScoped
public class SubscribeFrameHandler extends FrameHandler {

    @Inject
    private StompRegistry stompRegistry;

    @Inject
    @Channel("messagingInbound")
    private MutinyEmitter<Message> messagingInboundEmitter;

    public SubscribeFrameHandler() {
    }

    public SubscribeFrameHandler(StompRegistry stompRegistry, MutinyEmitter<ExternalMessage> serverOutboundEmitter,MutinyEmitter<Message> messagingInboundEmitter) {
        super(serverOutboundEmitter);
        this.stompRegistry = stompRegistry;
        this.messagingInboundEmitter = messagingInboundEmitter;
    }


    @Override
    public Uni<Void> handle(FrameHolder frameHolder) {
        String sessionId = frameHolder.sessionId();
        Frame frame = frameHolder.frame();

        String subscriptionId = frame.getHeader(Frame.ID);
        String destination = frame.getHeader(Frame.DESTINATION);
        if (destination == null || sessionId == null) {
            return serverOutboundEmitter.send(new ExternalMessage(sessionId, FrameUtils.frameToByteArray(Frames.createErrorFrame(
                    "Invalid subscription",
                    Headers.create(
                            frame.getHeaders()), "The 'destination' and 'session' headers must be set"))));
        }

        stompRegistry.addSessionSubscription(new StompRegistry.SessionSubscription(sessionId, subscriptionId, destination));
        Uni<Void> uniSend = messagingInboundEmitter.send(new SubscribeMessage(sessionId, destination));

        Uni<Void> uniReceipt = handleReceipt(sessionId, frame);

        return Uni.join().all(uniSend, uniReceipt).andFailFast().replaceWithVoid();
    }
}

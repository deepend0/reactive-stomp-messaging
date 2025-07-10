package com.github.deepend0.reactivestomp.stompprocessor.framehandler;

import com.github.deepend0.reactivestomp.messaging.model.Message;
import com.github.deepend0.reactivestomp.messaging.model.SubscribeMessage;
import com.github.deepend0.reactivestomp.stompprocessor.StompProcessor;
import com.github.deepend0.reactivestomp.stompprocessor.StompRegistry;
import com.github.deepend0.reactivestomp.websocket.ExternalMessage;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.MutinyEmitter;
import io.vertx.ext.stomp.Frame;
import io.vertx.ext.stomp.Frames;
import io.vertx.ext.stomp.utils.Headers;
import io.vertx.mutiny.core.eventbus.EventBus;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.reactive.messaging.Channel;

@ApplicationScoped
public class SubscribeFrameHandler extends FrameHandler {

    @Inject
    @Channel("messagingInbound")
    private MutinyEmitter<Message> messagingInboundEmitter;

    @Inject
    private EventBus eventBus;

    public SubscribeFrameHandler() {
    }

    public SubscribeFrameHandler(MutinyEmitter<ExternalMessage> serverOutboundEmitter,MutinyEmitter<Message> messagingInboundEmitter) {
        super(serverOutboundEmitter);
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
        SubscribeMessage subscribeMessage = new SubscribeMessage(sessionId, destination);
        Uni<Void> uniSend = messagingInboundEmitter.send(subscribeMessage);
        Uni<Void> uniReceipt = handleReceipt(sessionId, frame);
        eventBus.publish(StompProcessor.SUBSCRIBE_EVENT_DESTINATION, new StompRegistry.SessionSubscription(sessionId, subscriptionId, destination));

        return Uni.join().all(uniSend, uniReceipt).andFailFast().replaceWithVoid();
    }
}

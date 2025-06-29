package com.github.deepend0.reactivestomp.stompprocessor.framehandler;

import com.github.deepend0.reactivestomp.external.ExternalMessage;
import com.github.deepend0.reactivestomp.messaging.model.Message;
import com.github.deepend0.reactivestomp.messaging.model.UnsubscribeMessage;
import com.github.deepend0.reactivestomp.stompprocessor.StompRegistry;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.MutinyEmitter;
import io.vertx.ext.stomp.Frame;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.reactive.messaging.Channel;

@ApplicationScoped
public class UnsubscribeFrameHandler extends FrameHandler {

    @Inject
    private StompRegistry stompRegistry;

    @Inject
    @Channel("messagingInbound")
    private MutinyEmitter<Message> messagingInboundEmitter;

    public UnsubscribeFrameHandler() {
    }

    public UnsubscribeFrameHandler(StompRegistry stompRegistry, MutinyEmitter<ExternalMessage> serverOutboundEmitter, MutinyEmitter<Message> messagingInboundEmitter) {
        super(serverOutboundEmitter);
        this.stompRegistry = stompRegistry;
        this.messagingInboundEmitter = messagingInboundEmitter;
    }

    @Override
    public Uni<Void> handle(FrameHolder frameHolder) {
        String sessionId = frameHolder.sessionId();
        Frame frame = frameHolder.frame();
        final String subscriptionId = frame.getHeader(Frame.ID);

        StompRegistry.SessionSubscription sessionSubscription = stompRegistry.getSessionSubscriptionBySubscription(sessionId, subscriptionId);
        stompRegistry.deleteSessionSubscription(sessionSubscription);
        Uni<Void> uniSend = messagingInboundEmitter.send(new UnsubscribeMessage(sessionId, sessionSubscription.getDestination()));

        Uni<Void> uniReceipt = handleReceipt(sessionId, frame);

        return Uni.join().all(uniSend, uniReceipt).andFailFast().replaceWithVoid();
    }

}

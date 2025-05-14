package com.github.deepend0.reactivestomp.stompprocessor.framehandler;

import com.github.deepend0.reactivestomp.message.ExternalMessage;
import com.github.deepend0.reactivestomp.simplebroker.model.BrokerMessage;
import com.github.deepend0.reactivestomp.simplebroker.model.UnsubscribeMessage;
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
    @Channel("brokerInbound")
    private MutinyEmitter<BrokerMessage> brokerInboundEmitter;

    public UnsubscribeFrameHandler() {
    }

    public UnsubscribeFrameHandler(StompRegistry stompRegistry, MutinyEmitter<ExternalMessage> serverOutboundEmitter, MutinyEmitter<BrokerMessage> brokerInboundEmitter) {
        super(serverOutboundEmitter);
        this.stompRegistry = stompRegistry;
        this.brokerInboundEmitter = brokerInboundEmitter;
    }

    @Override
    public Uni<Void> handle(FrameHolder frameHolder) {
        String sessionId = frameHolder.sessionId();
        Frame frame = frameHolder.frame();
        final String subscriptionId = frame.getHeader(Frame.ID);

        StompRegistry.SessionSubscription sessionSubscription = stompRegistry.getSessionSubscription(new StompRegistry.SessionSubscription(sessionId, subscriptionId, null));
        stompRegistry.deleteSessionSubscription(sessionSubscription);
        Uni<Void> uniSend = brokerInboundEmitter.send(new UnsubscribeMessage(sessionId, sessionSubscription.getDestination()));

        Uni<Void> uniReceipt = handleReceipt(sessionId, frame);

        return Uni.join().all(uniSend, uniReceipt).andFailFast().replaceWithVoid();
    }

}

package com.github.deepend0.reactivestomp.stompprocessor.framehandler;

import com.github.deepend0.reactivestomp.stompprocessor.StompProcessor;
import com.github.deepend0.reactivestomp.stompprocessor.StompRegistry;
import com.github.deepend0.reactivestomp.websocket.ExternalMessage;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.MutinyEmitter;
import io.vertx.ext.stomp.Frame;
import io.vertx.mutiny.core.eventbus.EventBus;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class AckFrameHandler extends FrameHandler {
    @Inject
    private EventBus eventBus;

    public AckFrameHandler() {
    }

    public AckFrameHandler(MutinyEmitter<ExternalMessage> serverOutboundEmitter) {
        super(serverOutboundEmitter);
    }

    @Override
    public Uni<Void> handle(FrameHolder frameHolder) {String sessionId = frameHolder.sessionId();
        Frame frame = frameHolder.frame();
        String ackId = frame.getHeader(Frame.ID);
        eventBus.publish(StompProcessor.MESSAGE_ACK_DESTINATION, new StompRegistry.AckMessage(frameHolder.sessionId(), ackId, true));
        return Uni.createFrom().voidItem();
    }
}

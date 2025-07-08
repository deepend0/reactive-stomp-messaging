package com.github.deepend0.reactivestomp.stompprocessor.framehandler;

import com.github.deepend0.reactivestomp.messaging.model.DisconnectMessage;
import com.github.deepend0.reactivestomp.messaging.model.Message;
import com.github.deepend0.reactivestomp.stompprocessor.StompProcessor;
import com.github.deepend0.reactivestomp.websocket.ExternalMessage;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.MutinyEmitter;
import io.vertx.ext.stomp.Frame;
import io.vertx.mutiny.core.eventbus.EventBus;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.reactive.messaging.Channel;

@ApplicationScoped
public class DisconnectFrameHandler extends FrameHandler {
    @Inject
    @Channel("messagingInbound")
    private MutinyEmitter<Message> messagingInboundEmitter;

    @Inject
    private EventBus eventBus;

    public DisconnectFrameHandler() {
    }

    public DisconnectFrameHandler(MutinyEmitter<ExternalMessage> serverOutboundEmitter, MutinyEmitter<Message> messagingInboundEmitter) {
        super(serverOutboundEmitter);
        this.messagingInboundEmitter = messagingInboundEmitter;
    }

    @Override
    public Uni<Void> handle(FrameHolder frameHolder) {
        String sessionId = frameHolder.sessionId();
        Frame frame = frameHolder.frame();
        Uni<Void> uniSend = messagingInboundEmitter.send(new DisconnectMessage(sessionId));
        Uni<Void> uniReceipt = handleReceipt(sessionId, frame);
        eventBus.publish(StompProcessor.DISCONNECT_EVENT_DESTINATION, sessionId);
        return Uni.join().all(uniSend, uniReceipt).andFailFast().replaceWithVoid();
    }
}

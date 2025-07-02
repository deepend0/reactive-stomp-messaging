package com.github.deepend0.reactivestomp.stompprocessor.framehandler;

import com.github.deepend0.reactivestomp.websocket.ExternalMessage;
import com.github.deepend0.reactivestomp.messaging.model.Message;
import com.github.deepend0.reactivestomp.messaging.model.DisconnectMessage;
import com.github.deepend0.reactivestomp.stompprocessor.StompRegistry;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.MutinyEmitter;
import io.vertx.ext.stomp.Frame;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.reactive.messaging.Channel;

@ApplicationScoped
public class DisconnectFrameHandler extends FrameHandler {
    @Inject
    @Channel("messagingInbound")
    private MutinyEmitter<Message> messagingInboundEmitter;

    @Inject
    private StompRegistry stompRegistry;

    public DisconnectFrameHandler() {
    }

    public DisconnectFrameHandler(MutinyEmitter<ExternalMessage> serverOutboundEmitter, MutinyEmitter<Message> messagingInboundEmitter, StompRegistry stompRegistry) {
        super(serverOutboundEmitter);
        this.messagingInboundEmitter = messagingInboundEmitter;
        this.stompRegistry = stompRegistry;
    }

    @Override
    public Uni<Void> handle(FrameHolder frameHolder) {
        String sessionId = frameHolder.sessionId();
        Frame frame = frameHolder.frame();
        Uni<Void> uniSend = messagingInboundEmitter.send(new DisconnectMessage(sessionId));
        Uni<Void> uniReceipt = handleReceipt(sessionId, frame);
        stompRegistry.disconnect(sessionId);
        return Uni.join().all(uniSend, uniReceipt).andFailFast().replaceWithVoid();
    }
}

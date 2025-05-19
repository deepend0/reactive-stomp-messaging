package com.github.deepend0.reactivestomp.stompprocessor.framehandler;

import com.github.deepend0.reactivestomp.external.ExternalMessage;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.MutinyEmitter;
import io.vertx.ext.stomp.Frames;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class AckFrameHandler extends FrameHandler {

    public AckFrameHandler() {
    }

    public AckFrameHandler(MutinyEmitter<ExternalMessage> serverOutboundEmitter) {
        super(serverOutboundEmitter);
    }

    @Override
    public Uni<Void> handle(FrameHolder frameHolder) {
        return serverOutboundEmitter.send(new ExternalMessage(frameHolder.sessionId(), frameToByteArray(Frames.createErrorFrame(
                "Not Supported",
                null, "Not Supported"))));
    }
}

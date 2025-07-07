package com.github.deepend0.reactivestomp.stompprocessor.framehandler;

import com.github.deepend0.reactivestomp.websocket.ExternalMessage;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.MutinyEmitter;
import io.vertx.ext.stomp.Frames;
import jakarta.enterprise.context.ApplicationScoped;

// TODO
@ApplicationScoped
public class NackFrameHandler extends FrameHandler {

    public NackFrameHandler() {
    }

    public NackFrameHandler(MutinyEmitter<ExternalMessage> serverOutboundEmitter) {
        super(serverOutboundEmitter);
    }

    @Override
    public Uni<Void> handle(FrameHolder frameHolder) {
        return serverOutboundEmitter.send(new ExternalMessage(frameHolder.sessionId(), FrameUtils.frameToByteArray(Frames.createErrorFrame(
                "Not Supported",
                null, "Not Supported"))));
    }
}

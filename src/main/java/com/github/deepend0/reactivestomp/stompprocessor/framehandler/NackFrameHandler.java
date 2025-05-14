package com.github.deepend0.reactivestomp.stompprocessor.framehandler;

import com.github.deepend0.reactivestomp.message.ExternalMessage;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.MutinyEmitter;
import io.vertx.ext.stomp.Frames;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.reactive.messaging.Channel;

@ApplicationScoped
public class NackFrameHandler extends FrameHandler {

    public NackFrameHandler() {
    }

    public NackFrameHandler(MutinyEmitter<ExternalMessage> serverOutboundEmitter) {
        super(serverOutboundEmitter);
    }

    @Override
    public Uni<Void> handle(FrameHolder frameHolder) {
        return serverOutboundEmitter.send(new ExternalMessage(frameHolder.sessionId(), frameToByteArray(Frames.createErrorFrame(
                "Not Supported",
                null, "Not Supported"))));
    }
}

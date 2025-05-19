package com.github.deepend0.reactivestomp.stompprocessor.framehandler;

import com.github.deepend0.reactivestomp.external.ExternalMessage;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.MutinyEmitter;
import io.vertx.ext.stomp.Frame;
import io.vertx.ext.stomp.utils.Headers;
import jakarta.inject.Inject;
import org.eclipse.microprofile.reactive.messaging.Channel;

import static io.vertx.ext.stomp.Frames.createReceiptFrame;

public abstract class FrameHandler {
    @Inject
    @Channel("serverOutbound")
    protected MutinyEmitter<ExternalMessage> serverOutboundEmitter;

    public FrameHandler() {
    }

    public FrameHandler(MutinyEmitter<ExternalMessage> serverOutboundEmitter) {
        this.serverOutboundEmitter = serverOutboundEmitter;
    }

    public abstract Uni<Void> handle(FrameHolder frameHolder);

    public Uni<Void> handleReceipt(String sessionId, Frame frame) {
        String receipt = frame.getReceipt();
        if (receipt != null) {
            return serverOutboundEmitter.send(new ExternalMessage(sessionId, createReceiptFrame(receipt, Headers.create()).toBuffer().getBytes()));
        }
        return Uni.createFrom().voidItem();
    }

    public static byte [] frameToByteArray(Frame frame) {
        return frame.toBuffer().getBytes();
    }
}

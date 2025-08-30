package com.github.deepend0.reactivestomp.stompprocessor.framehandler;

import com.github.deepend0.reactivestomp.messaging.model.Message;
import com.github.deepend0.reactivestomp.messaging.model.SendMessage;
import com.github.deepend0.reactivestomp.websocket.ExternalMessage;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.MutinyEmitter;
import io.vertx.ext.stomp.Frame;
import io.vertx.ext.stomp.Frames;
import io.vertx.ext.stomp.utils.Headers;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.reactive.messaging.Channel;

@ApplicationScoped
public class SendFrameHandler extends FrameHandler {

    @Inject
    @Channel("messagingInbound")
    private MutinyEmitter<Message> messagingInboundEmitter;

    public SendFrameHandler() {
    }

    public SendFrameHandler(MutinyEmitter<ExternalMessage> serverOutboundEmitter, MutinyEmitter<Message> messagingInboundEmitter) {
        super(serverOutboundEmitter);
        this.messagingInboundEmitter = messagingInboundEmitter;
    }

    @Override
    public Uni<Void> handle(FrameHolder frameHolder) {
        String sessionId = frameHolder.sessionId();
        Frame frame = frameHolder.frame();
        String destination = frame.getHeader(Frame.DESTINATION);
        if (destination == null) {
            return serverOutboundEmitter.send(new ExternalMessage(sessionId, FrameUtils.frameToByteArray(Frames.createErrorFrame(
                    "Destination header missing",
                    Headers.create(frame.getHeaders()), "Invalid send frame - the " +
                            "'destination' must be set"))));
        }

        String txId = frame.getHeader(Frame.TRANSACTION);
        if (txId != null) {
            return serverOutboundEmitter.send(new ExternalMessage(sessionId, FrameUtils.frameToByteArray(Frames.createErrorFrame(
                    "No transaction support",
                    Headers.create(frame.getHeaders()), "No transaction support"))));
        }

        Uni<Void> uniSend = messagingInboundEmitter.send(new SendMessage(sessionId, destination, frame.getBodyAsByteArray()));

        Uni<Void> uniReceipt = handleReceipt(sessionId, frame);

        return Uni.join().all(uniSend, uniReceipt).andFailFast().replaceWithVoid();
    }
}

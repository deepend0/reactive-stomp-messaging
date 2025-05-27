package com.github.deepend0.reactivestomp.stompprocessor.framehandler;

import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class PingFrameHandler extends FrameHandler{
    private static final Logger LOGGER = LoggerFactory.getLogger(PingFrameHandler.class);

    @Override
    public Uni<Void> handle(FrameHolder frameHolder) {
            LOGGER.debug("Received heartbeat from client {}", frameHolder.sessionId());
            return Uni.createFrom().voidItem();
    }
}

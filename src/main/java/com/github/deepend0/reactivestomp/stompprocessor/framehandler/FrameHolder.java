package com.github.deepend0.reactivestomp.stompprocessor.framehandler;

import io.vertx.ext.stomp.Frame;

public record FrameHolder(String sessionId, Frame frame) {

}

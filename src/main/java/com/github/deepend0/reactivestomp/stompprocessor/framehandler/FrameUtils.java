package com.github.deepend0.reactivestomp.stompprocessor.framehandler;

import io.vertx.ext.stomp.Frame;

public class FrameUtils {
    public static byte [] frameToByteArray(Frame frame) {
        return frame.toBuffer().getBytes();
    }
}

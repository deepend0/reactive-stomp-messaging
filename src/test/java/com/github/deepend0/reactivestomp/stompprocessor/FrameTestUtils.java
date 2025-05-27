package com.github.deepend0.reactivestomp.stompprocessor;

import java.nio.charset.StandardCharsets;

public class FrameTestUtils {

    private static byte[] toBytes(String formattedFrame) {
        return formattedFrame.getBytes(StandardCharsets.UTF_8);
    }

    public static byte[] connectFrame(String host, String heartBeat) {
        String frame = """
            CONNECT
            accept-version:1.2
            host:%s
            heart-beat:%s

            \u0000""";
        return toBytes(String.format(frame, host, heartBeat));
    }

    public static byte[] connectedFrame(String sessionId, String heartBeat) {
        String frame = """
            CONNECTED
            server:vertx-stomp/4.5.13
            heart-beat:%s
            session:%s
            version:1.2

            \u0000""";
        return toBytes(String.format(frame, heartBeat, sessionId));
    }

    public static byte[] subscribeFrame(String id, String destination, String receipt) {
        String frame = """
            SUBSCRIBE
            id:%s
            destination:%s
            receipt:%s

            \u0000""";
        return toBytes(String.format(frame, id, destination, receipt));
    }

    public static byte[] unsubscribeFrame(String id, String receipt) {
        String frame = """
            UNSUBSCRIBE
            id:%s
            receipt:%s

            \u0000""";
        return toBytes(String.format(frame, id, receipt));
    }

    public static byte[] receiptFrame(String receiptId) {
        String frame = """
            RECEIPT
            receipt-id:%s

            \u0000""";
        return toBytes(String.format(frame, receiptId));
    }

    public static byte[] sendFrame(String destination, String contentType, String receipt, String payload) {
        String frame = """
            SEND
            destination:%s
            content-type:%s
            receipt:%s

            %s\u0000""";
        return toBytes(String.format(frame, destination, contentType, receipt, payload));
    }

    public static byte[] messageFrame(String destination, String messageId, String subscriptionId, String body) {
        String frame = """
            MESSAGE
            destination:%s
            messageId:%s
            subscription:%s
            
            %s\u0000""";
        return toBytes(String.format(frame, destination, messageId, subscriptionId, body));
    }

    public static byte[] disconnectFrame(String receipt) {
        String frame = """
            DISCONNECT
            receipt:%s

            \u0000""";
        return toBytes(String.format(frame, receipt));
    }
}

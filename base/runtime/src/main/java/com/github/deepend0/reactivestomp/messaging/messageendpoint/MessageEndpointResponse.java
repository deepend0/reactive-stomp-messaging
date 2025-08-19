package com.github.deepend0.reactivestomp.messaging.messageendpoint;

public record MessageEndpointResponse<O> (
        String outboundDestination,
        O value
) {
    public static <O> MessageEndpointResponse<O> of(String outboundDestination, O value) {
        return new MessageEndpointResponse<>(outboundDestination, value);
    }
}
package com.github.deepend0.reactivestomp.messageendpoint;

import jakarta.enterprise.context.ApplicationScoped;

import java.util.List;

@ApplicationScoped
public class DummyMessageEndpointRegistry implements MessageEndpointRegistry {
    @Override
    public List<MessageEndpointMethodWrapper<?, ?>> getMessageEndpoints(String destination) {
        // Return an empty list as a dummy implementation
        return List.of();
    }
}

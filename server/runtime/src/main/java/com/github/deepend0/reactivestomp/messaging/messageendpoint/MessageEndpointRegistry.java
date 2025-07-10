package com.github.deepend0.reactivestomp.messaging.messageendpoint;

import java.util.List;

public interface MessageEndpointRegistry {
    List<MessageEndpointMethodWrapper<?,?>> getMessageEndpoints(String destination);
}

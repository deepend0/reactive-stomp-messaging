package com.github.deepend0.reactivestomp.messageendpoint;

import java.util.List;

public interface MessageEndpointRegistry {
    List<MessageEndpointMethodWrapper<?,?>> getMessageEndpoints(String destination);
}

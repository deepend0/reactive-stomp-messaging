package com.github.deepend0.reactivestomp.messaging.messageendpoint;

public interface MessageEndpointRegistry {
    PathHandlerRouter.MatchResult<MessageEndpointMethodWrapper<?,?>> getMessageEndpoints(String destinationPath);
}

package com.github.deepend0.reactivestomp.test.stompprocessor;

import com.github.deepend0.reactivestomp.messaging.messagehandler.MessagingMessageHandler;
import io.quarkus.test.junit.QuarkusTestProfile;

import java.util.Map;

public class StompProcessorTestProfile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
        return Map.of("quarkus.arc.exclude-types", MessagingMessageHandler.class.getName());
    }
}

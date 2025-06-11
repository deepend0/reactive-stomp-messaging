package com.github.deepend0.reactivestomp.stompprocessor;

import io.quarkus.test.junit.QuarkusTestProfile;

import java.util.Map;

public class StompProcessorTestProfile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
        return Map.of("quarkus.arc.exclude-types", "com.github.deepend0.reactivestomp.simplebroker.messagehandler.BrokerMessageHandler");
    }
}

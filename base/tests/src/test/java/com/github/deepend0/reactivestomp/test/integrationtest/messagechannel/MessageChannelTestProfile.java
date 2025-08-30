package com.github.deepend0.reactivestomp.test.integrationtest.messagechannel;

import com.github.deepend0.reactivestomp.websocket.ConnectionRegistry;
import com.github.deepend0.reactivestomp.websocket.StompWebSocketServer;
import io.quarkus.test.junit.QuarkusTestProfile;

import java.util.Map;

public class MessageChannelTestProfile implements QuarkusTestProfile {
  @Override
  public Map<String, String> getConfigOverrides() {
    return Map.of(
        "quarkus.arc.exclude-types",
        String.join(
            ",",
            StompWebSocketServer.class.getName(),
            ConnectionRegistry.class.getName()));
  }
}

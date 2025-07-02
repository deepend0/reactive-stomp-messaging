package com.github.deepend0.reactivestomp.test.broker;

import com.github.deepend0.reactivestomp.websocket.StompWebSocketServer;
import com.github.deepend0.reactivestomp.websocket.ConnectionRegistry;
import io.quarkus.test.junit.QuarkusTestProfile;
import java.util.Map;

public class BrokerComponentTestProfile implements QuarkusTestProfile {
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

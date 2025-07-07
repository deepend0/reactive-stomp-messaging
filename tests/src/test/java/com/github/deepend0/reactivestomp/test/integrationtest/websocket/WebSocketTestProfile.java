package com.github.deepend0.reactivestomp.test.integrationtest.websocket;

import com.github.deepend0.reactivestomp.test.integrationtest.messagechannel.MessageChannelTestConfig;
import io.quarkus.test.junit.QuarkusTestProfile;

import java.util.Map;

public class WebSocketTestProfile implements QuarkusTestProfile {
  @Override
  public Map<String, String> getConfigOverrides() {
    return Map.of(
        "quarkus.arc.exclude-types",
        String.join(
            ",",
            MessageChannelTestConfig.class.getName()));
  }
}

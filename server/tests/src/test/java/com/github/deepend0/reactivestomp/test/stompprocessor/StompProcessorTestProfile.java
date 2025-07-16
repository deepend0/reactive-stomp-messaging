package com.github.deepend0.reactivestomp.test.stompprocessor;

import com.github.deepend0.reactivestomp.messaging.messagehandler.BrokerMessageHandler;
import com.github.deepend0.reactivestomp.messaging.messagehandler.MessageEndpointMessageHandler;
import com.github.deepend0.reactivestomp.messaging.messagehandler.MessagingMessageHandler;
import com.github.deepend0.reactivestomp.test.integrationtest.messagechannel.MessageChannelTestConfig;
import com.github.deepend0.reactivestomp.websocket.StompWebSocketServer;
import com.github.deepend0.reactivestomp.websocket.ConnectionRegistry;
import io.quarkus.test.junit.QuarkusTestProfile;

import java.util.Map;

public class StompProcessorTestProfile implements QuarkusTestProfile {
  @Override
  public Map<String, String> getConfigOverrides() {
    return Map.of(
        "quarkus.arc.exclude-types",
        String.join(
            ",",
            BrokerMessageHandler.class.getName(),
            MessageEndpointMessageHandler.class.getName(),
            MessagingMessageHandler.class.getName(),
            StompWebSocketServer.class.getName(),
            ConnectionRegistry.class.getName(),
            MessageChannelTestConfig.class.getName()));
  }
}

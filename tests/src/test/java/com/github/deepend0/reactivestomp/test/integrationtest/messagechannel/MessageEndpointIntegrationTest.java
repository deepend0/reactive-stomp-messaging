package com.github.deepend0.reactivestomp.test.integrationtest.messagechannel;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.deepend0.reactivestomp.websocket.ExternalMessage;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.smallrye.reactive.messaging.MutinyEmitter;
import jakarta.inject.Inject;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;

@QuarkusTest
@TestProfile(MessageChannelTestProfile.class)
public class MessageEndpointIntegrationTest {
    @Inject
    @Channel("serverInbound")
    private MutinyEmitter<ExternalMessage> serverInboundEmitter;

    @Inject
    private MessageChannelTestConfig messageChannelTestConfig;

    private MessageChannelITUtils messageChannelITUtils;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @BeforeEach
    public void init() {
        messageChannelTestConfig.resetServerOutboundList();
        messageChannelTestConfig.resetServerOutboundHeartbeats();
        messageChannelITUtils = new MessageChannelITUtils(serverInboundEmitter, messageChannelTestConfig.getServerOutboundList(), messageChannelTestConfig.getServerOutboundHeartbeats());
    }

    @Test
    public void uniMessageEndpointShouldHandleIncomingMessage() {
        String session1 = "session1";
        String session2 = "session2";
        String session3 = "session3";

        String subscription2 = "sub2";
        String subscription3 = "sub3";

        String sendDestination = "/messageEndpoint/helloAsync";
        String subscribeDestination = "/topic/helloAsync";

        long timer1 = messageChannelITUtils.connectClient(session1);
        long timer2 = messageChannelITUtils.connectClient(session2);
        long timer3 = messageChannelITUtils.connectClient(session3);

        messageChannelITUtils.subscribeClient(session2, subscription2, subscribeDestination, "1002");
        messageChannelITUtils.subscribeClient(session3, subscription3, subscribeDestination, "1003");

        String receivedMessage = "\"Hello World\"";
        CompletableFuture<Void> cf1 = CompletableFuture.runAsync(()-> messageChannelITUtils.sendMessage(session1, sendDestination, "\"World\"", "1004"));
        CompletableFuture<Void> cf3 = CompletableFuture.runAsync(()-> messageChannelITUtils.receiveMessage(session2, subscription2, subscribeDestination, receivedMessage));
        CompletableFuture<Void> cf4 = CompletableFuture.runAsync(()-> messageChannelITUtils.receiveMessage(session3, subscription3, subscribeDestination, receivedMessage));
        CompletableFuture.allOf(cf1, cf3, cf4).join();

        messageChannelITUtils.disconnectClient(session1, timer1, "1005");
        messageChannelITUtils.disconnectClient(session2, timer2, "1006");
        messageChannelITUtils.disconnectClient(session3, timer3, "1007");
    }

    @Test
    public void multiMessageEndpointShouldHandleIncomingMessage() {
        String session4 = "session4";
        String session5 = "session5";
        String session6 = "session6";

        String subscription5 = "sub5";
        String subscription6 = "sub6";

        String sendDestination = "/messageEndpoint/intSeries";
        String subscribeDestination = "/topic/intSeries";
        int value = 10;

        long timer4 = messageChannelITUtils.connectClient(session4);
        long timer5 = messageChannelITUtils.connectClient(session5);
        long timer6 = messageChannelITUtils.connectClient(session6);

        messageChannelITUtils.subscribeClient(session5, subscription5, subscribeDestination, "2005");
        messageChannelITUtils.subscribeClient(session6, subscription6, subscribeDestination, "2006");

        CompletableFuture<Void> cf1 = CompletableFuture.runAsync(()-> messageChannelITUtils.sendMessage(session4, sendDestination, String.valueOf(value), "2007"));
        CompletableFuture<Void> cf3 = CompletableFuture.runAsync(()->
                IntStream.range(value + 1, value + 11).boxed()
                        .forEach( i -> messageChannelITUtils.receiveMessage(session5, subscription5, subscribeDestination, String.valueOf(i))));
        CompletableFuture<Void> cf4 = CompletableFuture.runAsync(()->
                IntStream.range(value + 1, value + 11).boxed()
                        .forEach( i -> messageChannelITUtils.receiveMessage(session6, subscription6, subscribeDestination, String.valueOf(i))));
        CompletableFuture.allOf(cf1, cf3, cf4).join();

        messageChannelITUtils.disconnectClient(session4, timer4, "2007");
        messageChannelITUtils.disconnectClient(session5, timer5, "2008");
        messageChannelITUtils.disconnectClient(session6, timer6, "2009");
    }

    @Test
    public void syncMessageEndpointShouldHandleIncomingMessage() {
        String session7 = "session7";
        String session8 = "session8";
        String session9 = "session9";

        String subscription2 = "sub8";
        String subscription3 = "sub9";

        String sendDestination = "/messageEndpoint/helloSync";
        String subscribeDestination = "/topic/helloSync";

        long timer1 = messageChannelITUtils.connectClient(session7);
        long timer2 = messageChannelITUtils.connectClient(session8);
        long timer3 = messageChannelITUtils.connectClient(session9);

        messageChannelITUtils.subscribeClient(session8, subscription2, subscribeDestination, "3008");
        messageChannelITUtils.subscribeClient(session9, subscription3, subscribeDestination, "3009");

        String receivedMessage = "\"Bonjour World\"";
        CompletableFuture<Void> cf1 = CompletableFuture.runAsync(()-> messageChannelITUtils.sendMessage(session7, sendDestination, "\"World\"", "3010"));
        CompletableFuture<Void> cf3 = CompletableFuture.runAsync(()-> messageChannelITUtils.receiveMessage(session8, subscription2, subscribeDestination, receivedMessage));
        CompletableFuture<Void> cf4 = CompletableFuture.runAsync(()-> messageChannelITUtils.receiveMessage(session9, subscription3, subscribeDestination, receivedMessage));
        CompletableFuture.allOf(cf1, cf3, cf4).join();

        messageChannelITUtils.disconnectClient(session7, timer1, "3011");
        messageChannelITUtils.disconnectClient(session8, timer2, "3012");
        messageChannelITUtils.disconnectClient(session9, timer3, "3013");
    }

    @Test
    public void messageEndpointShouldHandleNumberIncomingMessage() {
        String session10 = "session10";
        String session11 = "session11";
        String session12 = "session12";

        String subscription2 = "sub11";
        String subscription3 = "sub12";

        String sendDestination = "/messageEndpoint/intValue";
        String subscribeDestination = "/topic/intValue";

        long timer1 = messageChannelITUtils.connectClient(session10);
        long timer2 = messageChannelITUtils.connectClient(session11);
        long timer3 = messageChannelITUtils.connectClient(session12);

        messageChannelITUtils.subscribeClient(session11, subscription2, subscribeDestination, "4001");
        messageChannelITUtils.subscribeClient(session12, subscription3, subscribeDestination, "4002");

        String message = "5";
        String receivedMessage = "6";
        CompletableFuture<Void> cf1 = CompletableFuture.runAsync(()-> messageChannelITUtils.sendMessage(session10, sendDestination, message, "4003"));
        CompletableFuture<Void> cf3 = CompletableFuture.runAsync(()-> messageChannelITUtils.receiveMessage(session11, subscription2, subscribeDestination, receivedMessage));
        CompletableFuture<Void> cf4 = CompletableFuture.runAsync(()-> messageChannelITUtils.receiveMessage(session12, subscription3, subscribeDestination, receivedMessage));
        CompletableFuture.allOf(cf1, cf3, cf4).join();

        messageChannelITUtils.disconnectClient(session10, timer1, "4004");
        messageChannelITUtils.disconnectClient(session11, timer2, "4005");
        messageChannelITUtils.disconnectClient(session12, timer3, "4006");
    }

    @Test
    public void messageEndpointShouldHandleObjectIncomingMessage() {
        String session10 = "session10";
        String session11 = "session11";
        String session12 = "session12";

        String subscription2 = "sub11";
        String subscription3 = "sub12";

        String sendDestination = "/messageEndpoint/obj";
        String subscribeDestination = "/topic/obj";

        long timer1 = messageChannelITUtils.connectClient(session10);
        long timer2 = messageChannelITUtils.connectClient(session11);
        long timer3 = messageChannelITUtils.connectClient(session12);

        messageChannelITUtils.subscribeClient(session11, subscription2, subscribeDestination, "4001");
        messageChannelITUtils.subscribeClient(session12, subscription3, subscribeDestination, "4002");

        String message = "{\"id\":\"12345\",\"value\":5}";
        //Regex escape for { and }
        String receivedMessage = "\\{\"id\":\"AA12345\",\"value\":6\\}";
        CompletableFuture<Void> cf1 = CompletableFuture.runAsync(()-> messageChannelITUtils.sendMessage(session10, sendDestination, message, "4003"));
        CompletableFuture<Void> cf3 = CompletableFuture.runAsync(()-> messageChannelITUtils.receiveMessage(session11, subscription2, subscribeDestination, receivedMessage));
        CompletableFuture<Void> cf4 = CompletableFuture.runAsync(()-> messageChannelITUtils.receiveMessage(session12, subscription3, subscribeDestination, receivedMessage));
        CompletableFuture.allOf(cf1, cf3, cf4).join();

        messageChannelITUtils.disconnectClient(session10, timer1, "4004");
        messageChannelITUtils.disconnectClient(session11, timer2, "4005");
        messageChannelITUtils.disconnectClient(session12, timer3, "4006");
    }
}

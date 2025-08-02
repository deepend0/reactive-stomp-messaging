package com.github.deepend0.reactivestomp.test.integrationtest.messagechannel;

import com.github.deepend0.reactivestomp.websocket.ExternalMessage;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.smallrye.reactive.messaging.MutinyEmitter;
import jakarta.inject.Inject;

import java.util.concurrent.CompletableFuture;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(MessageChannelTestProfile.class)
public class BrokerIntegrationTest {
    @Inject
    @Channel("serverInbound")
    private MutinyEmitter<ExternalMessage> serverInboundEmitter;

    @Inject
    private MessageChannelTestConfig messageChannelTestConfig;

    private MessageChannelITUtils messageChannelITUtils;

    @BeforeEach
    public void init() {
        messageChannelTestConfig.resetServerOutboundList();
        messageChannelTestConfig.resetServerOutboundHeartbeats();
        messageChannelITUtils = new MessageChannelITUtils(serverInboundEmitter, messageChannelTestConfig.getServerOutboundList(), messageChannelTestConfig.getServerOutboundHeartbeats());
    }

    @Test
    public void shouldSendMessageToMultipleSubscribers() {
        String session1 = "session1";
        String session2 = "session2";
        String session3 = "session3";

        String subscription1 = "sub1";
        String subscription2 = "sub2";
        String subscription3 = "sub3";

        String destination = "/topic/chat";
        String message = "Hello World!";

        long timer1 = messageChannelITUtils.connectClient(session1);
        long timer2 = messageChannelITUtils.connectClient(session2);
        long timer3 = messageChannelITUtils.connectClient(session3);

        messageChannelITUtils.subscribeClient(session1, subscription1, destination, "1001");
        messageChannelITUtils.subscribeClient(session2, subscription2, destination, "1002");
        messageChannelITUtils.subscribeClient(session3, subscription3, destination, "1003");

        CompletableFuture<Void> cf1 = CompletableFuture.runAsync(()-> messageChannelITUtils.sendMessage(session1, destination, message, "1004"));
        CompletableFuture<Void> cf2 = CompletableFuture.runAsync(()-> messageChannelITUtils.receiveMessage(session1, subscription1, destination, message));
        CompletableFuture<Void> cf3 = CompletableFuture.runAsync(()-> messageChannelITUtils.receiveMessage(session2, subscription2, destination, message));
        CompletableFuture<Void> cf4 = CompletableFuture.runAsync(()-> messageChannelITUtils.receiveMessage(session3, subscription3, destination, message));
        CompletableFuture.allOf(cf1, cf2, cf3, cf4).join();

        messageChannelITUtils.disconnectClient(session1, timer1, "1005");
        messageChannelITUtils.disconnectClient(session2, timer2, "1006");
        messageChannelITUtils.disconnectClient(session3, timer3, "1007");
    }

    @Test
    public void shouldSendMessageToSubscribedSubscribers() {
        String session1 = "session4";
        String session2 = "session5";
        String session3 = "session6";

        String subscription1 = "sub4";
        String subscription2 = "sub5";
        String subscription3 = "sub6";
        String subscription4 = "sub7";

        String destination1 = "/topic/chat2";
        String destination2 = "/topic/chat3";

        String message1 = "Hello World!";
        String message2 = "Hello Mars!";

        long timer1 = messageChannelITUtils.connectClient(session1);
        long timer2 = messageChannelITUtils.connectClient(session2);
        long timer3 = messageChannelITUtils.connectClient(session3);

        messageChannelITUtils.subscribeClient(session1, subscription1, destination1, "2001");
        messageChannelITUtils.subscribeClient(session2, subscription2, destination1, "2002");
        messageChannelITUtils.subscribeClient(session3, subscription3, destination2, "2003");

        CompletableFuture<Void> cf1 = CompletableFuture.runAsync(()-> messageChannelITUtils.sendMessage(session1, destination1, message1, "2004"));
        CompletableFuture<Void> cf2 = CompletableFuture.runAsync(()-> messageChannelITUtils.receiveMessage(session1, subscription1, destination1, message1));
        CompletableFuture<Void> cf3 = CompletableFuture.runAsync(()-> messageChannelITUtils.receiveMessage(session2, subscription2, destination1, message1));
        CompletableFuture<Void> cf4 = CompletableFuture.runAsync(()-> messageChannelITUtils.receiveMessageNot(session3));
        CompletableFuture.allOf(cf1, cf2, cf3, cf4).join();

        messageChannelITUtils.unsubscribeClient(session2, subscription2, "2005");
        messageChannelITUtils.subscribeClient(session3, subscription4, destination1, "2006");

        CompletableFuture<Void> cf5 = CompletableFuture.runAsync(()-> messageChannelITUtils.sendMessage(session1, destination1, message2, "2007"));
        CompletableFuture<Void> cf6 = CompletableFuture.runAsync(()-> messageChannelITUtils.receiveMessage(session1, subscription1, destination1, message2));
        CompletableFuture<Void> cf7 = CompletableFuture.runAsync(()-> messageChannelITUtils.receiveMessageNot(session2));
        CompletableFuture<Void> cf8 = CompletableFuture.runAsync(()-> messageChannelITUtils.receiveMessage(session3, subscription4, destination1, message2));
        CompletableFuture.allOf(cf5, cf6, cf7, cf8).join();

        messageChannelITUtils.disconnectClient(session1, timer1, "2008");
        messageChannelITUtils.disconnectClient(session2, timer2, "2009");
        messageChannelITUtils.disconnectClient(session3, timer3, "2010");
    }

    @Test
    public void shouldSendMessageToMultipleDestinations() {
        String session1 = "session7";
        String session2 = "session8";
        String session3 = "session9";

        String subscription1 = "sub8";
        String subscription2 = "sub9";
        String subscription3 = "sub10";

        String destination1 = "/topic/chat4";
        String destination2 = "/topic/chat5";
        String destination3 = "/topic/chat6";

        String message1 = "Hello World!";
        String message2 = "Hello Mars!";
        String message3 = "Hello Jupiter!";

        long timer1 = messageChannelITUtils.connectClient(session1);
        long timer2 = messageChannelITUtils.connectClient(session2);
        long timer3 = messageChannelITUtils.connectClient(session3);

        messageChannelITUtils.subscribeClient(session1, subscription1, destination1, "3001");
        messageChannelITUtils.subscribeClient(session2, subscription2, destination2, "3002");
        messageChannelITUtils.subscribeClient(session3, subscription3, destination3, "3003");

        CompletableFuture<Void> cf1 = CompletableFuture.runAsync(()-> messageChannelITUtils.sendMessage(session1, destination2, message1, "3004"));
        CompletableFuture<Void> cf2 = CompletableFuture.runAsync(()-> messageChannelITUtils.receiveMessage(session2, subscription2, destination2, message1));
        CompletableFuture.allOf(cf1, cf2).join();
        CompletableFuture<Void> cf3 = CompletableFuture.runAsync(()-> messageChannelITUtils.sendMessage(session2, destination3, message2, "3005"));
        CompletableFuture<Void> cf4 = CompletableFuture.runAsync(()-> messageChannelITUtils.receiveMessage(session3, subscription3, destination3, message2));
        CompletableFuture.allOf(cf3, cf4).join();
        CompletableFuture<Void> cf5 = CompletableFuture.runAsync(()-> messageChannelITUtils.sendMessage(session3, destination1, message3, "3006"));
        CompletableFuture<Void> cf6 = CompletableFuture.runAsync(()-> messageChannelITUtils.receiveMessage(session1, subscription1, destination1, message3));
        CompletableFuture.allOf(cf5, cf6).join();

        messageChannelITUtils.disconnectClient(session1, timer1, "3007");
        messageChannelITUtils.disconnectClient(session2, timer2, "3008");
        messageChannelITUtils.disconnectClient(session3, timer3, "3009");
    }

    @Test
    public void shouldDisconnectAndDontReceivesMessagesAndHeartbeatAnymore() throws InterruptedException {
        String session1 = "session10";
        String session2 = "session11";
        String session3 = "session12";

        String subscription2 = "sub12";
        String subscription3 = "sub13";

        String destination = "/topic/chat";
        String message = "Hello World!";

        long timer1 = messageChannelITUtils.connectClient(session1);
        long timer2 = messageChannelITUtils.connectClient(session2);
        long timer3 = messageChannelITUtils.connectClient(session3);

        messageChannelITUtils.subscribeClient(session2, subscription2, destination, "4002");
        messageChannelITUtils.subscribeClient(session3, subscription3, destination, "4003");

        CompletableFuture<Void> cf1 = CompletableFuture.runAsync(() -> messageChannelITUtils.sendMessage(session1, destination, message, "4004"));
        CompletableFuture<Void> cf2 = CompletableFuture.runAsync(() -> messageChannelITUtils.receiveMessage(session2, subscription2, destination, message));
        CompletableFuture<Void> cf3 = CompletableFuture.runAsync(() -> messageChannelITUtils.receiveMessage(session3, subscription3, destination, message));
        CompletableFuture.allOf(cf1, cf2, cf3).join();

        messageChannelITUtils.disconnectClient(session2, timer2, "4006");
        messageChannelITUtils.disconnectClient(session3, timer3, "4007");

        Thread.sleep(1000);
        messageChannelTestConfig.resetServerOutboundHeartbeats();

        CompletableFuture<Void> cf5 = CompletableFuture.runAsync(() -> messageChannelITUtils.sendMessage(session1, destination, message, "4005"));
        CompletableFuture<Void> cf7 = CompletableFuture.runAsync(() -> messageChannelITUtils.receiveFrameNot(session2));
        CompletableFuture<Void> cf8 = CompletableFuture.runAsync(() -> messageChannelITUtils.receiveFrameNot(session3));
        CompletableFuture.allOf(cf5, cf7, cf8).join();

        messageChannelITUtils.disconnectClient(session1, timer1, "4008");
    }


}

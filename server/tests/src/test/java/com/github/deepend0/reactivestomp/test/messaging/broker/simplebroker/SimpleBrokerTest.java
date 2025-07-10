package com.github.deepend0.reactivestomp.test.messaging.broker.simplebroker;

import com.github.deepend0.reactivestomp.messaging.broker.simplebroker.SimpleBroker;
import com.github.deepend0.reactivestomp.messaging.broker.simplebroker.Subscriber;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.smallrye.common.constraint.Assert;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

public class SimpleBrokerTest {

    private SimpleBroker simpleBroker;

    @BeforeEach
    public void init() {
        simpleBroker = SimpleBroker.build();
        simpleBroker.run();
    }

    @Test
    public void shouldSubscribeAndSend() {
        List<String> messages = Lists.newArrayList("message1", "message2", "message3");
        List<String> receivedMessages = new ArrayList<>();
        Subscriber subscriber = new Subscriber("subscriber1");
        simpleBroker.subscribe(subscriber, "topic1").subscribe().with(m->receivedMessages.add((String)m));
        messages.forEach(m->simpleBroker.send("topic1", m));
        Awaitility.await().atMost(Duration.ofMillis(3000)).pollInterval(Duration.ofMillis(1000)).until(()->messages.equals(receivedMessages));
        simpleBroker.stop();
    }

    @Test
    public void shouldUnsubscribeAndStopSending() throws InterruptedException {
        List<String> receivedMessages = new ArrayList<>();
        Subscriber subscriber = new Subscriber("subscriber1");

        new Thread(()-> {
            for (int i = 0; ; i++) {
                try {
                    Thread.sleep(100L);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                simpleBroker.send("topic1", "message" + i);
            }
        }).start();
        simpleBroker.subscribe(subscriber, "topic1").subscribe().with(m->receivedMessages.add((String)m));
        Thread.sleep(1000L);
        Assert.assertTrue(!receivedMessages.isEmpty());
        simpleBroker.unsubscribe(subscriber, "topic1");
        AtomicInteger atomicInteger = new AtomicInteger(receivedMessages.size());
        Awaitility.await().atLeast(Duration.ofMillis(1000)).and().atMost(Duration.ofMillis(3000)).pollInterval(Duration.ofMillis(1000)).until(()->{
            int val = atomicInteger.get();
            atomicInteger.set(receivedMessages.size());
            return val == receivedMessages.size();
        });
        simpleBroker.stop();
    }

    @Test
    public void shouldStop() throws InterruptedException {
        List<String> receivedMessages = new ArrayList<>();
        Subscriber subscriber = new Subscriber("subscriber1");

        new Thread(()-> {
            for (int i = 0; ; i++) {
                try {
                    Thread.sleep(100L);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                simpleBroker.send("topic1", "message" + i);
            }
        }).start();
        simpleBroker.subscribe(subscriber, "topic1").subscribe().with(m->receivedMessages.add((String)m));
        Thread.sleep(1000L);
        Assert.assertTrue(!receivedMessages.isEmpty());
        simpleBroker.stop();
        AtomicInteger atomicInteger = new AtomicInteger(receivedMessages.size());
        Awaitility.await().atLeast(Duration.ofMillis(1000)).and().atMost(Duration.ofMillis(3000)).pollInterval(Duration.ofMillis(1000)).until(()->{
            int val = atomicInteger.get();
            atomicInteger.set(receivedMessages.size());
            return val == receivedMessages.size();
        });
    }

    @Test
    public void shouldHaveMultipleSubscribers() {
        List<String> messages = Lists.newArrayList("message1", "message2", "message3");
        List<String> receivedMessages1 = new ArrayList<>();
        List<String> receivedMessages2 = new ArrayList<>();
        List<String> receivedMessages3 = new ArrayList<>();
        Subscriber subscriber1 = new Subscriber("subscriber1");
        Subscriber subscriber2 = new Subscriber("subscriber2");
        Subscriber subscriber3 = new Subscriber("subscriber3");
        simpleBroker.subscribe(subscriber1, "topic1").subscribe().with(m->receivedMessages1.add((String)m));
        simpleBroker.subscribe(subscriber2, "topic1").subscribe().with(m->receivedMessages2.add((String)m));
        simpleBroker.subscribe(subscriber3, "topic1").subscribe().with(m->receivedMessages3.add((String)m));
        messages.forEach(m->simpleBroker.send("topic1", m));
        //TODO Occasionally Fails
        Awaitility.await().atMost(Duration.ofMillis(3000)).pollInterval(Duration.ofMillis(1000)).until(()->messages.equals(receivedMessages1));
        Awaitility.await().atMost(Duration.ofMillis(3000)).pollInterval(Duration.ofMillis(1000)).until(()->messages.equals(receivedMessages2));
        Awaitility.await().atMost(Duration.ofMillis(3000)).pollInterval(Duration.ofMillis(1000)).until(()->messages.equals(receivedMessages3));
        simpleBroker.stop();
    }

    @Test
    public void shouldHaveMultipleTopics() {
        Set<String> messages1 = Sets.newHashSet("message1", "message2", "message3");
        Set<String> messages2 = Sets.newHashSet("message4", "message5", "message6");
        Set<String> messages3 = Sets.newHashSet("message7", "message8", "message9");
        Set<String> receivedMessages1 = new HashSet<>();
        Set<String> receivedMessages2 = new HashSet<>();
        Set<String> receivedMessages3 = new HashSet<>();
        Subscriber subscriber = new Subscriber("subscriber1");
        simpleBroker.subscribe(subscriber, "topic1").subscribe().with(m->receivedMessages1.add((String)m));
        simpleBroker.subscribe(subscriber, "topic2").subscribe().with(m->receivedMessages2.add((String)m));
        simpleBroker.subscribe(subscriber, "topic3").subscribe().with(m->receivedMessages3.add((String)m));
        CompletableFuture.runAsync(()->messages1.forEach(m->simpleBroker.send("topic1", m)));
        CompletableFuture.runAsync(()->messages2.forEach(m->simpleBroker.send("topic2", m)));
        CompletableFuture.runAsync(()->messages3.forEach(m->simpleBroker.send("topic3", m)));
        Awaitility.await().atMost(Duration.ofMillis(3000)).pollInterval(Duration.ofMillis(1000)).until(()->messages1.equals(receivedMessages1));
        Awaitility.await().atMost(Duration.ofMillis(3000)).pollInterval(Duration.ofMillis(1000)).until(()->messages2.equals(receivedMessages2));
        Awaitility.await().atMost(Duration.ofMillis(3000)).pollInterval(Duration.ofMillis(1000)).until(()->messages3.equals(receivedMessages3));
        simpleBroker.stop();
    }

    @Test
    public void shouldUnsubscribeAllTopics() {
        List<String> messages1 = Lists.newArrayList("message1", "message2", "message3");
        List<String> messages2 = Lists.newArrayList("message4", "message5", "message6");
        List<String> messages3 = Lists.newArrayList("message7", "message8", "message9");
        List<String> receivedMessages1 = new ArrayList<>();
        List<String> receivedMessages2 = new ArrayList<>();
        List<String> receivedMessages3 = new ArrayList<>();
        Subscriber subscriber = new Subscriber("subscriber1");
        simpleBroker.subscribe(subscriber, "topic1").subscribe().with(m->receivedMessages1.add((String)m));
        simpleBroker.subscribe(subscriber, "topic2").subscribe().with(m->receivedMessages2.add((String)m));
        simpleBroker.subscribe(subscriber, "topic3").subscribe().with(m->receivedMessages3.add((String)m));
        simpleBroker.unsubscribeAll(subscriber);
        messages1.forEach(m->simpleBroker.send("topic1", m));
        messages2.forEach(m->simpleBroker.send("topic2", m));
        messages3.forEach(m->simpleBroker.send("topic3", m));
        Awaitility.await().atMost(Duration.ofMillis(3000)).pollInterval(Duration.ofMillis(1000)).until(receivedMessages1::isEmpty);
        Awaitility.await().atMost(Duration.ofMillis(3000)).pollInterval(Duration.ofMillis(1000)).until(receivedMessages2::isEmpty);
        Awaitility.await().atMost(Duration.ofMillis(3000)).pollInterval(Duration.ofMillis(1000)).until(receivedMessages3::isEmpty);
        simpleBroker.stop();
    }
}

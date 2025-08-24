package com.github.deepend0.reactivestomp.test;

import com.github.deepend0.reactivestomp.messaging.messageendpoint.MessageEndpoint;
import com.github.deepend0.reactivestomp.messaging.messageendpoint.MessageEndpointResponse;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;

import java.util.UUID;
import java.util.stream.IntStream;

@ApplicationScoped
public class SampleMessageEndpoint {
    @MessageEndpoint(inboundDestination = "/messageEndpoint/intSeries", outboundDestination = "/topic/intSeries")
    public Multi<Integer> nextIntegers(Integer value) {
        return Multi.createFrom().items(IntStream.range(value + 1, value + 11).boxed());
    }

    @MessageEndpoint(inboundDestination = "/messageEndpoint/helloAsync", outboundDestination = "/topic/helloAsync")
    public Uni<String> greetingUni(String name) {
        return Uni.createFrom().item("Hello " + name);
    }

    @MessageEndpoint(inboundDestination = "/messageEndpoint/helloSync", outboundDestination = "/topic/helloSync")
    public String greetingSync(String name) {
        return "Bonjour " + name;
    }

    @MessageEndpoint(inboundDestination = "/messageEndpoint/intValue", outboundDestination = "/topic/intValue")
    public Uni<Integer> intUni(Integer value) {
        return Uni.createFrom().item(value + 1);
    }

    @MessageEndpoint(inboundDestination = "/messageEndpoint/obj", outboundDestination = "/topic/obj")
    public Uni<SampleObject> objUni(SampleObject sampleObject) {
        return Uni.createFrom().item(new SampleObject("AA" + sampleObject.getId(), sampleObject.getValue()+1));
    }

    @MessageEndpoint(inboundDestination = "/messageEndpoint/intValue2")
    public MessageEndpointResponse<Uni<Integer>> intUniMessageEndpointResponse(Integer value) {
        return new MessageEndpointResponse<>("/topic/intValue2", Uni.createFrom().item(value + 1));
    }

    @MessageEndpoint(inboundDestination = "/messageEndpoint/intValue3")
    public Uni<MessageEndpointResponse<Uni<Integer>>> intUniMessageEndpointResponseUni(Integer value) {
        return Uni.createFrom().item(new MessageEndpointResponse<>("/topic/intValue3", Uni.createFrom().item(value + 1)));
    }
}

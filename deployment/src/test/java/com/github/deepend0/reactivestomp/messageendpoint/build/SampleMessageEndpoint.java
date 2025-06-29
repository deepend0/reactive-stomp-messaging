package com.github.deepend0.reactivestomp.messageendpoint.build;

import com.github.deepend0.reactivestomp.messaging.messageendpoint.MessageEndpoint;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;

import java.util.stream.IntStream;

@ApplicationScoped
public class SampleMessageEndpoint {

    @MessageEndpoint(inboundDestination = "inboundDestination1", outboundDestination = "outboundDestination1")
    public Uni<String> greetingMethod(String name) {
        return Uni.createFrom().item("Hello " + name);
    }

    @MessageEndpoint(inboundDestination = "inboundDestination3", outboundDestination = "outboundDestination3")
    public Uni<Integer> squareMethod(Integer value) {
        return Uni.createFrom().item(value * value);
    }

    @MessageEndpoint(inboundDestination = "inboundDestination4", outboundDestination = "outboundDestination5")
    public Multi<Integer> nextTen(Integer value) {
        return Multi.createFrom().items(IntStream.range(value + 1, value + 11).boxed());
    }

    @MessageEndpoint(inboundDestination = "inboundDestination5", outboundDestination = "outboundDestination6")
    public String greetingNonAsyncValue(String name) {
        return "Bonjour " + name;
    }
}

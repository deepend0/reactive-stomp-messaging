package com.github.deepend0.reactivestomp.messaging.messageendpoint.buildstage;

import com.github.deepend0.reactivestomp.messaging.messageendpoint.MessageEndpoint;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;

import java.util.stream.IntStream;

@ApplicationScoped
public class SampleMessageEndpoint {

    @MessageEndpoint(inboundDestination = "e1inboundDestination1", outboundDestination = "e1outboundDestination1")
    public Uni<String> greetingMethod(String name) {
        return Uni.createFrom().item("Hello " + name);
    }

    @MessageEndpoint(inboundDestination = "e1inboundDestination4", outboundDestination = "e1outboundDestination5")
    public Multi<Integer> nextTen(Integer value) {
        return Multi.createFrom().items(IntStream.range(value + 1, value + 11).boxed());
    }

    @MessageEndpoint(inboundDestination = "e1inboundDestination5", outboundDestination = "e1outboundDestination6")
    public String greetingNonAsyncValue(String name) {
        return "Bonjour " + name;
    }
}

package com.github.deepend0.reactivestomp.messageendpoint.build;

import com.github.deepend0.reactivestomp.messageendpoint.MessageEndpoint;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;

import java.util.stream.IntStream;

@ApplicationScoped
public class SampleMessageEndpointTwo {

    @MessageEndpoint(inboundDestination = "inboundDestination2", outboundDestination = "outboundDestination2")
    public Uni<String> anotherGreetingMethod(String name) {
        return Uni.createFrom().item("Hallo " + name);
    }

    @MessageEndpoint(inboundDestination = "inboundDestination3", outboundDestination = "outboundDestination3")
    public Uni<Integer> cubeMethod(Integer value) {
        return Uni.createFrom().item(value * value * value);
    }

    @MessageEndpoint(inboundDestination = "inboundDestination3", outboundDestination = "outboundDestination4")
    public Uni<Integer> pow4Method(Integer value) {
        return Uni.createFrom().item(Double.valueOf(Math.pow(value, 4)).intValue());
    }

    @MessageEndpoint(inboundDestination = "inboundDestination4", outboundDestination = "outboundDestination5")
    public Multi<Integer> previousTen(Integer value) {
        return Multi.createFrom().items(IntStream.range(value - 10, value).boxed());
    }

    @MessageEndpoint(inboundDestination = "inboundDestination5", outboundDestination = "outboundDestination6")
    public String greetingNonAsyncValue(String name) {
        return "Ciao " + name;
    }
}

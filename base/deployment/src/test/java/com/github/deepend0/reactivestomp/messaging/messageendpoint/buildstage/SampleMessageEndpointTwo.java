package com.github.deepend0.reactivestomp.messaging.messageendpoint.buildstage;

import com.github.deepend0.reactivestomp.messaging.messageendpoint.MessageEndpoint;
import com.github.deepend0.reactivestomp.messaging.messageendpoint.PathParam;
import com.github.deepend0.reactivestomp.messaging.messageendpoint.Payload;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;

import java.util.stream.IntStream;

@ApplicationScoped
public class SampleMessageEndpointTwo {
    public record UserDto(String name) {}
    public record ResultDto(String result) {}

    @MessageEndpoint(inboundDestination = "e2inboundDestination2", outboundDestination = "e2outboundDestination2")
    public Uni<String> anotherGreetingMethod(String name) {
        return Uni.createFrom().item("Hallo " + name);
    }

    @MessageEndpoint(inboundDestination = "e2inboundDestination4", outboundDestination = "e2outboundDestination5")
    public Multi<Integer> previousTen(Integer value) {
        return Multi.createFrom().items(IntStream.range(value - 10, value).boxed());
    }

    @MessageEndpoint(inboundDestination = "e2inboundDestination5", outboundDestination = "e2outboundDestination6")
    public String greetingNonAsyncValue(String name) {
        return "Ciao " + name;
    }

    @MessageEndpoint(inboundDestination = "e2/inboundDestination7/{user}/send", outboundDestination = "e2outboundDestination7")
    public Uni<String> anotherGreetingMethodWithParam(@PathParam String user, @Payload String name) {
        return Uni.createFrom().item("Hello " + name + " from " + user);
    }

    @MessageEndpoint(inboundDestination = "e2/inboundDestination8/{user}/send/{times}", outboundDestination = "e2outboundDestination8")
    public Uni<String> anotherGreetingMethodWithMultiParam(@PathParam String user, @PathParam Integer times, @Payload String name) {
        return Uni.createFrom().item("Hello " + name + " from " + user + " " + times + " times");
    }

    @MessageEndpoint(inboundDestination = "e2inboundDestination9", outboundDestination = "e2outboundDestination9")
    public Uni<ResultDto> greetingMethodWithObject(UserDto userDto) {
        return Uni.createFrom().item(new ResultDto("Hello object " + userDto.name));
    }
}

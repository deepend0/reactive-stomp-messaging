package com.github.deepend0.reactivestomp.messageendpoint.build;

import com.github.deepend0.reactivestomp.messageendpoint.MessageEndpoint;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class SampleMessageEndpoint {

    @MessageEndpoint(inboundDestination = "inboundDestination", outboundDestination = "outboundDestination")
    public Uni<String> sampleEndpointMethod(String name) {
        return Uni.createFrom().item("Hello " + name);
    }
}

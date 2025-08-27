# Reactive STOMP Messaging Extension for Quarkus Websockets

`reactive-stomp-ws` is a Quarkus extension project and enables reactive STOMP messaging over Websockets on server-side. 
It can be thought as a mediator of clients including server-side logic for message brokers. 
The STOMP server provides direct broker interface as well as messaging endpoints. 

## Motivation
STOMP (Simple Text Oriented Messaging Protocol) is useful in light-weight protocol for messaging with brokers (see [STOMP specification](https://stomp.github.io/stomp-specification-1.2.html)). 
It is convenient for client-side applications having relatively light messaging requirements like web front-end applications dealing with user interactions.
However, instead of directly connecting to a messaging broker from client-side, having a server-side mediator service would be more useful to add server-side logic like security, data integrity and custom logic.

Another point is that STOMP messaging between a client and server, here via WebSockets, inherently provides routing in messaging. 
When multiple types of messages are sent over the same socket connection, messages are easily multiplexed with different destinations.

Also, though STOMP messaging over Websockets exists in some other frameworks than Quarkus, full reactive implementation and usage was not available.
This project aims to fill this gap.

## Features

### Relay Message Broker

Messages sent to particular inbound destinations are relayed towards a message broker.
Allowed destinations can be configured via `reactive-stomp.messaging.broker.regex` config parameter.

`reactive-stomp-ws-base` package has a simple in-memory broker, whereas `reactive-stomp-ws-kafka` and `reactive-stomp-ws-amqp` packages support Kafka and AMQP message brokers respectively.


### Message Endpoints

This extension allows you to define message-driven endpoints by simply annotating methods with `@MessageEndpoint`.  
Endpoints can handle incoming messages from a given `inboundDestination` and optionally publish responses to an `outboundDestination`.  
They support synchronous return values, asynchronous computations with `Uni`, streaming results with `Multi`, object payloads, dynamic outbound destinations, and path parameter substitution.
Path parameters are supported with curly brackets `{}`.

Messages sent to particular inbound destinations are handled by message endpoints.
Allowed destinations can be configured via `reactive-stomp.messaging.message-endpoint.regex` config parameter.

Below is a complete set of examples demonstrating the different possibilities:

```java
public class SampleMessageEndpoints {

    // 1. Synchronous response
    @MessageEndpoint(inboundDestination = "/messageEndpoint/helloSync", outboundDestination = "/topic/helloSync")
    public String greetingSync(String name) {
        return "Hey " + name;
    }

    // 2. Asynchronous response with Uni
    @MessageEndpoint(inboundDestination = "/messageEndpoint/helloAsync", outboundDestination = "/topic/helloAsync")
    public Uni<String> greetingUni(String name) {
        return Uni.createFrom().item("Hello " + name);
    }

    // 3. Streaming response with Multi
    @MessageEndpoint(inboundDestination = "/messageEndpoint/intSeries", outboundDestination = "/topic/intSeries")
    public Multi<Integer> nextIntegers(Integer value) {
        return Multi.createFrom().items(IntStream.range(value + 1, value + 11).boxed());
    }

    // 4. Consuming and producing custom objects
    @MessageEndpoint(inboundDestination = "/messageEndpoint/obj", outboundDestination = "/topic/obj")
    public Uni<SampleObject> objUni(SampleObject sampleObject) {
        return Uni.createFrom().item(
            new SampleObject("AA" + sampleObject.getId(), sampleObject.getValue() + 1)
        );
    }

    // 5. Explicit outbound destination with MessageEndpointResponse
    @MessageEndpoint(inboundDestination = "/messageEndpoint/intValue2")
    public MessageEndpointResponse<Uni<Integer>> intUniMessageEndpointResponse(Integer value) {
        return new MessageEndpointResponse<>(
            "/topic/intValue2",
            Uni.createFrom().item(value + 1)
        );
    }

    // 6. Fully async MessageEndpointResponse wrapped in Uni
    @MessageEndpoint(inboundDestination = "/messageEndpoint/intValue3")
    public Uni<MessageEndpointResponse<Uni<Integer>>> intUniMessageEndpointResponseUni(Integer value) {
        return Uni.createFrom().item(
            new MessageEndpointResponse<>("/topic/intValue3", Uni.createFrom().item(value + 1))
        );
    }

    // 7. Using inbound path parameters and payload
    @MessageEndpoint(
        inboundDestination = "/messageEndpoint/{user}/helloAsync2",
        outboundDestination = "/topic/helloAsync2"
    )
    public Uni<String> greetingUniWithInboundPathParams(@PathParam String user, @Payload String name) {
        return Uni.createFrom().item("Hello " + name + " from " + user);
    }

    // 8. Dynamic outbound destinations using path parameters
    @MessageEndpoint(
        inboundDestination = "/messageEndpoint/{user}/helloAsync5",
        outboundDestination = "/topic/{user}/helloAsync5/{name}"
    )
    public Uni<String> greetingUniWithIncomingAndOutgoingPathParam(@PathParam String user, @Payload String name) {
        return Uni.createFrom().item("Hello outgoing " + name + " from " + user);
    }
}
```

## Example

An example chat application using `reactive-stomp-ws` can be found [here](https://github.com/deepend0/reactive-stomp-ws-examples).
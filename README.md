# reactive-stomp-server

This project is a Quarkus Extension for STOMP Messaging. Stil under development.

STOMP (Simple Text Oriented Messaging Protocol) is useful in light-weight messaging with brokers. See [STOMP specification](https://stomp.github.io/stomp-specification-1.2.html). It is also quite convenient for socket connections in server-client architecture with message endpoints on servers. 

A client can send messages to destinations via broker to other clients. But when client sends a message to a destination under a particular path, it will be handled by server. Also servers response will be redirected into another broker destination.

This extension will provide message endpoints like this:
```
   @MessageEndpoint(inboundDestination = "/messageEndpoint/intSeries", outboundDestination = "/topic/intSeries")
    public Multi<Integer> nextIntegers(Integer value) {
        return Multi.createFrom().items(IntStream.range(value + 1, value + 11).boxed());
    }

    @MessageEndpoint(inboundDestination = "/messageEndpoint/helloAsync", outboundDestination = "/topic/helloAsync")
    public Uni<String> greetingUni(String name) {
        return Uni.createFrom().item("Hello " + name);
    }
```

The project is based on Quarkus reactive messaging and also internally works fully reactively.

When finished, it will include ( [+]: already implemented, [/] partially implemented, [-] missing ):

Stomp server [/] :
- Stomp processor [+]
- Simple Broker [+]
- Message Endpoints [+]
- External Brokers [-]
- Websocket connections [+]

Stomp Client [-] :
- Websocket connections

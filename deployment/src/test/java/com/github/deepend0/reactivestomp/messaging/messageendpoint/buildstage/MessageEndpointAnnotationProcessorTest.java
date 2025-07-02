package com.github.deepend0.reactivestomp.messaging.messageendpoint.buildstage;

import com.github.deepend0.reactivestomp.messaging.messageendpoint.MessageEndpointMethodWrapper;
import com.github.deepend0.reactivestomp.messaging.messageendpoint.MessageEndpointRegistry;
import io.quarkus.test.QuarkusUnitTest;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

public class MessageEndpointAnnotationProcessorTest {
    @RegisterExtension
    static final QuarkusUnitTest quarkusUnitTest = new QuarkusUnitTest().withApplicationRoot((jar) -> jar
                    .addClass(SampleMessageEndpoint.class)
                    .addClass(SampleMessageEndpointTwo.class))
            .debugBytecode(true);


    @Inject
    private MessageEndpointRegistry messageEndpointRegistry;

    @Test
    public void shouldProcessDestinationWithSingleUniEndpoints() {
        List<MessageEndpointMethodWrapper<?, ?>> messageEndpointMethodWrappers = messageEndpointRegistry.getMessageEndpoints("inboundDestination1");
        Assertions.assertEquals(1, messageEndpointMethodWrappers.size());
        MessageEndpointMethodWrapper<String, String> messageEndpointMethodWrapper = (MessageEndpointMethodWrapper<String, String>) messageEndpointMethodWrappers.getFirst();
        Assertions.assertEquals("inboundDestination1", messageEndpointMethodWrapper.getInboundDestination());
        Assertions.assertEquals("outboundDestination1", messageEndpointMethodWrapper.getOutboundDestination());
        Assertions.assertEquals(String.class, messageEndpointMethodWrapper.getParameterType());
        List<String> result = new ArrayList<>();
        Uni<String> uniResult = (Uni<String>) messageEndpointMethodWrapper.getMethodWrapper().apply("World");
        uniResult.subscribe().with(result::add);
        Awaitility.await().atMost(Duration.ofMillis(3000)).pollInterval(Duration.ofMillis(1000)).until(() -> "Hello World".equals(result.getFirst()));

        List<MessageEndpointMethodWrapper<?, ?>> messageEndpointMethodWrappers2 = messageEndpointRegistry.getMessageEndpoints("inboundDestination2");
        Assertions.assertEquals(1, messageEndpointMethodWrappers2.size());
        MessageEndpointMethodWrapper<String, String> messageEndpointMethodWrapper2 = (MessageEndpointMethodWrapper<String, String>) messageEndpointMethodWrappers2.getFirst();
        Assertions.assertEquals("inboundDestination2", messageEndpointMethodWrapper2.getInboundDestination());
        Assertions.assertEquals("outboundDestination2", messageEndpointMethodWrapper2.getOutboundDestination());
        Assertions.assertEquals(String.class, messageEndpointMethodWrapper2.getParameterType());
        List<String> result2 = new ArrayList<>();
        Uni<String> uniResult2 = (Uni<String>) messageEndpointMethodWrapper2.getMethodWrapper().apply("World");
        uniResult2.subscribe().with(result2::add);
        Awaitility.await().atMost(Duration.ofMillis(3000)).pollInterval(Duration.ofMillis(1000)).until(() -> "Hallo World".equals(result2.getFirst()));
    }

    @Test
    public void shouldProcessDestinationWithMultipleUniEndpoints() {
        List<MessageEndpointMethodWrapper<?, ?>> messageEndpointMethodWrappers = messageEndpointRegistry.getMessageEndpoints("inboundDestination3");
        Assertions.assertEquals(3, messageEndpointMethodWrappers.size());
        Assertions.assertTrue(messageEndpointMethodWrappers
                .stream()
                .anyMatch(messageEndpointMethodWrapper -> "inboundDestination3".equals(messageEndpointMethodWrapper.getInboundDestination())
                        && "outboundDestination3".equals(messageEndpointMethodWrapper.getOutboundDestination())));
        Assertions.assertTrue(messageEndpointMethodWrappers
                .stream()
                .anyMatch(messageEndpointMethodWrapper -> "inboundDestination3".equals(messageEndpointMethodWrapper.getInboundDestination())
                        && "outboundDestination4".equals(messageEndpointMethodWrapper.getOutboundDestination())));
        List<Integer> result = new ArrayList<>();
        messageEndpointMethodWrappers.stream().map(messageEndpointMethodWrapper ->
                        (Uni<Integer>) ((MessageEndpointMethodWrapper<Integer, Integer>) messageEndpointMethodWrapper).getMethodWrapper().apply(3))
                .forEach(integerUni -> integerUni.subscribe().with(result::add));
        Awaitility.await().atMost(Duration.ofMillis(3000)).pollInterval(Duration.ofMillis(1000)).until(() -> result.size() == 3 && result.containsAll(List.of(9, 27, 81)));
    }

    @Test
    public void shouldProcessDestinationWithMultipleMultiEndpoints() {
        List<MessageEndpointMethodWrapper<?, ?>> messageEndpointMethodWrappers = messageEndpointRegistry.getMessageEndpoints("inboundDestination4");
        Assertions.assertEquals(2, messageEndpointMethodWrappers.size());
        Assertions.assertTrue(messageEndpointMethodWrappers
                .stream()
                .allMatch(messageEndpointMethodWrapper -> "inboundDestination4".equals(messageEndpointMethodWrapper.getInboundDestination())
                        && "outboundDestination5".equals(messageEndpointMethodWrapper.getOutboundDestination())));
        List<Integer> result = new ArrayList<>();
        messageEndpointMethodWrappers.stream().map(messageEndpointMethodWrapper ->
                        (Multi<Integer>) ((MessageEndpointMethodWrapper<Integer, Integer>) messageEndpointMethodWrapper).getMethodWrapper().apply(11))
                .forEach(integerUni -> integerUni.subscribe().with(result::add));
        Awaitility.await().atMost(Duration.ofMillis(3000)).pollInterval(Duration.ofMillis(1000)).until(() -> result.size() == 20 && result.containsAll(IntStream.range(1, 22).filter(i -> i != 11).boxed().toList()));
    }

    @Test
    public void shouldProcessDestinationWithNonAsyncEndpoints() {
        List<MessageEndpointMethodWrapper<?, ?>> messageEndpointMethodWrappers = messageEndpointRegistry.getMessageEndpoints("inboundDestination5");
        Assertions.assertEquals(2, messageEndpointMethodWrappers.size());
        Assertions.assertTrue(messageEndpointMethodWrappers
                .stream()
                .allMatch(messageEndpointMethodWrapper -> "inboundDestination5".equals(messageEndpointMethodWrapper.getInboundDestination())
                        && "outboundDestination6".equals(messageEndpointMethodWrapper.getOutboundDestination())));
        List<Integer> result = new ArrayList<>();
        messageEndpointMethodWrappers.stream().map(messageEndpointMethodWrapper ->
                        (String) ((MessageEndpointMethodWrapper<String, String>) messageEndpointMethodWrapper).getMethodWrapper().apply("Jupiter"))
                .toList().containsAll(List.of("Bonjour Mars", "Ciao Mars"));
    }
}

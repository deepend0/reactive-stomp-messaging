package com.github.deepend0.reactivestomp.messaging.messageendpoint.buildstage;

import com.github.deepend0.reactivestomp.messaging.messageendpoint.MessageEndpointMethodWrapper;
import com.github.deepend0.reactivestomp.messaging.messageendpoint.MessageEndpointRegistry;
import com.github.deepend0.reactivestomp.messaging.messageendpoint.PathHandlerRouter;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

public class MessageEndpointAnnotationProcessorTest {
    @RegisterExtension
    static final QuarkusUnitTest quarkusUnitTest = new QuarkusUnitTest().withApplicationRoot((jar) -> jar
                    .addClass(SampleMessageEndpoint.class)
                    .addClass(SampleMessageEndpointTwo.class)
                    .addAsResource("application.yaml"))
            .debugBytecode(true);

    @Inject
    private MessageEndpointRegistry messageEndpointRegistry;

    @Test
    public void shouldProcessDestinationWithUniEndpoint() {
        PathHandlerRouter.MatchResult<MessageEndpointMethodWrapper<?, ?>> matchResult = messageEndpointRegistry.getMessageEndpoints("e1inboundDestination1");
        MessageEndpointMethodWrapper<String, Object> messageEndpointMethodWrapper = (MessageEndpointMethodWrapper<String, Object> ) matchResult.getHandler();
        Assertions.assertNotNull(messageEndpointMethodWrapper);
        Assertions.assertEquals("e1inboundDestination1", messageEndpointMethodWrapper.getInboundDestination());
        Assertions.assertEquals("e1outboundDestination1", messageEndpointMethodWrapper.getOutboundDestination());
        Assertions.assertEquals(String.class, messageEndpointMethodWrapper.getParameterType());
        List<String> result = new ArrayList<>();
        Uni<String> uniResult = (Uni<String>) messageEndpointMethodWrapper.getMethodWrapper().apply(new HashMap<>(), "World");
        uniResult.subscribe().with(result::add);
        Awaitility.await().atMost(Duration.ofMillis(3000)).pollInterval(Duration.ofMillis(1000)).until(() -> "Hello World".equals(result.getFirst()));

        PathHandlerRouter.MatchResult<MessageEndpointMethodWrapper<?, ?>> matchResult2 = messageEndpointRegistry.getMessageEndpoints("e2inboundDestination2");
        MessageEndpointMethodWrapper<String, Object> messageEndpointMethodWrapper2 = (MessageEndpointMethodWrapper<String, Object> ) matchResult2.getHandler();
        Assertions.assertNotNull(messageEndpointMethodWrapper2);
        Assertions.assertEquals("e2inboundDestination2", messageEndpointMethodWrapper2.getInboundDestination());
        Assertions.assertEquals("e2outboundDestination2", messageEndpointMethodWrapper2.getOutboundDestination());
        Assertions.assertEquals(String.class, messageEndpointMethodWrapper2.getParameterType());
        List<String> result2 = new ArrayList<>();
        Uni<String> uniResult2 = (Uni<String>) messageEndpointMethodWrapper2.getMethodWrapper().apply(new HashMap<>(), "World");
        uniResult2.subscribe().with(result2::add);
        Awaitility.await().atMost(Duration.ofMillis(3000)).pollInterval(Duration.ofMillis(1000)).until(() -> "Hallo World".equals(result2.getFirst()));
    }

    @Test
    public void shouldProcessDestinationWithMultiEndpoints() {
        PathHandlerRouter.MatchResult<MessageEndpointMethodWrapper<?, ?>> matchResult = messageEndpointRegistry.getMessageEndpoints("e1inboundDestination4");
        MessageEndpointMethodWrapper<?, ?> messageEndpointMethodWrapper = matchResult.getHandler();
        Assertions.assertNotNull(messageEndpointMethodWrapper);
        Assertions.assertTrue("e1inboundDestination4".equals(messageEndpointMethodWrapper.getInboundDestination())
                && "e1outboundDestination5".equals(messageEndpointMethodWrapper.getOutboundDestination()));
        List<Integer> result = new ArrayList<>();
        ((Multi<Integer>)((MessageEndpointMethodWrapper<Integer, Integer>) messageEndpointMethodWrapper).getMethodWrapper().apply(new HashMap<>(), 11))
                .subscribe().with(result::add);

        PathHandlerRouter.MatchResult<MessageEndpointMethodWrapper<?, ?>> matchResult2 = messageEndpointRegistry.getMessageEndpoints("e2inboundDestination4");
        MessageEndpointMethodWrapper<?, ?> messageEndpointMethodWrapper2 = matchResult2.getHandler();
        Assertions.assertNotNull(messageEndpointMethodWrapper2);
        Assertions.assertTrue("e2inboundDestination4".equals(messageEndpointMethodWrapper2.getInboundDestination())
                && "e2outboundDestination5".equals(messageEndpointMethodWrapper2.getOutboundDestination()));
        ((Multi<Integer>)((MessageEndpointMethodWrapper<Integer, Integer>) messageEndpointMethodWrapper2).getMethodWrapper().apply(new HashMap<>(), 11))
                .subscribe().with(result::add);

        Awaitility.await().atMost(Duration.ofMillis(3000)).pollInterval(Duration.ofMillis(1000)).until(() -> result.size() == 20 && result.containsAll(IntStream.range(1, 22).filter(i -> i != 11).boxed().toList()));
    }

    @Test
    public void shouldProcessDestinationWithNonAsyncEndpoint() {
        PathHandlerRouter.MatchResult<MessageEndpointMethodWrapper<?, ?>> matchResult = messageEndpointRegistry.getMessageEndpoints("e1inboundDestination5");
        MessageEndpointMethodWrapper<?, ?> messageEndpointMethodWrapper = matchResult.getHandler();

        Assertions.assertNotNull(messageEndpointMethodWrapper);
        Assertions.assertTrue("e1inboundDestination5".equals(messageEndpointMethodWrapper.getInboundDestination())
                        && "e1outboundDestination6".equals(messageEndpointMethodWrapper.getOutboundDestination()));

        ((String) ((MessageEndpointMethodWrapper<String, String>) messageEndpointMethodWrapper).getMethodWrapper().apply( new HashMap<>(), "Jupiter")) .equals("Bonjour Mars");
    }

    @Test
    public void shouldProcessDestinationWithParametrizedUniEndpoint() {
        PathHandlerRouter.MatchResult<MessageEndpointMethodWrapper<?, ?>> matchResult = messageEndpointRegistry.getMessageEndpoints("e2/inboundDestination7/john/send");
        Map<String, String> params = new HashMap<>();
        params.put("user", "john");
        Assertions.assertEquals(matchResult.getParams(), params);
        MessageEndpointMethodWrapper<String, Object> messageEndpointMethodWrapper = (MessageEndpointMethodWrapper<String, Object>) matchResult.getHandler();
        Assertions.assertNotNull(messageEndpointMethodWrapper);
        Assertions.assertEquals("e2/inboundDestination7/{user}/send", messageEndpointMethodWrapper.getInboundDestination());
        Assertions.assertEquals("e2outboundDestination7", messageEndpointMethodWrapper.getOutboundDestination());
        Assertions.assertEquals(String.class, messageEndpointMethodWrapper.getParameterType());
        List<String> result = new ArrayList<>();
        Uni<String> uniResult = (Uni<String>) messageEndpointMethodWrapper.getMethodWrapper().apply(params, "World");
        uniResult.subscribe().with(result::add);
        Awaitility.await().atMost(Duration.ofMillis(3000)).pollInterval(Duration.ofMillis(1000)).until(() -> "Hello World from john".equals(result.getFirst()));
    }

    @Test
    public void shouldProcessDestinationWithMultipleParametrizedUniEndpoint() {
        PathHandlerRouter.MatchResult<MessageEndpointMethodWrapper<?, ?>> matchResult = messageEndpointRegistry.getMessageEndpoints("e2/inboundDestination8/sam/send/5");
        Map<String, String> params = new HashMap<>();
        params.put("user", "sam");
        params.put("times", "5");
        Assertions.assertEquals(matchResult.getParams(), params);
        MessageEndpointMethodWrapper<String, Object> messageEndpointMethodWrapper = (MessageEndpointMethodWrapper<String, Object>) matchResult.getHandler();
        Assertions.assertNotNull(messageEndpointMethodWrapper);
        Assertions.assertEquals("e2/inboundDestination8/{user}/send/{times}", messageEndpointMethodWrapper.getInboundDestination());
        Assertions.assertEquals("e2outboundDestination8", messageEndpointMethodWrapper.getOutboundDestination());
        Assertions.assertEquals(String.class, messageEndpointMethodWrapper.getParameterType());
        List<String> result = new ArrayList<>();
        Uni<String> uniResult = (Uni<String>) messageEndpointMethodWrapper.getMethodWrapper().apply(params, "World");
        uniResult.subscribe().with(result::add);
        Awaitility.await().atMost(Duration.ofMillis(3000)).pollInterval(Duration.ofMillis(1000)).until(() -> "Hello World from sam 5 times".equals(result.getFirst()));
    }

    @Test
    public void shouldProcessDestinationWithObjectUniEndpoint() {
        PathHandlerRouter.MatchResult<MessageEndpointMethodWrapper<?, ?>> matchResult = messageEndpointRegistry.getMessageEndpoints("e2inboundDestination9");
        MessageEndpointMethodWrapper<SampleMessageEndpointTwo.UserDto, SampleMessageEndpointTwo.ResultDto> messageEndpointMethodWrapper = (MessageEndpointMethodWrapper<SampleMessageEndpointTwo.UserDto, SampleMessageEndpointTwo.ResultDto>) matchResult.getHandler();
        Assertions.assertNotNull(messageEndpointMethodWrapper);
        Assertions.assertEquals("e2inboundDestination9", messageEndpointMethodWrapper.getInboundDestination());
        Assertions.assertEquals("e2outboundDestination9", messageEndpointMethodWrapper.getOutboundDestination());
        Assertions.assertEquals(SampleMessageEndpointTwo.UserDto.class, messageEndpointMethodWrapper.getParameterType());
        List<SampleMessageEndpointTwo.ResultDto> result = new ArrayList<>();
        Uni<SampleMessageEndpointTwo.ResultDto> uniResult = (Uni<SampleMessageEndpointTwo.ResultDto>) messageEndpointMethodWrapper.getMethodWrapper().apply(new HashMap<>(), new SampleMessageEndpointTwo.UserDto("World"));
        uniResult.subscribe().with(result::add);
        Awaitility.await().atMost(Duration.ofMillis(3000)).pollInterval(Duration.ofMillis(1000)).until(() -> "Hello object World".equals(result.getFirst().result()));
    }
}

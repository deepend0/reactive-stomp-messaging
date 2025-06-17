package com.github.deepend0.reactivestomp.messageendpoint;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.function.Function;

public class MessageEndpointMethodWrapper<I, O> {
    private final Logger LOGGER = LoggerFactory.getLogger(MessageEndpointMethodWrapper.class);
    private final String inboundDestination;
    private final String outboundDestination;
    //TODO Create Uni and Multi wrappers separately
    private final Function<I, Object> methodWrapper;
    private final Class<I> parameterType;

    public MessageEndpointMethodWrapper(String inboundDestination, String outboundDestination, Function<I, Object> methodWrapper, Class<I> parameterType) {
        this.inboundDestination = inboundDestination;
        this.outboundDestination = outboundDestination;
        this.methodWrapper = methodWrapper;
        this.parameterType = parameterType;
    }

    public byte[] serialize(Serde serde, O o) throws IOException {
        return serde.serialize(o);
    }

    public I deserialize(Serde serde, byte[] bytes)  throws IOException {
        return serde.deserialize(bytes, parameterType);
    }

    public String getInboundDestination() {
        return inboundDestination;
    }

    public String getOutboundDestination() {
        return outboundDestination;
    }

    public Function<I, Object> getMethodWrapper() {
        return methodWrapper;
    }

    public Class<I> getParameterType() {
        return parameterType;
    }

    public Multi<byte[]> call(Serde serde, byte [] bytes) {
        try {
            Object result = methodWrapper.apply(deserialize(serde, bytes));

            Multi<O> multiResult = switch (result) {
                case Uni<?> uni -> ((Uni<O>) uni).toMulti();
                case Multi<?> multi -> (Multi<O>) multi;
                default -> throw new IllegalStateException("Method ref should return Uni or Multi");
            };
            return multiResult.onItem().transformToUni(o -> {
                try {
                    return Uni.createFrom().item(serialize(serde, o));
                } catch (IOException ioException) {
                    return Uni.createFrom().failure(ioException);
                }
            }).merge();
        } catch (IOException ioException) {
            LOGGER.error("Error during deserialization.", ioException);
            return Multi.createFrom().failure(ioException);
        }
    }
}

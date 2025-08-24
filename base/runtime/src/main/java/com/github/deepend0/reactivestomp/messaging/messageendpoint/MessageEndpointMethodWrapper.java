package com.github.deepend0.reactivestomp.messaging.messageendpoint;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.function.BiFunction;

public class MessageEndpointMethodWrapper<I, O> {
    private final Logger LOGGER = LoggerFactory.getLogger(MessageEndpointMethodWrapper.class);
    private final String inboundDestination;
    private final String outboundDestination;
    //TODO Create Uni and Multi wrappers separately
    private final BiFunction<Map<String, String>, I, Object> methodWrapper;
    private final Class<I> parameterType;
    private final Boolean wrappedResponse;

    public MessageEndpointMethodWrapper(String inboundDestination,
                                        String outboundDestination,
                                        BiFunction<Map<String, String>,I, Object> methodWrapper,
                                        Class<I> parameterType,
                                        Boolean wrappedResponse) {
        this.inboundDestination = inboundDestination;
        this.outboundDestination = outboundDestination;
        this.methodWrapper = methodWrapper;
        this.parameterType = parameterType;
        this.wrappedResponse = wrappedResponse;
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

    public BiFunction<Map<String, String>,I, Object> getMethodWrapper() {
        return methodWrapper;
    }

    public Class<I> getParameterType() {
        return parameterType;
    }

    public Multi<MessageEndpointResponse<Multi<byte[]>>> call(Serde serde, Map<String, String> inboundPathParams, byte [] bytes) {
        try {
            Object response = methodWrapper.apply(inboundPathParams, deserialize(serde, bytes));

            if(wrappedResponse) {
                Multi<MessageEndpointResponse<O>> messageEndpointResponseMulti;
                if(response instanceof MessageEndpointResponse messageEndpointResponseResult) {
                    messageEndpointResponseMulti = Multi.createFrom().item(messageEndpointResponseResult);
                } else if(response instanceof Uni) {
                    messageEndpointResponseMulti = ((Uni<MessageEndpointResponse<O>>) response).toMulti();
                }
                else {
                    messageEndpointResponseMulti = (Multi<MessageEndpointResponse<O>>) response;
                }
                return messageEndpointResponseMulti.map(messageEndpointResponse -> {
                    Multi<byte[]> multiByteArrayValue = convertToMultiAndSerialize(messageEndpointResponse.value(), serde);
                    return MessageEndpointResponse.of(messageEndpointResponse.outboundDestination(), multiByteArrayValue);
                });
            } else {
                Multi<byte[]> multiByteArrayValue = convertToMultiAndSerialize(response, serde);
                return Multi.createFrom().item(new MessageEndpointResponse<>(null, multiByteArrayValue));
            }
        } catch (IOException ioException) {
            LOGGER.error("Error during deserialization.", ioException);
            return Multi.createFrom().failure(ioException);
        }
    }

    private Multi<byte[]> convertToMultiAndSerialize(Object value, Serde serde) {
        Multi<byte[]> multiByteArrayValue;
        Multi<O> multiValue = switch (value) {
            case Uni<?> uni -> ((Uni<O>) uni).toMulti();
            case Multi<?> multi -> (Multi<O>) multi;
            default -> Multi.createFrom().item((O) value);
        };

        multiByteArrayValue =  multiValue.onItem().transformToUni(o -> {
            try {
                return Uni.createFrom().item(serialize(serde, o));
            } catch (IOException ioException) {
                return Uni.createFrom().failure(ioException);
            }
        }).merge();
        return multiByteArrayValue;
    }
}

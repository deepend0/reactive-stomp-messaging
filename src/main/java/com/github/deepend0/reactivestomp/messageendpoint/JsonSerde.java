package com.github.deepend0.reactivestomp.messageendpoint;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.enterprise.context.ApplicationScoped;

import java.io.IOException;

@ApplicationScoped
public class JsonSerde implements Serde {
    private final ObjectMapper mapper;

    public JsonSerde(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public <T> byte[] serialize(T t) throws IOException {
        return mapper.writeValueAsBytes(t);
    }

    @Override
    public <T> T deserialize(byte[] bytes, Class<T> clazz) throws IOException {
        return mapper.readValue(bytes, clazz);
    }
}

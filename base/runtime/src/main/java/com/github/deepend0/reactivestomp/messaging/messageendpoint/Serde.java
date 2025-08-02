package com.github.deepend0.reactivestomp.messaging.messageendpoint;

import java.io.IOException;

public interface Serde {
    <T> byte[] serialize(T t) throws IOException;
    <T> T deserialize(byte[] bytes, Class<T> clazz) throws IOException;
}

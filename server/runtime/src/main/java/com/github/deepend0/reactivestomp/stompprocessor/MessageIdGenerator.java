package com.github.deepend0.reactivestomp.stompprocessor;

import jakarta.enterprise.context.ApplicationScoped;

import java.util.UUID;

@ApplicationScoped
public class MessageIdGenerator {
    public String generate() {
        return UUID.randomUUID().toString();
    }
}

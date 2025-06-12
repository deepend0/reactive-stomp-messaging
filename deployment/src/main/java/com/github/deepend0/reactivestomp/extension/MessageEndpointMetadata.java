package com.github.deepend0.reactivestomp.extension;

import io.quarkus.builder.item.MultiBuildItem;
import org.jboss.jandex.ClassInfo;
import org.jboss.jandex.MethodInfo;

public class MessageEndpointMetadata extends MultiBuildItem {
    private final String inboundDestination;
    private final String outboundDestination;
    private final ClassInfo classInfo;
    private final MethodInfo methodInfo;

    public MessageEndpointMetadata(String inboundDestination, String outboundDestination, ClassInfo classInfo, MethodInfo methodInfo) {
        this.inboundDestination = inboundDestination;
        this.outboundDestination = outboundDestination;
        this.classInfo = classInfo;
        this.methodInfo = methodInfo;
    }

    public String getInboundDestination() {
        return inboundDestination;
    }

    public String getOutboundDestination() {
        return outboundDestination;
    }

    public ClassInfo getClassInfo() {
        return classInfo;
    }

    public MethodInfo getMethodInfo() {
        return methodInfo;
    }
}

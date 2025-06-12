package com.github.deepend0.reactivestomp.extension;

import com.github.deepend0.reactivestomp.messageendpoint.MessageEndpoint;
import com.github.deepend0.reactivestomp.messageendpoint.MessageEndpointMethodWrapper;
import com.github.deepend0.reactivestomp.messageendpoint.MessageEndpointRegistry;
import io.quarkus.Generated;
import io.quarkus.deployment.GeneratedClassGizmoAdaptor;
import io.quarkus.deployment.annotations.BuildProducer;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.builditem.CombinedIndexBuildItem;
import io.quarkus.deployment.builditem.GeneratedClassBuildItem;
import io.quarkus.gizmo.ClassCreator;
import io.quarkus.gizmo.ClassOutput;
import io.quarkus.gizmo.FieldCreator;
import io.quarkus.gizmo.FieldDescriptor;
import io.quarkus.gizmo.MethodCreator;
import io.quarkus.gizmo.MethodDescriptor;
import io.quarkus.gizmo.ResultHandle;
import io.quarkus.gizmo.Type;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import javax.lang.model.element.Modifier;

import jakarta.inject.Inject;
import org.jboss.jandex.*;

public class MessageEndpointAnnotationProcessor {

  private static final String REGISTRY_CLASS_NAME =
      "com.github.deepend0.reactivestomp.messageendpoint.MessageEndpointRegistryImpl";

  @BuildStep
  public void collectMessageEndpoints(
      CombinedIndexBuildItem combinedIndex,
      BuildProducer<MessageEndpointMetadata> messageEndpointMetadataBuildProducer) {
    IndexView index = combinedIndex.getIndex();
    for (ClassInfo classInfo : index.getKnownClasses()) {
      for (MethodInfo methodInfo : classInfo.methods()) {
        if (methodInfo.hasAnnotation(DotName.createSimple(MessageEndpoint.class.getName()))) {
          AnnotationInstance annotationInstance =
              methodInfo.annotations(DotName.createSimple(MessageEndpoint.class.getName())).get(0);
          String inboundDestination = annotationInstance.value("inboundDestination").asString();
          String outboundDestination = annotationInstance.value("outboundDestination").asString();
          MessageEndpointMetadata messageEndpointMetadata =
              new MessageEndpointMetadata(
                  inboundDestination, outboundDestination, classInfo, methodInfo);
          messageEndpointMetadataBuildProducer.produce(messageEndpointMetadata);
        }
      }
    }
  }

  /* Example of generated a message endpoint method wrapper class:

  @ApplicationScoped
  public final class com.example.MyEndpoint_processWrapper {

      private final MyEndpoint myEndpoint;

      public com.example.MyEndpoint_processWrapper(MyEndpoint myEndpoint) {
          this.myEndpoint = myEndpoint;
      }

      public String apply(String input) {
          return myEndpoint.process(input);
      }
  }
  */

  @BuildStep
  public void generateMethodWrapperClasses(
      List<MessageEndpointMetadata> messageEndpointMetadataList,
      BuildProducer<GeneratedClassBuildItem> generatedClassBuildItemBuildProducer) {
    ClassOutput classOutput =
        new GeneratedClassGizmoAdaptor(generatedClassBuildItemBuildProducer, true);

    for (MessageEndpointMetadata metadata : messageEndpointMetadataList) {
      ClassInfo classInfo = metadata.getClassInfo();
      MethodInfo methodInfo = metadata.getMethodInfo();

      String className = classInfo.name().toString();
      String wrapperClassName = className + "_" + methodInfo.name() + "Wrapper";

      try (ClassCreator classCreator =
          ClassCreator.builder()
              .classOutput(classOutput)
              .className(wrapperClassName)
              .setFinal(true)
              .superClass(Object.class)
              .build()) {

        classCreator.addAnnotation(Generated.class);
        classCreator.addAnnotation(ApplicationScoped.class);

        String fieldName = buildFieldNameFromClassName(classInfo.simpleName());

        FieldDescriptor fieldDescriptor =
            classCreator
                .getFieldCreator(fieldName, className)
                .setModifiers(Modifier.PRIVATE.ordinal() | Modifier.FINAL.ordinal())
                .getFieldDescriptor();

        // Constructor
        try (MethodCreator methodCreator = classCreator.getConstructorCreator(className)) {
          methodCreator.setModifiers(Modifier.PUBLIC.ordinal());
          methodCreator.writeInstanceField(
              fieldDescriptor, methodCreator.getThis(), methodCreator.getMethodParam(0));
          methodCreator.returnValue(null);
        }

        // Apply method
        List<MethodParameterInfo> params = methodInfo.parameters();
        if (params.size() == 1) {
          String returnType = methodInfo.returnType().name().toString();
          String paramType = params.get(0).name();

          try (MethodCreator apply =
              classCreator.getMethodCreator("apply", returnType, paramType)) {
            apply.setModifiers(Modifier.PUBLIC.ordinal());
            ResultHandle instance = apply.readInstanceField(fieldDescriptor, apply.getThis());
            ResultHandle result =
                apply.invokeVirtualMethod(
                    MethodDescriptor.ofMethod(className, methodInfo.name(), returnType, paramType),
                    instance,
                    apply.getMethodParam(0));
            apply.returnValue(result);
          }
        }
      }
    }
  }

  /* Example of generated class for a message endpoint registry:
  @ApplicationScoped
  public class GeneratedMessageEndpointRegistry implements MessageEndpointRegistry {

      @Inject
      MyService_myMethodWrapper myService_myMethodWrapper;

      private final Map<String, List<MessageEndpointMethodWrapper<?, ?>>> registry = new HashMap<>();

      @PostConstruct
      void init() {
          registry.put("inbound-topic", List.of(
              new MessageEndpointMethodWrapper<>(new MessageEndpoint("inbound-topic", "outbound-topic"), myService_myMethodWrapper::apply, String.class)
          ));
      }

      @Override
      public List<MessageEndpointMethodWrapper<?, ?>> getMessageEndpoints(String destination) {
          return registry.getOrDefault(destination, List.of());
      }
  }
  */
  @BuildStep
  void generateRegistryClass(
      List<MessageEndpointMetadata> messageEndpointMetadataList,
      BuildProducer<GeneratedClassBuildItem> generatedClassProducer) {

    ClassOutput classOutput = new GeneratedClassGizmoAdaptor(generatedClassProducer, true);

    try (ClassCreator classCreator =
        ClassCreator.builder()
            .classOutput(classOutput)
            .className(REGISTRY_CLASS_NAME)
            .interfaces(MessageEndpointRegistry.class)
            .superClass(Object.class)
            .setFinal(true)
            .build()) {

      classCreator.addAnnotation(Generated.class);
      classCreator.addAnnotation(ApplicationScoped.class);

      // Inject wrapper fields + collect by inbound destination
      Map<String, List<MessageEndpointMetadata>> groupedMetadata = new HashMap<>();

      for (MessageEndpointMetadata meta : messageEndpointMetadataList) {
        groupedMetadata
            .computeIfAbsent(meta.getInboundDestination(), k -> new ArrayList<>())
            .add(meta);

        // Declare injected field
        String wrapperClass =
            meta.getClassInfo().name() + "_" + meta.getMethodInfo().name() + "Wrapper";
        String fieldName = buildFieldNameFromClassName(wrapperClass);

        FieldCreator field =
            classCreator
                .getFieldCreator(fieldName, wrapperClass)
                .setModifiers(Modifier.PRIVATE.ordinal());
        field.addAnnotation(Inject.class);
      }

      Type registryType =
          Type.parameterizedType(
              Type.classType(Map.class),
              Type.classType(String.class),
              Type.parameterizedType(
                  Type.classType(List.class),
                  Type.parameterizedType(
                      Type.classType(MessageEndpointMethodWrapper.class),
                      Type.wildcardTypeUnbounded(),
                      Type.wildcardTypeUnbounded())));

      // Registry field
      FieldDescriptor registryField =
          classCreator
              .getFieldCreator("registry", registryType)
              .setModifiers(Modifier.PRIVATE.ordinal() | Modifier.FINAL.ordinal())
              .getFieldDescriptor();

      // Constructor
      try (MethodCreator ctor = classCreator.getConstructorCreator((Class<?>) null)) {
        ResultHandle newMap = ctor.newInstance(MethodDescriptor.ofConstructor(HashMap.class));
        ctor.writeInstanceField(registryField, ctor.getThis(), newMap);
      }

      // Move the post construct method in constructor
      // @PostConstruct init() to fill the map
      try (MethodCreator init = classCreator.getMethodCreator("init", void.class)) {

        init.addAnnotation(PostConstruct.class);
        ResultHandle mapRef = init.readInstanceField(registryField, init.getThis());

        for (Map.Entry<String, List<MessageEndpointMetadata>> entry : groupedMetadata.entrySet()) {
          String inbound = entry.getKey();
          List<MessageEndpointMetadata> endpointMetadataList = entry.getValue();

          ResultHandle list = init.newInstance(MethodDescriptor.ofConstructor(ArrayList.class));

          for (MessageEndpointMetadata endpointMetadata : endpointMetadataList) {
            String className = endpointMetadata.getClassInfo().name().toString();
            String methodName = endpointMetadata.getMethodInfo().name();
            String fieldClass = className + "_" + methodName + "Wrapper";
            String fieldName = buildFieldNameFromClassName(fieldClass);

            // Get wrapper field
            ResultHandle wrapper =
                init.readInstanceField(
                    FieldDescriptor.of(REGISTRY_CLASS_NAME, fieldName, fieldClass), init.getThis());

            // Build MessageEndpoint constructor
            ResultHandle messageEndpoint =
                init.newInstance(
                    MethodDescriptor.ofConstructor(
                        MessageEndpoint.class, String.class, String.class),
                    init.load(endpointMetadata.getInboundDestination()),
                    init.load(endpointMetadata.getOutboundDestination()));

            // Load class for input parameter type
            String inputType = endpointMetadata.getMethodInfo().parameters().get(0).name();
            ResultHandle inputClass = init.loadClass(inputType);

            // Build wrapper bean
            ResultHandle wrapperBean =
                init.newInstance(
                    MethodDescriptor.ofConstructor(
                        MessageEndpointMethodWrapper.class,
                        MessageEndpoint.class,
                        Function.class.getName(),
                        Class.class),
                    messageEndpoint,
                    wrapper, // function reference (uses apply)
                    inputClass);

            // list.add(wrapperBean)
            init.invokeInterfaceMethod(
                MethodDescriptor.ofMethod(List.class, "add", boolean.class, Object.class),
                list,
                wrapperBean);
          }

          // map.put(destination, list)
          init.invokeInterfaceMethod(
              MethodDescriptor.ofMethod(Map.class, "put", Object.class, Object.class, Object.class),
              mapRef,
              init.load(inbound),
              list);
        }

        init.returnValue(null);
      }

      // Method: getMessageEndpoints(String destination)
      try (MethodCreator method =
          classCreator.getMethodCreator(
              MethodDescriptor.ofMethod(
                  REGISTRY_CLASS_NAME,
                  "getMessageEndpoints",
                  List.class.getName(),
                  String.class.getName()))) {
        ResultHandle destination = method.getMethodParam(0);
        ResultHandle mapRef = method.readInstanceField(registryField, method.getThis());

        ResultHandle result =
            method.invokeInterfaceMethod(
                MethodDescriptor.ofMethod(
                    Map.class, "getOrDefault", Object.class, Object.class, Object.class),
                mapRef,
                destination,
                method.invokeStaticMethod(
                    MethodDescriptor.ofMethod("java.util.Collections", "emptyList", List.class)));

        method.returnValue(result);
      }
    }
  }

  private static String buildFieldNameFromClassName(String className) {
    return className.substring(0, 0).toLowerCase() + className.substring(1);
  }
}

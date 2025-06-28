package com.github.deepend0.reactivestomp.messageendpoint.build;

import com.github.deepend0.reactivestomp.messageendpoint.MessageEndpoint;
import com.github.deepend0.reactivestomp.messageendpoint.MessageEndpointMethodWrapper;
import com.github.deepend0.reactivestomp.messageendpoint.MessageEndpointRegistry;
import io.quarkus.arc.deployment.GeneratedBeanBuildItem;
import io.quarkus.arc.deployment.GeneratedBeanGizmoAdaptor;
import io.quarkus.deployment.annotations.BuildProducer;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.builditem.CombinedIndexBuildItem;
import io.quarkus.gizmo.*;
import io.quarkus.gizmo.Type;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.jboss.jandex.*;
import org.objectweb.asm.Opcodes;


//TODOs
// Handle classes annotated with @MessageEndpoint
// Handle primitive types as method parameters
// Check if input parameter types are assignable
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

  @BuildStep
  public void generateMethodWrapperClasses(
      List<MessageEndpointMetadata> messageEndpointMetadataList,
      BuildProducer<GeneratedBeanBuildItem> generatedBeanBuildItemBuildProducer) {
    ClassOutput classOutput =
        new GeneratedBeanGizmoAdaptor(generatedBeanBuildItemBuildProducer);

    for (MessageEndpointMetadata metadata : messageEndpointMetadataList) {
      ClassInfo classInfo = metadata.getClassInfo();
      MethodInfo methodInfo = metadata.getMethodInfo();

      String className = classInfo.name().toString('_');

      Type inputType = Type.classType(methodInfo.parameters().get(0).type().name());

      org.jboss.jandex.Type methodReturnTypeJandex = methodInfo.returnType();
      List<MethodParameterInfo> methodParams = methodInfo.parameters();
      Type methodReturnType;

      if(methodReturnTypeJandex.name().equals(DotName.createSimple(Uni.class))
              || methodReturnTypeJandex.name().equals(DotName.createSimple(Multi.class))) {
        methodReturnType = Type.parameterizedType(
                Type.classType(methodReturnTypeJandex.name()),
                Type.classType(methodReturnTypeJandex.asParameterizedType().arguments().get(0).name()));
      } else {
        methodReturnType = Type.classType(methodReturnTypeJandex.name());
      }

      Type.ParameterizedType functionType =
          Type.parameterizedType(
              Type.classType(Function.class),
              inputType,
              methodReturnType);

      SignatureBuilder signatureBuilder = SignatureBuilder.forClass()
          .addInterface(functionType);
      try (ClassCreator classCreator =
          ClassCreator.builder()
              .classOutput(classOutput)
              .className(getWrapperClassName(className, methodInfo.name()))
              .superClass(Object.class)
              .interfaces(Function.class)
              .signature(signatureBuilder.build())
              .build()) {

        classCreator.addAnnotation(ApplicationScoped.class);

        String fieldName = buildFieldNameFromClassName(classInfo.simpleName());

        FieldCreator fieldCreator =  classCreator
                .getFieldCreator(fieldName, classInfo.name().toString())
                .setModifiers(Opcodes.ACC_PRIVATE);
        FieldDescriptor fieldDescriptor = fieldCreator.getFieldDescriptor();

        // Constructor
        try (MethodCreator constructorCreator = classCreator.getConstructorCreator(new String[]{})) {
          constructorCreator.invokeSpecialMethod(MethodDescriptor.ofConstructor(Object.class), constructorCreator.getThis());
          constructorCreator.setModifiers(Opcodes.ACC_PUBLIC);
          constructorCreator.returnValue(null);
        }

        //Create getter and setter for the injected field
        try (MethodCreator getter =
            classCreator.getMethodCreator("get" + fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1), classInfo.name().toString())) {
          getter.setModifiers(Opcodes.ACC_PUBLIC);
          ResultHandle instance = getter.readInstanceField(fieldDescriptor, getter.getThis());
          getter.returnValue(instance);
        }
        try (MethodCreator setter =
            classCreator.getMethodCreator("set" + fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1), void.class, classInfo.name().toString())) {
          setter.setModifiers(Opcodes.ACC_PUBLIC);
          setter.addAnnotation(Inject.class);
          ResultHandle instance = setter.getMethodParam(0);
          setter.writeInstanceField(fieldDescriptor, setter.getThis(), instance);
          setter.returnValue(null);
        }

        // Apply method
        if (methodParams.size() == 1) {
          String paramType = methodParams.get(0).type().name().toString();

          try (MethodCreator applyObject =
                       classCreator.getMethodCreator("apply", Object.class, Object.class)) {
            applyObject.setModifiers(Opcodes.ACC_PUBLIC);
            ResultHandle instance = applyObject.readInstanceField(fieldDescriptor, applyObject.getThis());
            ResultHandle param = applyObject.getMethodParam(0);

            // Cast input to the expected type
            ResultHandle castedInput = applyObject.checkCast(param, methodParams.getFirst().type().name().toString());

            // Call the original apply method
            ResultHandle result =
                    applyObject.invokeVirtualMethod(
                            MethodDescriptor.ofMethod(classInfo.name().toString(), methodInfo.name(), methodReturnTypeJandex.name().toString(), paramType),
                            instance,
                            castedInput);

            // Cast result to Object
            applyObject.returnValue(applyObject.checkCast(result, methodReturnTypeJandex.name().toString()));
          }
        }
      }
    }
  }

  @BuildStep
  void generateRegistryClass(
      List<MessageEndpointMetadata> messageEndpointMetadataList,
      BuildProducer<GeneratedBeanBuildItem> generatedBeanProducer) {

    ClassOutput classOutput = new GeneratedBeanGizmoAdaptor(generatedBeanProducer);

    try (ClassCreator classCreator =
        ClassCreator.builder()
            .classOutput(classOutput)
            .className(REGISTRY_CLASS_NAME)
            .interfaces(MessageEndpointRegistry.class)
            .superClass(Object.class)
            .build()) {

      classCreator.addAnnotation(ApplicationScoped.class);

      // Inject wrapper fields + collect by inbound destination
      Map<String, List<MessageEndpointMetadata>> groupedMetadata = new HashMap<>();

      for (MessageEndpointMetadata meta : messageEndpointMetadataList) {
        groupedMetadata
            .computeIfAbsent(meta.getInboundDestination(), k -> new ArrayList<>())
            .add(meta);

        // Declare injected field
        String wrapperClass = getWrapperClassName(meta.getClassInfo().name().toString('_'), meta.getMethodInfo().name());
        String fieldName = getWrapperFieldName(meta.getClassInfo().name().toString('_'), meta.getMethodInfo().name());

        FieldCreator field =
            classCreator
                .getFieldCreator(fieldName, wrapperClass)
                .setModifiers(Opcodes.ACC_PRIVATE);

        //Create getter and setter for the injected field
        try (MethodCreator getter =
            classCreator.getMethodCreator("get" + fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1), wrapperClass)) {
          getter.setModifiers(Opcodes.ACC_PUBLIC);
          ResultHandle instance = getter.readInstanceField(field.getFieldDescriptor(), getter.getThis());
          getter.returnValue(instance);
        }

        try (MethodCreator setter =
            classCreator.getMethodCreator("set" + fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1), void.class, wrapperClass)) {
          setter.setModifiers(Opcodes.ACC_PUBLIC);
          setter.addAnnotation(Inject.class);
          ResultHandle instance = setter.getMethodParam(0);
          setter.writeInstanceField(field.getFieldDescriptor(), setter.getThis(), instance);
          setter.returnValue(null);
        }
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
              .getFieldCreator("registry", Map.class)
              .setSignature(SignatureBuilder.forField().setType(registryType).build())
              .setModifiers(Opcodes.ACC_PUBLIC | Opcodes.ACC_FINAL)
              .getFieldDescriptor();

      // Constructor
      try (MethodCreator constructorCreator = classCreator.getConstructorCreator(new String[]{})) {
        constructorCreator.invokeSpecialMethod(MethodDescriptor.ofConstructor(Object.class), constructorCreator.getThis());
        ResultHandle newMap = constructorCreator.newInstance(MethodDescriptor.ofConstructor(HashMap.class));
        constructorCreator.writeInstanceField(registryField, constructorCreator.getThis(), newMap);
        constructorCreator.returnValue(null);
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
            String fieldClass = getWrapperClassName(endpointMetadata.getClassInfo().name().toString('_'),
                    endpointMetadata.getMethodInfo().name());
            String fieldName = getWrapperFieldName(endpointMetadata.getClassInfo().name().toString('_'),
                    endpointMetadata.getMethodInfo().name());

            // Get wrapper field
            ResultHandle wrapper =
                init.readInstanceField(
                    FieldDescriptor.of(REGISTRY_CLASS_NAME, fieldName, fieldClass), init.getThis());

            // Load class for input parameter type
            String inputType = endpointMetadata.getMethodInfo().parameters().get(0).type().name().toString();

            org.jboss.jandex.Type returnType = endpointMetadata.getMethodInfo().returnType();

            org.jboss.jandex.Type functionType = ParameterizedType.create(Function.class, endpointMetadata.getMethodInfo().parameters().get(0).type(), returnType);

            // Build wrapper
            ResultHandle methodWrapper =
                init.newInstance(
                    MethodDescriptor.ofConstructor(
                            MessageEndpointMethodWrapper.class,
                            String.class,
                            String.class,
                            DescriptorUtils.typeToString(functionType),
                            Class.class),
                    init.load(endpointMetadata.getInboundDestination()),
                    init.load(endpointMetadata.getOutboundDestination()),
                    init.checkCast(wrapper, DescriptorUtils.typeToString(functionType)), // function reference (uses apply)
                    init.loadClass(inputType));

            // list.add(wrapperBean)
            init.invokeInterfaceMethod(
                MethodDescriptor.ofMethod(List.class, "add", boolean.class, Object.class),
                list,
                methodWrapper);
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
    return className.substring(0, 1).toLowerCase() + className.substring(1);
  }

  private static String getWrapperClassName(String className, String methodName) {
    return "com.github.deepend0.reactivestomp.messageendpoint." + className + "_" + methodName + "Wrapper";
  }

  private static String getWrapperFieldName(String className, String methodName) {
    return buildFieldNameFromClassName(className + "_" + methodName + "Wrapper");
  }
}

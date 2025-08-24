package com.github.deepend0.reactivestomp.messaging.messageendpoint.buildstage;

import com.github.deepend0.reactivestomp.messaging.messageendpoint.*;
import io.quarkus.arc.deployment.GeneratedBeanBuildItem;
import io.quarkus.arc.deployment.GeneratedBeanGizmoAdaptor;
import io.quarkus.deployment.annotations.BuildProducer;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.builditem.CombinedIndexBuildItem;
import io.quarkus.gizmo.Type;
import io.quarkus.gizmo.*;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.jandex.*;
import org.objectweb.asm.Opcodes;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;


//TODOs
// Handle classes annotated with @MessageEndpoint
// Handle primitive types as method parameters
// Check if input parameter types are assignable
public class MessageEndpointAnnotationProcessor {

  private static final String REGISTRY_CLASS_NAME =
          MessageEndpoint.class.getPackageName() + "." + "MessageEndpointRegistryImpl";

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
          AnnotationValue outboundDestinationValue = annotationInstance.value("outboundDestination");
          String outboundDestination = outboundDestinationValue != null ? outboundDestinationValue.asString() : null;
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

      Type.ParameterizedType bifunctionType =
          Type.parameterizedType(
              Type.classType(BiFunction.class),
              Type.classType(Map.class),
              inputType,
              methodReturnType);

      SignatureBuilder signatureBuilder = SignatureBuilder.forClass()
          .addInterface(bifunctionType);
      try (ClassCreator classCreator =
          ClassCreator.builder()
              .classOutput(classOutput)
              .className(getWrapperClassName(className, methodInfo.name()))
              .superClass(Object.class)
              .interfaces(BiFunction.class)
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

        try (MethodCreator applyMethod =
                     classCreator.getMethodCreator("apply", Object.class, Object.class, Object.class)) {
          applyMethod.setModifiers(Opcodes.ACC_PUBLIC);
          ResultHandle instance = applyMethod.readInstanceField(fieldDescriptor, applyMethod.getThis());
          ResultHandle paramMap = applyMethod.getMethodParam(0); // Map<String, String>
          ResultHandle paramInput = applyMethod.getMethodParam(1); // Payload input

          // Build argument array for method call
          List<ResultHandle> args = new ArrayList<>();

          for (MethodParameterInfo paramInfo : methodParams) {
            String paramTypeName = paramInfo.type().name().toString();
            String paramName = paramInfo.name();
            if (paramInfo.hasAnnotation(DotName.createSimple(PathParam.class.getName()))) {
              // Lookup value from map

              ResultHandle rawValue = applyMethod.invokeInterfaceMethod(
                      MethodDescriptor.ofMethod(Map.class, "get", Object.class, Object.class),
                      paramMap,
                      applyMethod.load(paramName)
              );

              // Cast String to target type
              ResultHandle castedValue = castStringToType(applyMethod, rawValue, paramTypeName);
              args.add(castedValue);

            } else if (paramInfo.hasAnnotation(DotName.createSimple(Payload.class.getName()))) {
              // Use paramInput → cast to expected type
              ResultHandle castedPayload = applyMethod.checkCast(paramInput, paramTypeName);
              args.add(castedPayload);

            } else {
              ResultHandle castedPayload = applyMethod.checkCast(paramInput, paramTypeName);
              args.add(castedPayload);
            }
          }

          // Call the original method with assembled args
          ResultHandle result =
                  applyMethod.invokeVirtualMethod(
                          MethodDescriptor.ofMethod(
                                  classInfo.name().toString(),
                                  methodInfo.name(),
                                  methodReturnTypeJandex.name().toString(),
                                  methodParams.stream().map(p -> p.type().name().toString()).toArray(String[]::new)
                          ),
                          instance,
                          args.toArray(new ResultHandle[args.size()])
                  );

          // Cast result to Object
          applyMethod.returnValue(applyMethod.checkCast(result, methodReturnTypeJandex.name().toString()));
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

      Type registryType = Type.classType(PathHandlerRouter.class);

      // Registry field
      FieldDescriptor registryField =
          classCreator
              .getFieldCreator("registry", PathHandlerRouter.class)
              .setSignature(SignatureBuilder.forField().setType(registryType).build())
              .setModifiers(Opcodes.ACC_PUBLIC | Opcodes.ACC_FINAL)
              .getFieldDescriptor();

      // Constructor
      try (MethodCreator constructorCreator = classCreator.getConstructorCreator(new String[]{})) {
        constructorCreator.invokeSpecialMethod(MethodDescriptor.ofConstructor(Object.class), constructorCreator.getThis());
        ResultHandle newPathHandlerRouter = constructorCreator.newInstance(MethodDescriptor.ofConstructor(PathHandlerRouter.class));
        constructorCreator.writeInstanceField(registryField, constructorCreator.getThis(), newPathHandlerRouter);
        constructorCreator.returnValue(null);
      }

      // Move the post construct method in constructor
      // @PostConstruct init() to fill the map
      try (MethodCreator init = classCreator.getMethodCreator("init", void.class)) {

        init.addAnnotation(PostConstruct.class);
        ResultHandle registryRef = init.readInstanceField(registryField, init.getThis());

        for (Map.Entry<String, List<MessageEndpointMetadata>> entry : groupedMetadata.entrySet()) {
          String inbound = entry.getKey();
          List<MessageEndpointMetadata> endpointMetadataList = entry.getValue();

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

            org.jboss.jandex.Type mapType = org.jboss.jandex.Type.create(Map.class);
            org.jboss.jandex.Type returnType = endpointMetadata.getMethodInfo().returnType();
            org.jboss.jandex.Type payloadType = endpointMetadata.getMethodInfo().parameters()
                    .stream()
                    .filter(methodParameterInfo -> methodParameterInfo.hasAnnotation(Payload.class))
                    .findFirst()
                    .orElse(endpointMetadata.getMethodInfo().parameters()
                            .stream()
                            .filter(methodParameterInfo -> !methodParameterInfo.hasAnnotation(PathParam.class)).findFirst().get()).type();
            org.jboss.jandex.Type bifunctionType = ParameterizedType.create(BiFunction.class, mapType, payloadType, returnType);

            Boolean wrappedResponse = false;
            DotName messageEndpointResponseName = DotName.createSimple(MessageEndpointResponse.class.getName());
            DotName uniName = DotName.createSimple(Uni.class.getName());
            DotName multiName = DotName.createSimple(Multi.class.getName());
            DotName returnTypeName = returnType.name();

            if (returnTypeName.equals(messageEndpointResponseName)) {
              wrappedResponse = true;
            } else if (returnTypeName.equals(uniName)) {
              ParameterizedType parameterizedType = returnType.asParameterizedType();
              org.jboss.jandex.Type genericArg = parameterizedType.arguments().getFirst();
              wrappedResponse = genericArg.name().equals(messageEndpointResponseName);
            } else if (returnTypeName.equals(multiName)) {
              ParameterizedType parameterizedType = returnType.asParameterizedType();
              org.jboss.jandex.Type genericArg = parameterizedType.arguments().getFirst();
              wrappedResponse = genericArg.name().equals(messageEndpointResponseName);
            }

            // Build wrapper
            ResultHandle methodWrapper =
                init.newInstance(
                    MethodDescriptor.ofConstructor(
                            MessageEndpointMethodWrapper.class,
                            String.class,
                            String.class,
                            DescriptorUtils.typeToString(bifunctionType),
                            Class.class,
                            Boolean.class),
                    init.load(endpointMetadata.getInboundDestination()),
                    endpointMetadata.getOutboundDestination() == null? init.loadNull() : init.load(endpointMetadata.getOutboundDestination()),
                    init.checkCast(wrapper, DescriptorUtils.typeToString(bifunctionType)), // function reference (uses apply)
                    init.loadClass(inputType),
                    init.load(wrappedResponse));

            // registry.addPath
            init.invokeVirtualMethod(
                MethodDescriptor.ofMethod(PathHandlerRouter.class, "addPath", void.class, String.class, Object.class),
                registryRef,
                init.load(inbound),
                methodWrapper);
          }
        }

        init.returnValue(null);
      }

      // Method: getMessageEndpoints(String destination)
      try (MethodCreator method =
          classCreator.getMethodCreator(
              MethodDescriptor.ofMethod(
                  REGISTRY_CLASS_NAME,
                  "getMessageEndpoints",
                  PathHandlerRouter.MatchResult.class.getName(),
                  String.class.getName()))) {
        ResultHandle destination = method.getMethodParam(0);
        ResultHandle registryRef = method.readInstanceField(registryField, method.getThis());

        ResultHandle result =
            method.invokeVirtualMethod(
                MethodDescriptor.ofMethod(
                    PathHandlerRouter.class, "matchPath", PathHandlerRouter.MatchResult.class, String.class),
                    registryRef,
                destination);

        method.returnValue(result);
      }
    }
  }

  private static String buildFieldNameFromClassName(String className) {
    return className.substring(0, 1).toLowerCase() + className.substring(1);
  }

  private static String getWrapperClassName(String className, String methodName) {
    return MessageEndpoint.class.getPackageName() + "." + className + "_" + methodName + "Wrapper";
  }

  private static String getWrapperFieldName(String className, String methodName) {
    return buildFieldNameFromClassName(className + "_" + methodName + "Wrapper");
  }
  private ResultHandle castStringToType(MethodCreator mc, ResultHandle raw, String targetType) {
    switch (targetType) {
      case "int":
      case "java.lang.Integer":
        return mc.invokeStaticMethod(
                MethodDescriptor.ofMethod(Integer.class, "valueOf", Integer.class, String.class),
                raw
        );
      case "long":
      case "java.lang.Long":
        return mc.invokeStaticMethod(
                MethodDescriptor.ofMethod(Long.class, "valueOf", Long.class, String.class),
                raw
        );
      case "boolean":
      case "java.lang.Boolean":
        return mc.invokeStaticMethod(
                MethodDescriptor.ofMethod(Boolean.class, "valueOf", Boolean.class, String.class),
                raw
        );
      case "double":
      case "java.lang.Double":
        return mc.invokeStaticMethod(
                MethodDescriptor.ofMethod(Double.class, "valueOf", Double.class, String.class),
                raw
        );
      case "float":
      case "java.lang.Float":
        return mc.invokeStaticMethod(
                MethodDescriptor.ofMethod(Float.class, "valueOf", Float.class, String.class),
                raw
        );
      case "java.lang.String":
        return mc.checkCast(raw, String.class);

      default:
        // fallback: attempt cast (user-defined types)
        return mc.checkCast(raw, targetType);
    }
  }
}

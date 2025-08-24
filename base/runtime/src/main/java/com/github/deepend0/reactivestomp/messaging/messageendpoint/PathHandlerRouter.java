package com.github.deepend0.reactivestomp.messaging.messageendpoint;

import java.util.*;

public class PathHandlerRouter<T> {

    public static class Node<T> {
        Map<String, Node<T>> literalChildren = new HashMap<>();
        Node<T> paramChild = null;
        String paramName = null;
        T handler;
    }

    private final Node<T> root = new Node<>();

    public void addPath(String path, T handler) {
        String[] segments = normalize(path);
        Node<T> current = root;

        for (String seg : segments) {
            if (isParam(seg)) {
                String paramName = paramName(seg);
                if (current.paramChild == null) {
                    current.paramChild = new Node<>();
                    current.paramChild.paramName = paramName;
                }
                current = current.paramChild;
            } else {
                current = current.literalChildren.computeIfAbsent(seg, k -> new Node<>());
            }
        }
        if(current.handler != null) {
            throw new IllegalArgumentException("Handler already exists.");
        }
        current.handler = handler;
    }

    public MatchResult<T> matchPath(String path) {
        String[] segments = normalize(path);
        Map<String, String> params = new HashMap<>();
        Node<T> current = root;

        for (String seg : segments) {
            if (current.literalChildren.containsKey(seg)) {
                current = current.literalChildren.get(seg);
            } else if (current.paramChild != null) {
                params.put(current.paramChild.paramName, seg);
                current = current.paramChild;
            } else {
                return null; // no match
            }
        }

        if (current.handler != null) {
            return new MatchResult<>(current.handler, params);
        }
        return null;
    }

    private String[] normalize(String path) {
        return Arrays.stream(path.split("/"))
                .filter(s -> !s.isEmpty())
                .toArray(String[]::new);
    }

    private boolean isParam(String segment) {
        return segment.startsWith("{") && segment.endsWith("}");
    }

    private String paramName(String segment) {
        return segment.substring(1, segment.length() - 1);
    }

    public static class MatchResult<T> {
        private final T handler;
        private final Map<String, String> params;

        MatchResult(T handler, Map<String, String> params) {
            this.handler = handler;
            this.params = params;
        }

        public T getHandler() {
            return handler;
        }

        public Map<String, String> getParams() {
            return params;
        }
    }
}
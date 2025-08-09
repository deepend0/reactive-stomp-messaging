package com.github.deepend0.reactivestomp.test;

public class SampleObject {
    private String id;
    private Integer value;

    public SampleObject(String id, Integer value) {
        this.id = id;
        this.value = value;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Integer getValue() {
        return value;
    }

    public void setValue(Integer value) {
        this.value = value;
    }
}

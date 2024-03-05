package com.cisco.josouthe;

public class Metric {
    public Metric( String name, String kafkaId, String topic, String value) {
        this.name = String.format("%s|%s|%s",kafkaId, topic, name);
        this.value = value;
    }

    public String name,value;
}

package com.cisco.josouthe;

public class Metric {
    public Metric( String metricName, String name, String extraDescription, String value) {
        if( extraDescription.isEmpty() ) extraDescription = "|";
        this.name = String.format("%s|%s%s",name, extraDescription, metricName);
        if( value.endsWith(".0") ) value = value.substring(0, value.length()-2);

        this.value = value;
    }

    public String name,value;
}

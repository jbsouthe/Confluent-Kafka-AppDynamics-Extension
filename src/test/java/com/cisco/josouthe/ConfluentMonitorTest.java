package com.cisco.josouthe;

import com.singularity.ee.agent.systemagent.api.exception.TaskExecutionException;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ConfluentMonitorTest {
    private static final Logger logger = LogManager.getFormatterLogger();

    @org.junit.jupiter.api.Test
    void readConfluentConfiguration () throws TaskExecutionException {
        Configurator.setAllLevels("", Level.ALL);
        File testConfigFile = new File("Confluent-Config-test.json");
        if(!testConfigFile.exists()) {
            return; //don't test if a test file isn't there
        }

        ConfluentEndpoint[] confluentEndpoints = new ConfluentMonitor().readConfluentConfiguration(testConfigFile.getAbsolutePath());
        assert confluentEndpoints.length > 0;
    }

    @org.junit.jupiter.api.Test
    void execute () throws TaskExecutionException, IOException {
        Configurator.setAllLevels("", Level.ALL);
        File testConfigFile = new File("Confluent-Config-test.json");
        if(!testConfigFile.exists()) {
            return; //don't test if a test file isn't there
        }

        ConfluentMonitor monitor = new ConfluentMonitor();
        ConfluentEndpoint[] confluentEndpoints = monitor.readConfluentConfiguration(testConfigFile.getAbsolutePath());
        assert confluentEndpoints.length > 0;


        for( ConfluentEndpoint confluentEndpoint : confluentEndpoints ) {
            for( String dataset : new String[] {"cloud"}) {
                try {
                    List<Metric> metrics = monitor.getMetrics("cloud", confluentEndpoints[0] );
                    for( Metric metric : metrics) {
                        System.out.println("Metric: "+ metric.name +"="+ metric.value);
                    }
                    System.out.println(confluentEndpoint.name+"|"+"Cluster Count"+"="+ monitor.getClusterCount());
                    System.out.println(confluentEndpoint.name+"|"+"Topic Count"+"="+ monitor.getTopicCount());
                    for( String name : monitor.getSummationMap().keySet()) {
                        System.out.println(confluentEndpoint.name+"|"+"Total "+name+"="+ monitor.getSummationMap().get(name).longValue());
                    }
                } catch (IOException e) {
                    throw new TaskExecutionException("Exception in getMetrics: " + e);
                }
            }
        }


/*
        List<Metric> metrics = monitor.getMetrics("cloud", confluentEndpoints[0] );
        assert metrics.size() > 0;
        for( Metric metric : metrics )
            System.out.println("Metric: "+ metric.name +"="+ metric.value);

        metrics = monitor.getMetrics("cloud-custom", confluentEndpoints[0] );
        for( Metric metric : metrics )
            System.out.println("Metric: "+ metric.name +"="+ metric.value);

        System.out.println("Cluster Count: "+ monitor.getClusterCount());
        System.out.println("Topic Count: "+ monitor.getTopicCount());
        for( String name : monitor.getSummationMap().keySet())  System.out.println(name +" = "+ monitor.getSummationMap().get(name));

 */
    }
}
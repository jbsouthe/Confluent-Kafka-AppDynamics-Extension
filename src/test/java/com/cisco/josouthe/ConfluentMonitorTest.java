package com.cisco.josouthe;

import com.singularity.ee.agent.systemagent.api.exception.TaskExecutionException;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ConfluentMonitorTest {

    @org.junit.jupiter.api.Test
    void readConfluentConfiguration () throws TaskExecutionException {
        File testConfigFile = new File("Confluent-Config-test.json");
        if(!testConfigFile.exists()) {
            return; //don't test if a test file isn't there
        }

        ConfluentEndpoint[] confluentEndpoints = new ConfluentMonitor().readConfluentConfiguration(testConfigFile.getAbsolutePath());
        assert confluentEndpoints.length > 0;
    }

    @org.junit.jupiter.api.Test
    void execute () throws TaskExecutionException, IOException {
        File testConfigFile = new File("Confluent-Config-test.json");
        if(!testConfigFile.exists()) {
            return; //don't test if a test file isn't there
        }
        ConfluentEndpoint[] confluentEndpoints = new ConfluentMonitor().readConfluentConfiguration(testConfigFile.getAbsolutePath());
        assert confluentEndpoints.length > 0;

        List<Metric> metrics = new ConfluentMonitor().getMetrics("cloud", confluentEndpoints[0] );
        assert metrics.size() > 0;
        for( Metric metric : metrics )
            System.out.println("Metric: "+ metric.name +"="+ metric.value);
    }
}
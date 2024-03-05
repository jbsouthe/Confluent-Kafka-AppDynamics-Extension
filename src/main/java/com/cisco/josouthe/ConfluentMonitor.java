package com.cisco.josouthe;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.singularity.ee.agent.systemagent.api.AManagedMonitor;
import com.singularity.ee.agent.systemagent.api.MetricWriter;
import com.singularity.ee.agent.systemagent.api.TaskExecutionContext;
import com.singularity.ee.agent.systemagent.api.TaskOutput;
import com.singularity.ee.agent.systemagent.api.exception.TaskExecutionException;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ConfluentMonitor extends AManagedMonitor {
    private Logger logger = LogManager.getFormatterLogger();
    private String metricPrefix = "Custom Metrics|Confluent Kafka|";

    public ConfluentEndpoint[] readConfluentConfiguration( String configFileName ) throws TaskExecutionException {
        ConfluentEndpoint[] confluentEndpoints = null;
        try {
            Gson gson = new GsonBuilder().setPrettyPrinting().create();
            BufferedReader reader = new BufferedReader(new FileReader(configFileName));
            StringBuilder jsonFileContent = new StringBuilder();
            while (reader.ready()) {
                jsonFileContent.append(reader.readLine());
            }
            confluentEndpoints = gson.fromJson(jsonFileContent.toString(), ConfluentEndpoint[].class);
            if (confluentEndpoints != null) return confluentEndpoints;
        } catch (IOException exception) {
            logger.warn(String.format("Exception while reading the external file %s, message: %s", configFileName, exception));
        }
        throw new TaskExecutionException("Could not read Confluence Configuration JSON File: "+ configFileName);
    }

    @Override
    public TaskOutput execute (Map<String, String> configMap, TaskExecutionContext taskExecutionContext) throws TaskExecutionException {
        this.logger = taskExecutionContext.getLogger();
        ConfluentEndpoint[] confluentEndpoints = null;
        if( configMap.getOrDefault("ConfluentConfigFile","unconfigured").equals("unconfigured") ){
            throw new TaskExecutionException("Confluent Config File Not Set, nothing to do");
        } else {
            confluentEndpoints = readConfluentConfiguration(taskExecutionContext.getTaskDir() +"/"+ configMap.get("ConfluentConfigFile") );
            if( confluentEndpoints == null ) throw new TaskExecutionException("No End Points read from configuration, something must be wrong");
        }
        printMetric("up", 1,
                MetricWriter.METRIC_AGGREGATION_TYPE_OBSERVATION,
                MetricWriter.METRIC_TIME_ROLLUP_TYPE_SUM,
                MetricWriter.METRIC_CLUSTER_ROLLUP_TYPE_COLLECTIVE
        );

        for( ConfluentEndpoint confluentEndpoint : confluentEndpoints ) {
            for( String dataset : new String[] {"cloud", "cloud-custom"}) {
                try {
                    for( Metric metric : getMetrics(dataset, confluentEndpoint)) {
                        printMetricCurrent(metric);
                    }
                } catch (IOException e) {
                    throw new TaskExecutionException("Exception in getMetrics: " + e);
                }
            }
        }
        return null;
    }

    private String getApiToken( ConfluentEndpoint endpoint) {
        return Base64.getEncoder().encodeToString(String.format("%s:%s", endpoint.apiKey, endpoint.apiSecret).getBytes(StandardCharsets.UTF_8));
    }

    private OkHttpClient client = null;
    private static final Pattern METRIC_LINE_PATTERN = Pattern.compile("^(?<metricName>[a-zA-Z_][a-zA-Z0-9_]*)(?:\\{kafka_id=\"(?<kafkaId>[^\"]+)\",topic=\"(?<topic>[^\"]+)\"\\})\\s+(?<value>[-0-9.e+]+)\\s+(?<timestamp>[-0-9.e+]+)$\n");


    public List<Metric> getMetrics(String dataset, ConfluentEndpoint endpoint ) throws IOException {
        OkHttpClient client = null;
        try {
            client = getTrustAllCertsClient();
        } catch (KeyManagementException e) {
            throw new IOException("Key Management Exception: "+ e.getLocalizedMessage());
        } catch (NoSuchAlgorithmException e) {
            throw new IOException("No Such Algorithm Exception: "+ e.getLocalizedMessage());
        }
        String requestLine = String.format("%s/v2/metrics/%s/export?resource.kafka.id=%s", endpoint.url, dataset, endpoint.id);
        logger.debug("Request: GET "+ requestLine);
        Request request = new Request.Builder().url( requestLine )
                .method("GET", null)
                .addHeader("Authorization", "Bearer "+ getApiToken(endpoint))
                .addHeader("Content-Type", "application/openmetrics-text; version=1.0.0; charset=utf-8")
                .build();
        System.out.println("Request: "+ request);
        Response response = client.newCall(request).execute();
        return parseResponseMetrics( endpoint, response.body().string() );
    }

    private List<Metric> parseResponseMetrics (ConfluentEndpoint endpoint, String output) {
        List<Metric> metrics = new ArrayList<>();
        for( String line : output.split("\n")) {
            if( line.startsWith("#") || line.trim().isEmpty() ) continue;
            Matcher matcher = METRIC_LINE_PATTERN.matcher(line);
            String metricName = matcher.group("metricName");
            String kafkaId = matcher.group("kafkaId");
            String topic = matcher.group("topic");
            String value = matcher.group("value");
            metrics.add(new Metric(metricName, endpoint.name, topic, value));
        }
        return metrics;
    }

    private OkHttpClient getTrustAllCertsClient() throws NoSuchAlgorithmException, KeyManagementException {
        TrustManager[] trustAllCerts = new TrustManager[] { new X509TrustManager() {
            // @Override
            public void checkClientTrusted(java.security.cert.X509Certificate[] chain, String authType) {
            }

            // @Override
            public void checkServerTrusted(java.security.cert.X509Certificate[] chain, String authType) {
            }

            // @Override
            public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                return new java.security.cert.X509Certificate[] {};
            }
        } };

        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(null, trustAllCerts, new java.security.SecureRandom());

        OkHttpClient.Builder newBuilder = new OkHttpClient.Builder();
        newBuilder.sslSocketFactory(sslContext.getSocketFactory(), (X509TrustManager) trustAllCerts[0]);
        newBuilder.hostnameVerifier((hostname, session) -> true);
        return newBuilder.build();
    }

    public void printMetricCurrent( Metric metric ) {
        printMetricCurrent(metric.name, metric.value);
    }

    public void printMetricCurrent(String metricName, Object metricValue) {
        printMetric(metricName, metricValue,
                MetricWriter.METRIC_AGGREGATION_TYPE_OBSERVATION,
                MetricWriter.METRIC_TIME_ROLLUP_TYPE_CURRENT,
                MetricWriter.METRIC_CLUSTER_ROLLUP_TYPE_COLLECTIVE
        );
    }
    public void printMetricSum(String metricName, Object metricValue) {
        printMetric(metricName, metricValue,
                MetricWriter.METRIC_AGGREGATION_TYPE_OBSERVATION,
                MetricWriter.METRIC_TIME_ROLLUP_TYPE_SUM,
                MetricWriter.METRIC_CLUSTER_ROLLUP_TYPE_COLLECTIVE
        );
    }

    public void printMetric(String metricName, Object metricValue, String aggregation, String timeRollup, String cluster)
    {
        if( Utility.isDecimalNumber(String.valueOf(metricValue))){
            metricName += " (x100)";
            metricValue = Utility.decimalToLong(String.valueOf(metricValue));
        }
        logger.info(String.format("Print Metric: '%s%s'=%s",this.metricPrefix, metricName, metricValue));
        MetricWriter metricWriter = getMetricWriter(this.metricPrefix + metricName,
                aggregation,
                timeRollup,
                cluster
        );

        metricWriter.printMetric(String.valueOf(metricValue));
    }

}

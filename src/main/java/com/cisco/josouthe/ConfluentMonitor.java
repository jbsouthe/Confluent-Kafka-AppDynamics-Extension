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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ConfluentMonitor extends AManagedMonitor {
    private Logger logger = LogManager.getFormatterLogger();
    private String metricPrefix = "Custom Metrics|Confluent Kafka|";
    private Set<String> topicSet = new HashSet<>();
    private Set<String> kafkaIdSet = new HashSet<>();
    private Map<String,Double> summationMap = new HashMap<>();

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

        int totalTopicCount = 0;

        for( ConfluentEndpoint confluentEndpoint : confluentEndpoints ) {
            for( String dataset : new String[] {"cloud"}) {
                try {
                    for( Metric metric : getMetrics(dataset, confluentEndpoint)) {
                        printMetricCurrent(metric);
                    }
                    printMetricCurrent(confluentEndpoint.name+"|"+"Topic Count", topicSet.size());
                    for( String name : summationMap.keySet()) {
                        printMetricCurrent(confluentEndpoint.name+"|"+"Total "+name, summationMap.get(name).longValue());
                    }
                    totalTopicCount += topicSet.size();
                    topicSet.clear();
                    summationMap.clear();
                } catch (IOException e) {
                    throw new TaskExecutionException("Exception in getMetrics: " + e);
                }
            }
        }
        printMetricCurrent("Cluster Count", kafkaIdSet.size());
        printMetricCurrent("Topic Count", totalTopicCount);

        return null;
    }

    public int getClusterCount() {
        return kafkaIdSet.size();
    }

    public int getTopicCount() {
        return topicSet.size();
    }

    public Map<String,Double> getSummationMap() { return summationMap; }

    private String getApiToken( ConfluentEndpoint endpoint) {
        return Base64.getEncoder().encodeToString(String.format("%s:%s", endpoint.apiKey, endpoint.apiSecret).getBytes(StandardCharsets.UTF_8));
    }

    private OkHttpClient client = null;

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
        Request request = new Request.Builder().url( requestLine )
                .method("GET", null)
                .addHeader("Authorization", "Basic "+ getApiToken(endpoint))
                .addHeader("Content-Type", "application/openmetrics-text; version=1.0.0; charset=utf-8")
                .build();
        logger.debug("Request: "+ request);
        Response response = client.newCall(request).execute();
        return parseResponseMetrics( endpoint, response.body().string() );
    }

    private static final Pattern METRIC_LINE_PATTERN = Pattern.compile("^(?<metricName>[a-zA-Z_][a-zA-Z0-9_]*)\\{(?<extraDescription>.*)\\}\\s+(?<value>[-0-9.e+]+)\\s+(?<timestamp>\\d+)$");
    private static final Pattern METRIC_DESCRIPTION_PATTERN = Pattern.compile("(?<name>[\\w+]+)=\"(?<value>[^\"]+)\"");

    private List<Metric> parseResponseMetrics (ConfluentEndpoint endpoint, String output) {
        List<Metric> metrics = new ArrayList<>();
        for( String line : output.split("\n")) {
            logger.debug("line: "+ line);
            if( line.startsWith("#") || line.trim().isEmpty() ) continue;
            Matcher matcher = METRIC_LINE_PATTERN.matcher(line);
            if( matcher.matches() ){
                String metricName = matcher.group("metricName");
                String extraDescription = matcher.group("extraDescription");
                String metricPath = "";
                if( extraDescription != null && extraDescription.length() > 0) {
                    Matcher descriptionMatcher = METRIC_DESCRIPTION_PATTERN.matcher(extraDescription);
                    while(descriptionMatcher.find()) {
                        String dName = descriptionMatcher.group("name");
                        String dValue = descriptionMatcher.group("value");
                        metricPath += dName +"="+ dValue +"|";
                        switch (dName.toLowerCase()) {
                            case "topic": {
                                if( endpoint.ignoreHiddenTopics && dValue.startsWith("_") ) break;
                                topicSet.add(dValue);
                                break;
                            }
                            case "kafka_id": {
                                kafkaIdSet.add(dValue);
                                break;
                            }
                        }
                    }
                }
                String metricValue = matcher.group("value");
                metrics.add(new Metric(metricName, endpoint.name, metricPath, metricValue));
                addToSummation(metricName, metricValue);
            } else {
                logger.warn("line does not match: "+ line);
            }
        }
        return metrics;
    }

    private void addToSummation(String name, String value) {
        Double sum = summationMap.get(name);
        if( sum == null ) sum = new Double(0);
        sum += Double.valueOf(value);
        summationMap.put(name, sum);
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

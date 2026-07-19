package com.fdb.job.metrics;

import com.fdb.common.metrics.StageMetricSample;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;

public final class MetricSamplePublisher implements AutoCloseable {
    private static final long DEFAULT_CLOSE_TIMEOUT_MS = 5000L;
    private final KafkaProducer<String, String> producer;
    private final String topic;
    private final boolean enabled;
    private final long closeTimeoutMs;

    public MetricSamplePublisher() {
        this(
            System.getenv().getOrDefault("FDB_KAFKA_BOOTSTRAP", "localhost:9092"),
            System.getenv().getOrDefault("FDB_METRICS_TOPIC", "fdb-stage-metrics"),
            metricsEnabled(System.getenv(), System.getProperties()));
    }

    public MetricSamplePublisher(boolean enabled) {
        this(
            System.getenv().getOrDefault("FDB_KAFKA_BOOTSTRAP", "localhost:9092"),
            System.getenv().getOrDefault("FDB_METRICS_TOPIC", "fdb-stage-metrics"),
            enabled);
    }

    MetricSamplePublisher(String bootstrap, String topic, boolean enabled) {
        this.topic = topic;
        this.enabled = enabled;
        this.closeTimeoutMs = closeTimeoutMs(System.getenv(), System.getProperties());
        if (!enabled) {
            producer = null;
            return;
        }
        producer = new KafkaProducer<>(producerProperties(bootstrap, System.getenv(), System.getProperties()));
    }

    public void publish(StageMetricSample sample) {
        if (!enabled || producer == null) {
            return;
        }
        producer.send(new ProducerRecord<>(topic, sample.stageId(), sample.toJson()));
    }

    @Override
    public void close() {
        if (!enabled || producer == null) {
            return;
        }
        try {
            producer.close(Duration.ofMillis(closeTimeoutMs));
        } catch (RuntimeException ignored) {
            // Metrics are best effort and must not block operator cancellation.
        }
    }

    boolean enabled() {
        return enabled;
    }

    static boolean metricsEnabledFromEnv() {
        return metricsEnabled(System.getenv(), System.getProperties());
    }

    static boolean metricsEnabled(Map<String, String> env, Properties properties) {
        return MetricRuntimeConfig.metricsEnabled(env, properties);
    }

    static Properties producerProperties(String bootstrap, Map<?, ?> env, Properties properties) {
        Properties producerProperties = new Properties();
        producerProperties.put("bootstrap.servers", bootstrap);
        producerProperties.put("key.serializer", StringSerializer.class.getName());
        producerProperties.put("value.serializer", StringSerializer.class.getName());
        producerProperties.put("delivery.timeout.ms", Long.toString(resolveLong(
            env, properties, "FDB_METRICS_PRODUCER_DELIVERY_TIMEOUT_MS",
            "fdb.metrics.producer.delivery.timeout.ms", 10000L)));
        producerProperties.put("request.timeout.ms", Long.toString(resolveLong(
            env, properties, "FDB_METRICS_PRODUCER_REQUEST_TIMEOUT_MS",
            "fdb.metrics.producer.request.timeout.ms", 5000L)));
        producerProperties.put("max.block.ms", Long.toString(resolveLong(
            env, properties, "FDB_METRICS_PRODUCER_MAX_BLOCK_MS",
            "fdb.metrics.producer.max.block.ms", 5000L)));
        producerProperties.put("retries", Integer.toString((int) resolveLong(
            env, properties, "FDB_METRICS_PRODUCER_RETRIES",
            "fdb.metrics.producer.retries", 3L)));
        return producerProperties;
    }

    static long closeTimeoutMs(Map<String, String> env, Properties properties) {
        return resolveLong(env, properties, "FDB_METRICS_PRODUCER_CLOSE_TIMEOUT_MS",
            "fdb.metrics.producer.close.timeout.ms", DEFAULT_CLOSE_TIMEOUT_MS);
    }

    private static long resolveLong(Map<?, ?> env, Properties properties, String envName,
                                    String propertyName, long defaultValue) {
        Object configured = env.get(envName);
        String value = configured == null ? null : configured.toString();
        if (value == null || value.isBlank()) {
            value = properties.getProperty(propertyName);
        }
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        try {
            long parsed = Long.parseLong(value.trim());
            return parsed >= 0L ? parsed : defaultValue;
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }
}

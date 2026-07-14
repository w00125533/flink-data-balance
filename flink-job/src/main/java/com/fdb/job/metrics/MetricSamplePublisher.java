package com.fdb.job.metrics;

import com.fdb.common.metrics.StageMetricSample;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Map;
import java.util.Properties;

public final class MetricSamplePublisher implements AutoCloseable {
    private final KafkaProducer<String, String> producer;
    private final String topic;
    private final boolean enabled;

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
        if (!enabled) {
            producer = null;
            return;
        }
        Properties properties = new Properties();
        properties.put("bootstrap.servers", bootstrap);
        properties.put("key.serializer", StringSerializer.class.getName());
        properties.put("value.serializer", StringSerializer.class.getName());
        producer = new KafkaProducer<>(properties);
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
        producer.flush();
        producer.close();
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
}

package com.fdb.job.metrics;

import com.fdb.common.metrics.StageMetricSample;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public final class MetricSamplePublisher implements AutoCloseable {
    private final KafkaProducer<String, String> producer;
    private final String topic;

    public MetricSamplePublisher() {
        String bootstrap = System.getenv().getOrDefault("FDB_KAFKA_BOOTSTRAP", "localhost:9092");
        topic = System.getenv().getOrDefault("FDB_METRICS_TOPIC", "fdb-stage-metrics");
        Properties properties = new Properties();
        properties.put("bootstrap.servers", bootstrap);
        properties.put("key.serializer", StringSerializer.class.getName());
        properties.put("value.serializer", StringSerializer.class.getName());
        producer = new KafkaProducer<>(properties);
    }

    public void publish(StageMetricSample sample) {
        producer.send(new ProducerRecord<>(topic, sample.stageId(), sample.toJson()));
    }

    @Override
    public void close() {
        producer.flush();
        producer.close();
    }
}

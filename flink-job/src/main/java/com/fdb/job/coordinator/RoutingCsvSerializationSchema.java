package com.fdb.job.coordinator;

import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.nio.charset.StandardCharsets;

public class RoutingCsvSerializationSchema implements KafkaRecordSerializationSchema<String> {
    private final String topic;

    public RoutingCsvSerializationSchema(String topic) { this.topic = topic; }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(String csv, KafkaSinkContext context, Long timestamp) {
        String siteId = csv.substring(0, csv.indexOf(','));
        return new ProducerRecord<>(topic, siteId.getBytes(StandardCharsets.UTF_8),
            csv.getBytes(StandardCharsets.UTF_8));
    }
}

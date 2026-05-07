package com.fdb.topology;

import com.fdb.common.avro.TopologyRecord;
import com.fdb.common.kafka.AvroSerde;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;

public class KafkaTopologyPublisher {

    private static final Logger log = LoggerFactory.getLogger(KafkaTopologyPublisher.class);

    private final String topic;
    private final KafkaProducer<String, TopologyRecord> producer;

    public KafkaTopologyPublisher(String bootstrapServers, String topic) {
        this.topic = topic;
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        this.producer = new KafkaProducer<>(props, new StringSerializer(),
            AvroSerde.serializer(TopologyRecord.class));
    }

    public void publishAll(List<TopologyRecord> records) {
        log.info("Publishing {} topology records to topic '{}'", records.size(), topic);
        int count = 0;
        for (TopologyRecord record : records) {
            producer.send(new ProducerRecord<>(topic, record.getSiteId().toString(), record),
                (meta, ex) -> {
                    if (ex != null) {
                        log.error("Failed to publish topology for {}", record.getCellId(), ex);
                    }
                });
            count++;
            if (count % 1000 == 0) {
                producer.flush();
                log.info("Published {} / {} records", count, records.size());
            }
        }
        producer.flush();
        log.info("Published all {} topology records", records.size());
    }

    public void close() {
        producer.close();
    }
}

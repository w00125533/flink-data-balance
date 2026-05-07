package com.fdb.simulator;

import com.fdb.common.kafka.AvroSerde;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class KafkaPublisher<T extends SpecificRecord> implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(KafkaPublisher.class);

    private final KafkaProducer<String, T> producer;
    private final String topic;

    public KafkaPublisher(String bootstrapServers, String topic, Class<T> recordClass) {
        this.topic = topic;
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.LINGER_MS_CONFIG, "10");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, "32768");
        this.producer = new KafkaProducer<>(props, new StringSerializer(),
            AvroSerde.serializer(recordClass));
    }

    public void publish(String key, T record) {
        producer.send(new ProducerRecord<>(topic, key, record), (meta, ex) -> {
            if (ex != null) {
                log.warn("Failed to publish to {}: {}", topic, ex.getMessage());
            }
        });
    }

    public void flush() {
        producer.flush();
    }

    @Override
    public void close() {
        producer.close();
    }
}

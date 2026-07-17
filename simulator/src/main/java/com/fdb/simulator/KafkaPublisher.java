package com.fdb.simulator;

import com.fdb.common.kafka.AvroSerde;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

public class KafkaPublisher<T extends SpecificRecord> implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(KafkaPublisher.class);

    private final Sender<T> producer;
    private final String topic;
    private final AtomicLong submittedRecords = new AtomicLong();
    private final AtomicLong deliveredRecords = new AtomicLong();
    private final AtomicLong failedRecords = new AtomicLong();

    public KafkaPublisher(String bootstrapServers, String topic, Class<T> recordClass) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.LINGER_MS_CONFIG, "10");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, "32768");
        this.topic = topic;
        this.producer = new KafkaProducerSender<>(new KafkaProducer<>(props, new StringSerializer(),
            AvroSerde.serializer(recordClass)));
    }

    KafkaPublisher(String topic, Sender<T> producer) {
        this.topic = topic;
        this.producer = producer;
    }

    public void publish(String key, T record) {
        submittedRecords.incrementAndGet();
        try {
            producer.send(new ProducerRecord<>(topic, key, record), (meta, ex) -> {
                if (ex != null) {
                    failedRecords.incrementAndGet();
                    log.warn("Failed to publish to {}: {}", topic, ex.getMessage());
                    return;
                }
                deliveredRecords.incrementAndGet();
            });
        } catch (RuntimeException e) {
            failedRecords.incrementAndGet();
            throw e;
        }
    }

    public long submittedRecords() {
        return submittedRecords.get();
    }

    public long deliveredRecords() {
        return deliveredRecords.get();
    }

    public long failedRecords() {
        return failedRecords.get();
    }

    public long undeliveredRecords() {
        return Math.max(0L, submittedRecords() - deliveredRecords());
    }

    interface Sender<T extends SpecificRecord> extends AutoCloseable {
        void send(ProducerRecord<String, T> record, Callback callback);

        void flush();

        @Override
        void close();
    }

    private record KafkaProducerSender<T extends SpecificRecord>(
        KafkaProducer<String, T> producer) implements Sender<T> {
        @Override
        public void send(ProducerRecord<String, T> record, Callback callback) {
            producer.send(record, callback);
        }

        @Override
        public void flush() {
            producer.flush();
        }

        @Override
        public void close() {
            producer.close();
        }
    }

    public void flush() {
        producer.flush();
    }

    @Override
    public void close() {
        producer.close();
    }
}

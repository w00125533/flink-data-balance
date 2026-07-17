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
import java.util.concurrent.atomic.AtomicLong;

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

    public PublishResult publishAll(List<TopologyRecord> records) {
        log.info("Publishing {} topology records to topic '{}'", records.size(), topic);
        int count = 0;
        AtomicLong published = new AtomicLong();
        AtomicLong failed = new AtomicLong();
        for (TopologyRecord record : records) {
            try {
                producer.send(new ProducerRecord<>(topic, record.getCellId().toString(), record),
                    (meta, ex) -> {
                        if (ex != null) {
                            failed.incrementAndGet();
                            log.error("Failed to publish topology for {}", record.getCellId(), ex);
                        } else {
                            published.incrementAndGet();
                        }
                    });
            } catch (RuntimeException e) {
                failed.incrementAndGet();
                log.error("Failed to enqueue topology for {}", record.getCellId(), e);
            }
            count++;
            if (count % 1000 == 0) {
                producer.flush();
                log.info("Published {} / {} records", count, records.size());
            }
        }
        producer.flush();
        log.info("Published topology records attempted={} published={} failed={}",
            records.size(), published.get(), failed.get());
        return new PublishResult(records.size(), published.get(), failed.get());
    }

    public void close() {
        producer.close();
    }

    public record PublishResult(long attemptedRecords, long publishedRecords, long failedRecords) {
    }
}

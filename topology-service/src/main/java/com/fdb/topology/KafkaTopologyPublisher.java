package com.fdb.topology;

import com.fdb.common.avro.TopologyRecord;
import com.fdb.common.kafka.AvroSerde;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

public class KafkaTopologyPublisher {

    private static final Logger log = LoggerFactory.getLogger(KafkaTopologyPublisher.class);
    private static final Duration CLOSE_TIMEOUT = Duration.ofSeconds(5);
    private static final long DEFAULT_METADATA_TIMEOUT_MS = 120_000L;
    private static final long METADATA_RETRY_SLEEP_MS = 1_000L;
    private static final String DEFAULT_MAX_BLOCK_MS = "15000";
    private static final String DEFAULT_REQUEST_TIMEOUT_MS = "15000";
    private static final String DEFAULT_DELIVERY_TIMEOUT_MS = "30000";

    private final String topic;
    private final Sender producer;
    private final Sleeper sleeper;
    private final long metadataTimeoutMs;

    public KafkaTopologyPublisher(String bootstrapServers, String topic) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, env("FDB_TOPOLOGY_PRODUCER_MAX_BLOCK_MS",
            DEFAULT_MAX_BLOCK_MS));
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, env("FDB_TOPOLOGY_PRODUCER_REQUEST_TIMEOUT_MS",
            DEFAULT_REQUEST_TIMEOUT_MS));
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, env("FDB_TOPOLOGY_PRODUCER_DELIVERY_TIMEOUT_MS",
            DEFAULT_DELIVERY_TIMEOUT_MS));
        this.topic = topic;
        this.producer = new KafkaProducerSender(new KafkaProducer<>(props, new StringSerializer(),
            AvroSerde.serializer(TopologyRecord.class)));
        this.sleeper = Thread::sleep;
        this.metadataTimeoutMs = longEnv("FDB_TOPOLOGY_METADATA_TIMEOUT_MS", DEFAULT_METADATA_TIMEOUT_MS);
    }

    KafkaTopologyPublisher(String topic, Sender producer, Sleeper sleeper, long metadataTimeoutMs) {
        this.topic = topic;
        this.producer = producer;
        this.sleeper = sleeper;
        this.metadataTimeoutMs = metadataTimeoutMs;
    }

    public PublishResult publishAll(List<TopologyRecord> records) {
        log.info("Publishing {} topology records to topic '{}'", records.size(), topic);
        awaitTopicMetadata();
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
        producer.close(CLOSE_TIMEOUT);
    }

    public record PublishResult(long attemptedRecords, long publishedRecords, long failedRecords) {
    }

    interface Sender {
        List<PartitionInfo> partitionsFor(String topic);

        void send(ProducerRecord<String, TopologyRecord> record, Callback callback);

        void flush();

        void close(Duration timeout);
    }

    interface Sleeper {
        void sleep(long millis) throws InterruptedException;
    }

    private void awaitTopicMetadata() {
        long deadlineMs = System.currentTimeMillis() + Math.max(0L, metadataTimeoutMs);
        RuntimeException lastFailure = null;
        do {
            try {
                List<PartitionInfo> partitions = producer.partitionsFor(topic);
                if (partitions != null && !partitions.isEmpty()) {
                    return;
                }
            } catch (RuntimeException e) {
                lastFailure = e;
            }
            long remainingMs = deadlineMs - System.currentTimeMillis();
            if (remainingMs <= 0L) {
                break;
            }
            try {
                sleeper.sleep(Math.min(METADATA_RETRY_SLEEP_MS, remainingMs));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while waiting for topic metadata: " + topic, e);
            }
        } while (true);
        throw new IllegalStateException(
            "Topic metadata not visible within " + metadataTimeoutMs + "ms for topic " + topic,
            lastFailure);
    }

    private static String env(String key, String defaultValue) {
        String value = System.getenv(key);
        return value == null || value.isBlank() ? defaultValue : value.trim();
    }

    private static long longEnv(String key, long defaultValue) {
        String value = System.getenv(key);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    private record KafkaProducerSender(KafkaProducer<String, TopologyRecord> producer) implements Sender {
        @Override
        public List<PartitionInfo> partitionsFor(String topic) {
            return producer.partitionsFor(topic);
        }

        @Override
        public void send(ProducerRecord<String, TopologyRecord> record, Callback callback) {
            producer.send(record, callback);
        }

        @Override
        public void flush() {
            producer.flush();
        }

        @Override
        public void close(Duration timeout) {
            producer.close(timeout);
        }
    }
}

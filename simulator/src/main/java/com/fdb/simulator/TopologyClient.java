package com.fdb.simulator;

import com.fdb.common.avro.TopoCellType;
import com.fdb.common.avro.TopologyRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

public class TopologyClient implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(TopologyClient.class);

    private final KafkaConsumer<String, TopologyRecord> consumer;
    private final Map<String, List<TopologyRecord>> siteToCells = new ConcurrentHashMap<>();
    private final List<TopologyRecord> allCells = new CopyOnWriteArrayList<>();
    private volatile boolean running = true;
    private Thread pollThread;

    public TopologyClient(String bootstrapServers, String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        this.consumer = new KafkaConsumer<>(props, new StringDeserializer(),
            com.fdb.common.kafka.AvroSerde.deserializer(TopologyRecord.class));
    }

    public void start(String topic) {
        consumer.subscribe(Collections.singletonList(topic));
        pollThread = new Thread(() -> {
            while (running) {
                ConsumerRecords<String, TopologyRecord> records = consumer.poll(Duration.ofSeconds(1));
                records.forEach(record -> {
                    TopologyRecord tr = record.value();
                    allCells.add(tr);
                    siteToCells.computeIfAbsent(tr.getSiteId().toString(), k -> new CopyOnWriteArrayList<>()).add(tr);
                });
            }
        }, "topology-client");
        pollThread.setDaemon(true);
        pollThread.start();
    }

    public List<TopologyRecord> getAllCells() {
        return List.copyOf(allCells);
    }

    public List<TopologyRecord> getCellsForSite(String siteId) {
        return siteToCells.getOrDefault(siteId, List.of());
    }

    public int getCellCount() { return allCells.size(); }

    public boolean isReady() { return !allCells.isEmpty(); }

    public void awaitReady(Duration timeout) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            if (isReady()) return;
            Thread.sleep(100);
        }
        throw new IllegalStateException("Timeout waiting for topology data");
    }

    @Override
    public void close() {
        running = false;
        if (pollThread != null) {
            try { pollThread.join(2000); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
        consumer.close();
    }
}

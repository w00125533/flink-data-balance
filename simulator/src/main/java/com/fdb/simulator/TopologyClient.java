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
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class TopologyClient implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(TopologyClient.class);
    private static final long READY_QUIET_MS = 1000L;

    private final KafkaConsumer<String, TopologyRecord> consumer;
    private final Map<String, List<TopologyRecord>> siteToCells = new ConcurrentHashMap<>();
    private final Map<String, TopologyRecord> cellsById = new ConcurrentHashMap<>();
    private volatile boolean running = true;
    private volatile long lastRecordAt = 0;
    private Thread pollThread;

    public TopologyClient(String bootstrapServers, String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId + "-" + UUID.randomUUID());
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
                    String cellId = tr.getCellId().toString();
                    String siteId = tr.getSiteId().toString();
                    TopologyRecord previous = cellsById.put(cellId, tr);
                    if (previous != null && !previous.getSiteId().toString().equals(siteId)) {
                        siteToCells.computeIfPresent(previous.getSiteId().toString(), (ignored, cells) -> {
                            cells.removeIf(cell -> cell.getCellId().toString().equals(cellId));
                            return cells;
                        });
                    }
                    siteToCells.computeIfAbsent(siteId, ignored -> new CopyOnWriteArrayList<>())
                        .removeIf(cell -> cell.getCellId().toString().equals(cellId));
                    siteToCells.get(siteId).add(tr);
                    lastRecordAt = System.currentTimeMillis();
                });
            }
        }, "topology-client");
        pollThread.setDaemon(true);
        pollThread.start();
    }

    public List<TopologyRecord> getAllCells() {
        return List.copyOf(cellsById.values());
    }

    public List<TopologyRecord> getCellsForSite(String siteId) {
        return siteToCells.getOrDefault(siteId, List.of());
    }

    public int getCellCount() { return cellsById.size(); }

    public boolean isReady() {
        return isReady(0);
    }

    public boolean isReady(int expectedCells) {
        return isReady(cellsById.size(), lastRecordAt, System.currentTimeMillis(), expectedCells);
    }

    static boolean isReady(int loadedCells, long lastRecordAtMs, long nowMs, int expectedCells) {
        if (loadedCells <= 0 || lastRecordAtMs <= 0) {
            return false;
        }
        if (expectedCells > 0 && loadedCells < expectedCells) {
            return false;
        }
        return nowMs - lastRecordAtMs >= READY_QUIET_MS;
    }

    public void awaitReady(Duration timeout) throws InterruptedException {
        awaitReady(timeout, 0);
    }

    public void awaitReady(Duration timeout, int expectedCells) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            if (isReady(expectedCells)) return;
            Thread.sleep(100);
        }
        String expected = expectedCells > 0 ? "/" + expectedCells : "";
        throw new IllegalStateException(
            "Timeout waiting for topology data: loaded " + cellsById.size() + expected + " cells");
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

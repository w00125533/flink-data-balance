package com.fdb.simulator;

import com.fdb.common.avro.MrStat;
import com.fdb.common.avro.TopologyRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Random;

public class MrSimulator {

    private static final Logger log = LoggerFactory.getLogger(MrSimulator.class);

    private final String configPath;
    private final Random rng = new Random(43);

    public MrSimulator(String configPath) {
        this.configPath = configPath;
    }

    public void run() throws Exception {
        String bootstrap = System.getenv().getOrDefault("FDB_KAFKA_BOOTSTRAP", "localhost:9092");
        String topic = "mr-stats";

        TopologyClient topology = new TopologyClient(bootstrap, "sim-mr");
        topology.start("topology");
        topology.awaitReady(Duration.ofSeconds(30));

        List<TopologyRecord> cells = topology.getAllCells();
        log.info("Loaded {} cells from topology for MR simulator", cells.size());

        try (KafkaPublisher<MrStat> publisher = new KafkaPublisher<>(bootstrap, topic, MrStat.class)) {
            long startTime = System.currentTimeMillis();

            while (!Thread.currentThread().isInterrupted()) {
                long wallNow = System.currentTimeMillis();
                long windowEnd = alignTo10s(wallNow);
                long windowStart = windowEnd - 10_000;

                for (TopologyRecord cell : cells) {
                    MrStat stat = generateMrStat(cell, windowStart, windowEnd);
                    publisher.publish(cell.getSiteId().toString(), stat);
                }

                publisher.flush();

                long nextBoundary = windowEnd + 10_000;
                long sleepMs = nextBoundary - System.currentTimeMillis();
                if (sleepMs > 0) Thread.sleep(sleepMs);
            }
        }
    }

    private MrStat generateMrStat(TopologyRecord cell, long windowStart, long windowEnd) {
        double distKm = haversine(cell.getSiteLat(), cell.getSiteLon(),
            cell.getSiteLat() + rng.nextGaussian() * 0.001,
            cell.getSiteLon() + rng.nextGaussian() * 0.001);
        double maxDist = cell.getCoverageRadiusM() / 1000.0;
        double load = Math.max(0.1, Math.min(0.95, 0.3 + (1 - distKm / Math.max(maxDist, 0.1)) * 0.5 + rng.nextGaussian() * 0.1));

        int activeUsers = (int) (load * 200 + rng.nextInt(20));
        int totalConnections = activeUsers + rng.nextInt(50);
        int dropped = rng.nextInt(Math.max(1, totalConnections / 20));
        int hoAttempt = rng.nextInt(50);
        int hoFail = rng.nextInt(Math.max(1, hoAttempt / 3 + 1));
        int prachAttempt = rng.nextInt(100);
        int prachFail = rng.nextInt(Math.max(1, prachAttempt / 5 + 1));
        int rrcAttempt = rng.nextInt(60);
        int rrcSuccess = rrcAttempt - rng.nextInt(Math.max(1, rrcAttempt / 10 + 1));
        float latency = (float) (5 + load * 40 + rng.nextFloat() * 10);

        return MrStat.newBuilder()
            .setSiteId(cell.getSiteId().toString())
            .setCellId(cell.getCellId().toString())
            .setWindowStartTs(windowStart)
            .setWindowEndTs(windowEnd)
            .setPrbUsageDl((float) Math.min(1.0, load + rng.nextFloat() * 0.1))
            .setPrbUsageUl((float) Math.min(1.0, load * 0.6 + rng.nextFloat() * 0.1))
            .setActiveUsers(activeUsers)
            .setAvgRsrp((float) (-80 - (1 - (float) load) * 40 + rng.nextFloat() * 5))
            .setAvgRsrq((float) (-5 - (1 - (float) load) * 12 + rng.nextFloat() * 3))
            .setAvgSinr((float) (load * 20 + rng.nextFloat() * 5))
            .setAvgCqi((float) (load * 12 + rng.nextFloat() * 3))
            .setAvgMcs((float) (load * 22 + rng.nextFloat() * 5))
            .setAvgBler(rng.nextFloat() * 0.1f)
            .setThroughputDlMbps((float) (load * 200 + rng.nextFloat() * 50))
            .setThroughputUlMbps((float) (load * 50 + rng.nextFloat() * 20))
            .setDroppedConnections(dropped)
            .setHandoverSuccess(hoAttempt - hoFail)
            .setHandoverFailure(hoFail)
            .setPrachAttempt(prachAttempt)
            .setPrachFailure(prachFail)
            .setRrcEstabAttempt(rrcAttempt)
            .setRrcEstabSuccess(rrcSuccess)
            .setAvgLatencyMs(latency)
            .setPacketLossRate(rng.nextFloat() * 0.02f)
            .build();
    }

    private static long alignTo10s(long ts) {
        return (ts / 10_000) * 10_000;
    }

    private static double haversine(double lat1, double lon1, double lat2, double lon2) {
        double dLat = Math.toRadians(lat2 - lat1);
        double dLon = Math.toRadians(lon2 - lon1);
        double a = Math.sin(dLat / 2) * Math.sin(dLat / 2) +
            Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) *
                Math.sin(dLon / 2) * Math.sin(dLon / 2);
        return 6371 * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
    }
}

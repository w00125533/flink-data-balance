package com.fdb.simulator;

import com.fdb.common.avro.*;
import com.fdb.common.summary.SummarySwitch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalTime;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ChrSimulator {

    private static final Logger log = LoggerFactory.getLogger(ChrSimulator.class);
    private static final long DEFAULT_LOOP_INTERVAL_MS = 100L;

    private static final List<String> IMSI_PREFIXES = List.of("46000", "46001", "46002", "46003", "46004");
    private static final List<String> IMEI_POOL = IntStream.range(0, 500)
        .mapToObj(i -> String.format("86%014d", ThreadLocalRandom.current().nextLong(1_000_000_000_000L)))
        .collect(Collectors.toList());

    private final String configPath;
    private final Random rng = new Random(42);

    public ChrSimulator(String configPath) {
        this.configPath = configPath;
    }

    public void run() throws Exception {
        SimulatorConfig config = SimulatorConfig.load("sim-chr.yaml", configPath);
        String bootstrap = config.bootstrap();
        String topic = config.topic("chr-events");

        TopologyClient topology = new TopologyClient(bootstrap, "sim-chr");
        topology.start(config.topologyTopic());
        topology.awaitReady(java.time.Duration.ofSeconds(30));

        List<TopologyRecord> cells = topology.getAllCells();
        log.info("Loaded {} cells from topology", cells.size());

        Map<String, List<String>> cellUsers = assignUsers(cells);
        boolean summaryEnabled = SummarySwitch.enabled();
        if (summaryEnabled) {
            int assignedUsers = cellUsers.values().stream().mapToInt(List::size).sum();
            long sites = cells.stream().map(c -> c.getSiteId().toString()).distinct().count();
            log.info(SummarySwitch.format("sim-chr", "loaded_sites", sites));
            log.info(SummarySwitch.format("sim-chr", "loaded_cells", cells.size()));
            log.info(SummarySwitch.format("sim-chr", "assigned_users", assignedUsers));
        }
        long baseEps = config.getLong("rate.eps", 5000);
        long totalCells = cells.size();
        double lambdaPerCell = (double) baseEps / totalCells;
        SourceMetricsWriter sourceMetrics = new SourceMetricsWriter("FDB_CHR_METRICS_FILE");
        if (summaryEnabled) {
            log.info(SummarySwitch.format("sim-chr", "configured_base_eps", baseEps));
            log.info(SummarySwitch.format("sim-chr", "lambda_per_cell", String.format("%.3f", lambdaPerCell)));
        }

        try (KafkaPublisher<ChrEvent> publisher = new KafkaPublisher<>(bootstrap, topic, ChrEvent.class)) {
            long startTime = System.currentTimeMillis();
            long counter = 0;

            while (!Thread.currentThread().isInterrupted()) {
                long now = System.currentTimeMillis();
                long due = dueEvents(baseEps, startTime, now, counter);
                for (long i = 0; i < due; i++) {
                    TopologyRecord cell = cells.get(rng.nextInt(cells.size()));
                    List<String> users = cellUsers.get(cell.getCellId().toString());
                    if (users == null || users.isEmpty()) {
                        continue;
                    }

                    String imsi = users.get(rng.nextInt(users.size()));
                    ChrEvent event = generateEvent(cell, imsi, now,
                        config.getDouble("rate.outOfOrderProb", 0.05),
                        config.getLong("rate.maxOutOfOrderLagMs", 5000));
                    publisher.publish(cell.getSiteId().toString(), event);
                    counter++;

                    if (counter % 10000 == 0) {
                        publisher.flush();
                        double eps = counter / ((System.currentTimeMillis() - startTime) / 1000.0 + 1);
                        log.info("Published {} CHR events (EPS: {})", counter, eps);
                        if (summaryEnabled) {
                            log.info(SummarySwitch.format("sim-chr", "events_published", counter));
                            log.info(SummarySwitch.format("sim-chr", "observed_eps", String.format("%.1f", eps)));
                        }
                    }
                }
                long metricsNow = System.currentTimeMillis();
                long durationMs = Math.max(0L, metricsNow - startTime);
                long delivered = publisher.deliveredRecords();
                double observedEps = delivered / Math.max(durationMs / 1000.0d, 0.001d);
                long expected = expectedEvents(baseEps, startTime, metricsNow);
                long pending = backlogRecords(expected, counter);
                sourceMetrics.write("chr", baseEps, delivered, observedEps, durationMs,
                    pending, undeliveredRecords(counter, delivered));

                long elapsed = System.currentTimeMillis() - now;
                if (elapsed < DEFAULT_LOOP_INTERVAL_MS) Thread.sleep(DEFAULT_LOOP_INTERVAL_MS - elapsed);
            }
        }
    }

    static long dueEvents(long targetEps, long startMs, long nowMs, long alreadyPublished) {
        long expected = expectedEvents(targetEps, startMs, nowMs);
        return Math.max(0L, expected - alreadyPublished);
    }

    static long expectedEvents(long targetEps, long startMs, long nowMs) {
        if (targetEps <= 0 || nowMs <= startMs) {
            return 0L;
        }
        return Math.floorDiv((nowMs - startMs) * targetEps, 1000L);
    }

    static long backlogRecords(long expected, long submitted) {
        return Math.max(0L, expected - submitted);
    }

    static long undeliveredRecords(long submitted, long delivered) {
        return Math.max(0L, submitted - delivered);
    }

    private Map<String, List<String>> assignUsers(List<TopologyRecord> cells) {
        Map<String, List<String>> result = new HashMap<>();
        int userCounter = 0;
        for (TopologyRecord cell : cells) {
            int numUsers = 50 + rng.nextInt(200);
            List<String> users = new ArrayList<>(numUsers);
            for (int i = 0; i < numUsers; i++) {
                users.add(imsiFromCounter(userCounter++));
            }
            result.put(cell.getCellId().toString(), users);
        }
        return result;
    }

    private String imsiFromCounter(int counter) {
        String prefix = IMSI_PREFIXES.get(counter % IMSI_PREFIXES.size());
        return prefix + String.format("%010d", counter);
    }

    private double computeSkew(TopologyRecord cell, long elapsedMs) {
        double staticWeight = 1.0;

        double lat = cell.getSiteLat();
        double lon = cell.getSiteLon();
        double cbdLat = 39.91, cbdLon = 116.40;
        double distKm = haversine(lat, lon, cbdLat, cbdLon);
        if (distKm < 5.0) staticWeight = 5.0;
        else if (distKm < 10.0) staticWeight = 2.5;

        LocalTime now = LocalTime.now();
        int hour = now.getHour();
        double diurnalWeight = 1.0;
        if (hour >= 7 && hour < 10) diurnalWeight = 2.5;
        else if (hour >= 17 && hour < 20) diurnalWeight = 3.0;
        else if (hour >= 0 && hour < 5) diurnalWeight = 0.3;

        double noise = 0.85 + rng.nextDouble() * 0.3;

        return staticWeight * diurnalWeight * noise;
    }

    private ChrEvent generateEvent(TopologyRecord cell, String imsi, long now,
                                   double outOfOrderProbability, long maxOutOfOrderLagMs) {
        double distKm = haversine(cell.getSiteLat(), cell.getSiteLon(),
            cell.getSiteLat() + rng.nextGaussian() * 0.001,
            cell.getSiteLon() + rng.nextGaussian() * 0.001);
        double maxDist = cell.getCoverageRadiusM() / 1000.0;
        double signalQuality = Math.max(0, 1.0 - (distKm / Math.max(maxDist, 0.1)));

        float rsrp = (float) (-80 - (1 - signalQuality) * 50 + rng.nextGaussian() * 5);
        float sinr = (float) (signalQuality * 25 + rng.nextGaussian() * 3);
        float rsrq = (float) (-5 - (1 - signalQuality) * 10 + rng.nextGaussian() * 2);

        int cqi = Math.max(0, Math.min(15, (int) (signalQuality * 15 + rng.nextGaussian())));
        int mcs = Math.max(0, Math.min(28, (int) (signalQuality * 28 + rng.nextGaussian())));

        ChrEventType[] eventTypes = ChrEventType.values();
        ChrEventType eventType = eventTypes[rng.nextInt(eventTypes.length)];

        int outOfOrderLag = rng.nextDouble() < outOfOrderProbability
            ? rng.nextInt((int) Math.max(1, maxOutOfOrderLagMs)) + 100 : 0;

        return ChrEvent.newBuilder()
            .setChrId(UUID.randomUUID().toString())
            .setEventTs(now - outOfOrderLag)
            .setImsi(imsi)
            .setImei(IMEI_POOL.get(rng.nextInt(IMEI_POOL.size())))
            .setSiteId(cell.getSiteId().toString())
            .setCellId(cell.getCellId().toString())
            .setEventType(eventType)
            .setRatType(RatType.NR_SA)
            .setPci(cell.getPci())
            .setTac(cell.getTac())
            .setEci(cell.getEci())
            .setMcc(cell.getMcc().toString())
            .setMnc(cell.getMnc().toString())
            .setArfcn(cell.getArfcn())
            .setResultCode(eventType == ChrEventType.RRC_SETUP_FAIL || eventType == ChrEventType.DETACH ? 1 : 0)
            .setLatitude(cell.getSiteLat() + rng.nextGaussian() * 0.002)
            .setLongitude(cell.getSiteLon() + rng.nextGaussian() * 0.002)
            .setRsrp(rsrp)
            .setRsrq(rsrq)
            .setSinr(sinr)
            .setCqi(cqi)
            .setMcs(mcs)
            .setDurationMs(eventType == ChrEventType.DATA_SESSION ? (long) (1000 + rng.nextDouble() * 30000) : null)
            .setBytesUp(eventType == ChrEventType.DATA_SESSION ? (long) (1024 + rng.nextDouble() * 1024 * 1024) : null)
            .setBytesDown(eventType == ChrEventType.DATA_SESSION ? (long) (2048 + rng.nextDouble() * 10 * 1024 * 1024) : null)
            .build();
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

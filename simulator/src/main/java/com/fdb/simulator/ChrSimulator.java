package com.fdb.simulator;

import com.fdb.common.avro.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalTime;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ChrSimulator {

    private static final Logger log = LoggerFactory.getLogger(ChrSimulator.class);

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
        String bootstrap = System.getenv().getOrDefault("FDB_KAFKA_BOOTSTRAP", "localhost:9092");
        String topic = "chr-events";

        TopologyClient topology = new TopologyClient(bootstrap, "sim-chr");
        topology.start("topology");
        topology.awaitReady(java.time.Duration.ofSeconds(30));

        List<TopologyRecord> cells = topology.getAllCells();
        log.info("Loaded {} cells from topology", cells.size());

        Map<String, List<String>> cellUsers = assignUsers(cells);
        long baseEps = 5000;
        long totalCells = cells.size();
        double lambdaPerCell = (double) baseEps / totalCells;

        try (KafkaPublisher<ChrEvent> publisher = new KafkaPublisher<>(bootstrap, topic, ChrEvent.class)) {
            long startTime = System.currentTimeMillis();
            long counter = 0;

            while (!Thread.currentThread().isInterrupted()) {
                long now = System.currentTimeMillis();
                long elapsedMs = now - startTime;

                for (TopologyRecord cell : cells) {
                    double skewMultiplier = computeSkew(cell, elapsedMs);
                    double adjustedLambda = lambdaPerCell * skewMultiplier;

                    if (rng.nextDouble() < adjustedLambda * 0.001) {
                        List<String> users = cellUsers.get(cell.getCellId().toString());
                        if (users == null || users.isEmpty()) continue;

                        String imsi = users.get(rng.nextInt(users.size()));
                        ChrEvent event = generateEvent(cell, imsi, now);
                        publisher.publish(cell.getSiteId().toString(), event);
                        counter++;

                        if (counter % 10000 == 0) {
                            publisher.flush();
                            log.info("Published {} CHR events (EPS: {})", counter,
                                counter / ((System.currentTimeMillis() - startTime) / 1000.0 + 1));
                        }
                    }
                }

                long elapsed = System.currentTimeMillis() - now;
                if (elapsed < 100) Thread.sleep(100 - elapsed);
            }
        }
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

    private ChrEvent generateEvent(TopologyRecord cell, String imsi, long now) {
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

        int outOfOrderLag = rng.nextInt(100) < 5 ? rng.nextInt(5000) + 100 : 0;

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

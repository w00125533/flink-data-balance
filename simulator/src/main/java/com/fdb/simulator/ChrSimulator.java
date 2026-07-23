package com.fdb.simulator;

import com.fdb.common.avro.*;
import com.fdb.common.summary.SummarySwitch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalTime;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ChrSimulator {

    private static final Logger log = LoggerFactory.getLogger(ChrSimulator.class);
    private static final long DEFAULT_LOOP_INTERVAL_MS = 100L;
    private static final long METRICS_INTERVAL_MS = 1_000L;
    private static final long DEFAULT_MAX_OUT_OF_ORDER_LAG_MS = 2_000L;
    private static final int MAX_PRODUCER_THREADS = 256;
    private static final Coordinate[] ANOMALY_GRID_HOTSPOTS = {
        new Coordinate(39.9042d, 116.4074d),
        new Coordinate(39.9142d, 116.3974d),
        new Coordinate(39.8942d, 116.4174d),
        new Coordinate(39.8842d, 116.3974d)
    };

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
        topology.awaitReady(java.time.Duration.ofSeconds(30), config.topologyTargetCells());

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
        long simulationDurationSec = config.getLong("simulation.duration.sec", 0);
        long totalCells = cells.size();
        double lambdaPerCell = (double) baseEps / totalCells;
        SourceMetricsWriter sourceMetrics = new SourceMetricsWriter("FDB_CHR_METRICS_FILE");
        double anomalyInjectionRatio = anomalyInjectionRatio(config);
        int producerThreads = producerThreads(config);
        if (summaryEnabled) {
            log.info(SummarySwitch.format("sim-chr", "configured_base_eps", baseEps));
            log.info(SummarySwitch.format("sim-chr", "lambda_per_cell", String.format("%.3f", lambdaPerCell)));
            log.info(SummarySwitch.format("sim-chr", "anomaly_injection_ratio",
                String.format("%.3f", anomalyInjectionRatio)));
            log.info(SummarySwitch.format("sim-chr", "producer_threads", producerThreads));
            log.info(SummarySwitch.format("sim-chr", "simulation_duration_sec", simulationDurationSec));
        }

        long startTime = System.currentTimeMillis();
        long[] threadTargets = splitTargetEps(baseEps, producerThreads);
        List<ChrProducerWorker> workers = new ArrayList<>(producerThreads);
        List<Thread> threads = new ArrayList<>(producerThreads);
        for (int i = 0; i < producerThreads; i++) {
            ChrProducerWorker worker = new ChrProducerWorker(
                i,
                threadTargets[i],
                startTime,
                bootstrap,
                topic,
                cells,
                cellUsers,
                config.getDouble("rate.outOfOrderProb", 0.05),
                validateMaxOutOfOrderLagMs(config.getLong(
                    "rate.maxOutOfOrderLagMs", DEFAULT_MAX_OUT_OF_ORDER_LAG_MS)),
                anomalyInjectionRatio);
            workers.add(worker);
            threads.add(new Thread(worker, "chr-producer-" + i));
        }
        try {
            threads.forEach(Thread::start);
            long lastLoggedDelivered = 0L;
            while (!Thread.currentThread().isInterrupted()
                && SimulationRuntime.shouldContinue(startTime, System.currentTimeMillis(), simulationDurationSec)) {
                Throwable failure = firstWorkerFailure(workers);
                if (failure != null) {
                    throw new IllegalStateException("CHR producer worker failed", failure);
                }
                long metricsNow = System.currentTimeMillis();
                long delivered = writeSourceMetrics(sourceMetrics, baseEps, startTime, metricsNow,
                    simulationDurationSec, workers);
                if (delivered - lastLoggedDelivered >= 100_000L) {
                    lastLoggedDelivered = delivered;
                    long durationMs = SimulationRuntime.metricDurationMs(startTime, metricsNow, simulationDurationSec);
                    double observedEps = delivered / Math.max(durationMs / 1000.0d, 0.001d);
                    log.info("Published {} CHR events (EPS: {})", delivered, observedEps);
                    if (summaryEnabled) {
                        log.info(SummarySwitch.format("sim-chr", "events_published", delivered));
                        log.info(SummarySwitch.format("sim-chr", "observed_eps", String.format("%.1f", observedEps)));
                    }
                }
                Thread.sleep(METRICS_INTERVAL_MS);
            }
        } finally {
            stopWorkers(workers, threads);
            writeSourceMetrics(sourceMetrics, baseEps, startTime, System.currentTimeMillis(),
                simulationDurationSec, workers);
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

    static double eventProbability(double eventsPerSecond, long tickMillis) {
        if (eventsPerSecond <= 0.0 || tickMillis <= 0L) {
            return 0.0;
        }
        return Math.min(1.0, eventsPerSecond * tickMillis / 1000.0);
    }

    static boolean inAnomalyCohort(String id, double ratio) {
        return AnomalyInjection.inAnomalyCohort(id, ratio);
    }

    static int producerThreads(SimulatorConfig config) {
        return validateProducerThreads(config.getLong("chr.producer.threads", 1L));
    }

    static int validateProducerThreads(long threads) {
        if (threads < 1 || threads > MAX_PRODUCER_THREADS) {
            throw new IllegalArgumentException(
                "FDB_CHR_PRODUCER_THREADS must be between 1 and " + MAX_PRODUCER_THREADS);
        }
        return (int) threads;
    }

    static long validateMaxOutOfOrderLagMs(long configuredLagMs) {
        if (configuredLagMs <= 0L) {
            return DEFAULT_MAX_OUT_OF_ORDER_LAG_MS;
        }
        return Math.min(configuredLagMs, DEFAULT_MAX_OUT_OF_ORDER_LAG_MS);
    }

    static long[] splitTargetEps(long targetEps, int producerThreads) {
        int threads = validateProducerThreads(producerThreads);
        long normalizedTarget = Math.max(0L, targetEps);
        long[] targets = new long[threads];
        long base = normalizedTarget / threads;
        long remainder = normalizedTarget % threads;
        for (int i = 0; i < threads; i++) {
            targets[i] = base + (i < remainder ? 1L : 0L);
        }
        return targets;
    }

    static double anomalyInjectionRatio(SimulatorConfig config) {
        return validateAnomalyInjectionRatio(config.getDouble("benchmark.anomaly.injection.ratio", 0.05d));
    }

    static double validateAnomalyInjectionRatio(double ratio) {
        if (!Double.isFinite(ratio) || ratio < 0.0d || ratio > 1.0d) {
            throw new IllegalArgumentException("FDB_BENCHMARK_ANOMALY_INJECTION_RATIO must be between 0 and 1");
        }
        return ratio;
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

    ChrEvent generateEvent(TopologyRecord cell, String imsi, long now,
                           double outOfOrderProbability, long maxOutOfOrderLagMs,
                           double anomalyInjectionRatio) {
        return generateEvent(cell, imsi, now, outOfOrderProbability, maxOutOfOrderLagMs,
            anomalyInjectionRatio, rng);
    }

    private ChrEvent generateEvent(TopologyRecord cell, String imsi, long now,
                           double outOfOrderProbability, long maxOutOfOrderLagMs,
                           double anomalyInjectionRatio, Random random) {
        double distKm = haversine(cell.getSiteLat(), cell.getSiteLon(),
            cell.getSiteLat() + random.nextGaussian() * 0.001,
            cell.getSiteLon() + random.nextGaussian() * 0.001);
        double maxDist = cell.getCoverageRadiusM() / 1000.0;
        double signalQuality = Math.max(0, 1.0 - (distKm / Math.max(maxDist, 0.1)));

        float rsrp = (float) (-80 - (1 - signalQuality) * 50 + random.nextGaussian() * 5);
        float sinr = (float) (signalQuality * 25 + random.nextGaussian() * 3);
        float rsrq = (float) (-5 - (1 - signalQuality) * 10 + random.nextGaussian() * 2);

        int cqi = Math.max(0, Math.min(15, (int) (signalQuality * 15 + random.nextGaussian())));
        int mcs = Math.max(0, Math.min(28, (int) (signalQuality * 28 + random.nextGaussian())));

        ChrEventType[] eventTypes = ChrEventType.values();
        ChrEventType eventType = eventTypes[random.nextInt(eventTypes.length)];
        boolean anomalous = inAnomalyCohort(imsi, anomalyInjectionRatio)
            || inAnomalyCohort(cell.getCellId().toString(), anomalyInjectionRatio);
        if (anomalous) {
            eventType = ChrEventType.RRC_SETUP_FAIL;
            rsrp = -125.0f;
            sinr = -8.0f;
            rsrq = -18.0f;
            cqi = 1;
            mcs = 1;
        }

        long effectiveMaxOutOfOrderLagMs = validateMaxOutOfOrderLagMs(maxOutOfOrderLagMs);
        int outOfOrderLag = random.nextDouble() < outOfOrderProbability
            ? random.nextInt((int) Math.max(1, effectiveMaxOutOfOrderLagMs)) + 1 : 0;
        int resultCode = anomalous || eventType == ChrEventType.RRC_SETUP_FAIL || eventType == ChrEventType.DETACH
            ? 1 : 0;
        boolean dataSession = eventType == ChrEventType.DATA_SESSION;
        Long durationMs = anomalous ? Long.valueOf(60_000L)
            : dataSession ? Long.valueOf((long) (1000 + random.nextDouble() * 30000)) : null;
        Long bytesUp = dataSession ? Long.valueOf((long) (1024 + random.nextDouble() * 1024 * 1024)) : null;
        Long bytesDown = dataSession ? Long.valueOf((long) (2048 + random.nextDouble() * 10 * 1024 * 1024)) : null;
        Float latencyMs = anomalous ? Float.valueOf(2_500.0f) : null;
        Coordinate eventLocation = anomalous
            ? anomalyGridHotspot(cell, imsi)
            : new Coordinate(cell.getSiteLat() + random.nextGaussian() * 0.002,
                cell.getSiteLon() + random.nextGaussian() * 0.002);

        return ChrEvent.newBuilder()
            .setChrId(UUID.randomUUID().toString())
            .setEventTs(now - outOfOrderLag)
            .setImsi(imsi)
            .setImei(IMEI_POOL.get(random.nextInt(IMEI_POOL.size())))
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
            .setResultCode(resultCode)
            .setLatitude(eventLocation.latitude())
            .setLongitude(eventLocation.longitude())
            .setRsrp(rsrp)
            .setRsrq(rsrq)
            .setSinr(sinr)
            .setCqi(cqi)
            .setMcs(mcs)
            .setDurationMs(durationMs)
            .setBytesUp(bytesUp)
            .setBytesDown(bytesDown)
            .setLatencyMs(latencyMs)
            .build();
    }

    private static long sumSubmittedRecords(List<ChrProducerWorker> workers) {
        return workers.stream().mapToLong(ChrProducerWorker::submittedRecords).sum();
    }

    private static long sumDeliveredRecords(List<ChrProducerWorker> workers) {
        return workers.stream().mapToLong(ChrProducerWorker::deliveredRecords).sum();
    }

    private static Throwable firstWorkerFailure(List<ChrProducerWorker> workers) {
        return workers.stream()
            .map(ChrProducerWorker::failure)
            .filter(Objects::nonNull)
            .findFirst()
            .orElse(null);
    }

    private static long writeSourceMetrics(SourceMetricsWriter sourceMetrics, long targetEps, long startTime,
                                           long metricsNow, long simulationDurationSec,
                                           List<ChrProducerWorker> workers) {
        long durationMs = SimulationRuntime.metricDurationMs(startTime, metricsNow, simulationDurationSec);
        long submitted = sumSubmittedRecords(workers);
        long delivered = sumDeliveredRecords(workers);
        double observedEps = delivered / Math.max(durationMs / 1000.0d, 0.001d);
        long expected = expectedEvents(targetEps, startTime, startTime + durationMs);
        long pending = backlogRecords(expected, submitted);
        sourceMetrics.write("chr", targetEps, delivered, observedEps, durationMs,
            pending, undeliveredRecords(submitted, delivered));
        return delivered;
    }

    private static void stopWorkers(List<ChrProducerWorker> workers, List<Thread> threads) {
        boolean interrupted = false;
        for (ChrProducerWorker worker : workers) {
            worker.stop();
        }
        for (Thread thread : threads) {
            thread.interrupt();
        }
        for (Thread thread : threads) {
            try {
                thread.join(5_000L);
            } catch (InterruptedException e) {
                interrupted = true;
                Thread.currentThread().interrupt();
            }
        }
        for (ChrProducerWorker worker : workers) {
            worker.close();
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private static Coordinate anomalyGridHotspot(TopologyRecord cell, String imsi) {
        String key = value(cell.getCellId()) + ":" + value(imsi);
        int index = Math.floorMod(key.hashCode(), ANOMALY_GRID_HOTSPOTS.length);
        return ANOMALY_GRID_HOTSPOTS[index];
    }

    private static String value(Object value) {
        return value == null ? "" : value.toString();
    }

    private static double haversine(double lat1, double lon1, double lat2, double lon2) {
        double dLat = Math.toRadians(lat2 - lat1);
        double dLon = Math.toRadians(lon2 - lon1);
        double a = Math.sin(dLat / 2) * Math.sin(dLat / 2) +
            Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) *
                Math.sin(dLon / 2) * Math.sin(dLon / 2);
        return 6371 * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
    }

    private record Coordinate(double latitude, double longitude) {
    }

    private final class ChrProducerWorker implements Runnable, AutoCloseable {
        private final int id;
        private final long targetEps;
        private final long startTime;
        private final String topic;
        private final List<TopologyRecord> cells;
        private final Map<String, List<String>> cellUsers;
        private final double outOfOrderProbability;
        private final long maxOutOfOrderLagMs;
        private final double anomalyInjectionRatio;
        private final KafkaPublisher<ChrEvent> publisher;
        private final Random random;
        private final AtomicBoolean stopping = new AtomicBoolean(false);
        private final AtomicLong submittedRecords = new AtomicLong();
        private final AtomicReference<Throwable> failure = new AtomicReference<>();

        ChrProducerWorker(
            int id,
            long targetEps,
            long startTime,
            String bootstrap,
            String topic,
            List<TopologyRecord> cells,
            Map<String, List<String>> cellUsers,
            double outOfOrderProbability,
            long maxOutOfOrderLagMs,
            double anomalyInjectionRatio) {
            this.id = id;
            this.targetEps = targetEps;
            this.startTime = startTime;
            this.topic = topic;
            this.cells = cells;
            this.cellUsers = cellUsers;
            this.outOfOrderProbability = outOfOrderProbability;
            this.maxOutOfOrderLagMs = maxOutOfOrderLagMs;
            this.anomalyInjectionRatio = anomalyInjectionRatio;
            this.publisher = new KafkaPublisher<>(bootstrap, topic, ChrEvent.class);
            this.random = new Random(42L + id);
        }

        @Override
        public void run() {
            try {
                while (!stopping.get() && !Thread.currentThread().isInterrupted()) {
                    long now = System.currentTimeMillis();
                    long due = dueEvents(targetEps, startTime, now, submittedRecords.get());
                    for (long i = 0; i < due && !stopping.get(); i++) {
                        TopologyRecord cell = cells.get(random.nextInt(cells.size()));
                        List<String> users = cellUsers.get(cell.getCellId().toString());
                        if (users == null || users.isEmpty()) {
                            continue;
                        }
                        String imsi = users.get(random.nextInt(users.size()));
                        ChrEvent event = generateEvent(cell, imsi, now, outOfOrderProbability,
                            maxOutOfOrderLagMs, anomalyInjectionRatio, random);
                        publisher.publish(cell.getSiteId().toString(), event);
                        submittedRecords.incrementAndGet();
                    }
                    long elapsed = System.currentTimeMillis() - now;
                    if (elapsed < DEFAULT_LOOP_INTERVAL_MS) {
                        Thread.sleep(DEFAULT_LOOP_INTERVAL_MS - elapsed);
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Throwable e) {
                failure.compareAndSet(null, e);
                log.warn("CHR producer worker {} failed while publishing to {}: {}", id, topic, e.getMessage());
            }
        }

        long submittedRecords() {
            return submittedRecords.get();
        }

        long deliveredRecords() {
            return publisher.deliveredRecords();
        }

        Throwable failure() {
            return failure.get();
        }

        void stop() {
            stopping.set(true);
        }

        @Override
        public void close() {
            publisher.close();
        }
    }
}

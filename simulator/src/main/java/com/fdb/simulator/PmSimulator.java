package com.fdb.simulator;

import com.fdb.common.avro.PmStat;
import com.fdb.common.avro.TopologyRecord;
import com.fdb.common.summary.SummarySwitch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Random;

public class PmSimulator {

    private static final Logger log = LoggerFactory.getLogger(PmSimulator.class);
    private static final long DEFAULT_MAX_OUT_OF_ORDER_LAG_MS = 2_000L;

    private final String configPath;
    private final Random rng = new Random(43);

    public PmSimulator(String configPath) {
        this.configPath = configPath;
    }

    public void run() throws Exception {
        SimulatorConfig config = SimulatorConfig.load("sim-pm.yaml", configPath);
        String bootstrap = config.bootstrap();
        String topic = config.topic("pm-stats");

        TopologyClient topology = new TopologyClient(bootstrap, "sim-pm");
        topology.start(config.topologyTopic());
        topology.awaitReady(Duration.ofSeconds(30), config.topologyTargetCells());

        List<TopologyRecord> cells = topology.getAllCells();
        log.info("Loaded {} cells from topology for PM simulator", cells.size());
        boolean summaryEnabled = SummarySwitch.enabled();
        if (summaryEnabled) {
            long sites = cells.stream().map(c -> c.getSiteId().toString()).distinct().count();
            log.info(SummarySwitch.format("sim-pm", "loaded_sites", sites));
            log.info(SummarySwitch.format("sim-pm", "loaded_cells", cells.size()));
        }

        SourceMetricsWriter sourceMetrics = new SourceMetricsWriter("FDB_PM_METRICS_FILE");
        double epsPerCell = pmEpsPerCell(config);
        long targetEps = targetEpsForCellRate(cells.size(), epsPerCell);
        long publishIntervalMs = publishIntervalMs(epsPerCell);
        double anomalyInjectionRatio = anomalyInjectionRatio(config);
        long maxOutOfOrderLagMs = maxOutOfOrderLagMs(config);
        long simulationDurationSec = config.getLong("simulation.duration.sec", 0);
        try (KafkaPublisher<PmStat> publisher = new KafkaPublisher<>(bootstrap, topic, PmStat.class)) {
            long metricsStartMs = System.currentTimeMillis();
            long nextPublishAtMs = metricsStartMs;
            if (summaryEnabled) {
                log.info(SummarySwitch.format("sim-pm", "simulation_duration_sec", simulationDurationSec));
            }
            try {
                while (!Thread.currentThread().isInterrupted()
                    && SimulationRuntime.shouldContinue(metricsStartMs, System.currentTimeMillis(),
                        simulationDurationSec)) {
                    long wallNow = System.currentTimeMillis();
                    long windowEnd = boundedWindowEnd(wallNow, maxOutOfOrderLagMs);
                    long windowStart = windowEnd - 10_000;
                    int published = 0;
                    long activeUsers = 0;
                    double prbUsageDl = 0.0;

                    for (TopologyRecord cell : cells) {
                        PmStat stat = generatePmStat(cell, windowStart, windowEnd, anomalyInjectionRatio);
                        publisher.publish(cell.getSiteId().toString(), stat);
                        published++;
                        if (summaryEnabled) {
                            activeUsers += stat.getActiveUsers();
                            prbUsageDl += stat.getPrbUsageDl();
                        }
                    }

                    publisher.flush();
                    writeSourceMetrics(sourceMetrics, targetEps, metricsStartMs, System.currentTimeMillis(),
                        simulationDurationSec, publisher);
                    if (summaryEnabled && published > 0) {
                        log.info(SummarySwitch.format("sim-pm", "records_published_last_window", published));
                        log.info(SummarySwitch.format("sim-pm", "avg_active_users_last_window", activeUsers / published));
                        log.info(SummarySwitch.format("sim-pm", "avg_prb_usage_dl_last_window",
                            String.format("%.3f", prbUsageDl / published)));
                        log.info(SummarySwitch.format("sim-pm", "window_ts", windowStart + ".." + windowEnd));
                    }

                    nextPublishAtMs += publishIntervalMs;
                    long sleepMs = nextPublishAtMs - System.currentTimeMillis();
                    if (sleepMs > 0) {
                        Thread.sleep(sleepMs);
                    } else {
                        nextPublishAtMs = System.currentTimeMillis();
                    }
                }
            } finally {
                publisher.flush();
                writeSourceMetrics(sourceMetrics, targetEps, metricsStartMs, System.currentTimeMillis(),
                    simulationDurationSec, publisher);
            }
        }
    }

    private static void writeSourceMetrics(SourceMetricsWriter sourceMetrics, long targetEps, long startTime,
                                           long metricsNow, long simulationDurationSec,
                                           KafkaPublisher<PmStat> publisher) {
        long durationMs = SimulationRuntime.metricDurationMs(startTime, metricsNow, simulationDurationSec);
        long delivered = publisher.deliveredRecords();
        double observedEps = delivered / Math.max(durationMs / 1000.0d, 0.001d);
        sourceMetrics.write("pm", targetEps, delivered, observedEps, durationMs, 0L,
            publisher.undeliveredRecords());
    }

    PmStat generatePmStat(TopologyRecord cell, long windowStart, long windowEnd,
                          double anomalyInjectionRatio) {
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
        boolean anomalous = AnomalyInjection.inAnomalyCohort(cell.getCellId().toString(), anomalyInjectionRatio);
        AnomalyValues anomalyValues = anomalous ? anomalousValues() : null;
        if (anomalous) {
            activeUsers = 180;
            totalConnections = 200;
            dropped = Math.round(anomalyValues.dropRate() * totalConnections);
            rrcAttempt = 100;
            rrcSuccess = 40;
            latency = anomalyValues.avgLatencyMs();
        }

        return PmStat.newBuilder()
            .setSiteId(cell.getSiteId().toString())
            .setCellId(cell.getCellId().toString())
            .setWindowStartTs(windowStart)
            .setWindowEndTs(windowEnd)
            .setPrbUsageDl((float) Math.min(1.0, load + rng.nextFloat() * 0.1))
            .setPrbUsageUl((float) Math.min(1.0, load * 0.6 + rng.nextFloat() * 0.1))
            .setActiveUsers(activeUsers)
            .setAvgRsrp(anomalous ? anomalyValues.avgRsrp() : (float) (-80 - (1 - (float) load) * 40 + rng.nextFloat() * 5))
            .setAvgRsrq(anomalous ? -18.0f : (float) (-5 - (1 - (float) load) * 12 + rng.nextFloat() * 3))
            .setAvgSinr(anomalous ? anomalyValues.avgSinr() : (float) (load * 20 + rng.nextFloat() * 5))
            .setAvgCqi(anomalous ? 1.0f : (float) (load * 12 + rng.nextFloat() * 3))
            .setAvgMcs(anomalous ? 1.0f : (float) (load * 22 + rng.nextFloat() * 5))
            .setAvgBler(anomalous ? 0.35f : rng.nextFloat() * 0.1f)
            .setThroughputDlMbps(anomalous ? 5.0f : (float) (load * 200 + rng.nextFloat() * 50))
            .setThroughputUlMbps(anomalous ? 1.0f : (float) (load * 50 + rng.nextFloat() * 20))
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

    static long boundedWindowEnd(long wallNow, long maxOutOfOrderLagMs) {
        long effectiveMaxLag = validateMaxOutOfOrderLagMs(maxOutOfOrderLagMs);
        long aligned = alignTo10s(wallNow);
        long lowerBound = wallNow - effectiveMaxLag;
        return Math.min(wallNow, Math.max(aligned, lowerBound));
    }

    static long targetEpsForWindow(int cellCount) {
        return cellCount <= 0 ? 0L : Math.max(1L, Math.round(cellCount / 10.0d));
    }

    static long targetEpsForCellRate(int cellCount, double epsPerCell) {
        if (cellCount <= 0) {
            return 0L;
        }
        return Math.max(1L, Math.round(cellCount * validatePmEpsPerCell(epsPerCell)));
    }

    static long publishIntervalMs(double epsPerCell) {
        return Math.max(1L, Math.round(1000.0d / validatePmEpsPerCell(epsPerCell)));
    }

    static double pmEpsPerCell(SimulatorConfig config) {
        return validatePmEpsPerCell(config.getDouble("pm.eps.per.cell", 0.1d));
    }

    static long maxOutOfOrderLagMs(SimulatorConfig config) {
        return validateMaxOutOfOrderLagMs(config.getLong(
            "pm.max.out.of.order.lag.ms", DEFAULT_MAX_OUT_OF_ORDER_LAG_MS));
    }

    static long validateMaxOutOfOrderLagMs(long configuredLagMs) {
        if (configuredLagMs <= 0L) {
            return DEFAULT_MAX_OUT_OF_ORDER_LAG_MS;
        }
        return Math.min(configuredLagMs, DEFAULT_MAX_OUT_OF_ORDER_LAG_MS);
    }

    static double validatePmEpsPerCell(double epsPerCell) {
        if (!Double.isFinite(epsPerCell) || epsPerCell <= 0.0d) {
            throw new IllegalArgumentException("FDB_PM_EPS_PER_CELL must be a finite positive number");
        }
        return epsPerCell;
    }

    static AnomalyValues anomalousValues() {
        return new AnomalyValues(-125.0f, -8.0f, 0.125f, 250.0f);
    }

    static double anomalyInjectionRatio(SimulatorConfig config) {
        return ChrSimulator.validateAnomalyInjectionRatio(
            config.getDouble("benchmark.anomaly.injection.ratio", 0.05d));
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

record AnomalyValues(float avgRsrp, float avgSinr, float dropRate, float avgLatencyMs) {
}

package com.fdb.job.coordinator;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class LoadCoordinator extends KeyedProcessFunction<String, HeartbeatPayload, RoutingEntry> {

    private static final Logger log = LoggerFactory.getLogger(LoadCoordinator.class);

    private static final long EVALUATION_INTERVAL_MS = 10_000;
    private static final long REBALANCE_BOUNDARY_MS = 300_000;
    private static final double OVERLOAD_THRESHOLD = 1.5;
    private static final long OVERLOAD_DURATION_MS = 60_000;
    private static final int NUM_HOTSPOTS = 3;
    private static final int NUM_VBUCKETS = 1024;

    private transient MapState<Integer, HeartbeatPayload> heartbeatState;
    private transient MapState<String, RoutingEntry> routingState;
    private transient MapState<String, Long> overloadStartState;
    private transient ValueState<Long> routingVersionState;
    private transient ValueState<Long> lastBoundaryState;

    private transient RebalancePolicy policy;
    private transient String snapshotDir;

    private transient long lastTimerTime = 0;

    @Override
    public void open(Configuration parameters) {
        heartbeatState = getRuntimeContext().getMapState(
            new MapStateDescriptor<>("heartbeats", Integer.class, HeartbeatPayload.class));
        routingState = getRuntimeContext().getMapState(
            new MapStateDescriptor<>("routing", String.class, RoutingEntry.class));
        overloadStartState = getRuntimeContext().getMapState(
            new MapStateDescriptor<>("overload-start", String.class, Long.class));
        routingVersionState = getRuntimeContext().getState(
            new ValueStateDescriptor<>("routing-version", Long.class));
        lastBoundaryState = getRuntimeContext().getState(
            new ValueStateDescriptor<>("last-boundary", Long.class));

        policy = new RebalancePolicy(OVERLOAD_THRESHOLD, OVERLOAD_DURATION_MS,
            NUM_HOTSPOTS, NUM_VBUCKETS);
        snapshotDir = System.getProperty("fdb.snapshot.dir", "/tmp/fdb-state");
    }

    @Override
    public void processElement(HeartbeatPayload hb, Context ctx, Collector<RoutingEntry> out) throws Exception {
        heartbeatState.put(hb.getSubtaskId(), hb);
        updateOverloadTracking(hb, ctx.timerService().currentProcessingTime());

        if (lastTimerTime == 0) {
            ctx.timerService().registerProcessingTimeTimer(
                ctx.timerService().currentProcessingTime() + EVALUATION_INTERVAL_MS);
            lastTimerTime = ctx.timerService().currentProcessingTime();
        }
    }

    private void updateOverloadTracking(HeartbeatPayload hb, long now) throws Exception {
        double median = computeMedianEps();
        double overloadLine = median * OVERLOAD_THRESHOLD;

        if (hb.getEps() > overloadLine && median > 0) {
            Long start = overloadStartState.get(String.valueOf(hb.getSubtaskId()));
            if (start == null) {
                overloadStartState.put(String.valueOf(hb.getSubtaskId()), now);
                log.info("Subtask {} crossed overload line (eps={}, threshold={})",
                    hb.getSubtaskId(), String.format("%.1f", hb.getEps()), String.format("%.1f", overloadLine));
            }
        } else {
            overloadStartState.remove(String.valueOf(hb.getSubtaskId()));
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<RoutingEntry> out) throws Exception {
        long now = ctx.timerService().currentProcessingTime();
        lastTimerTime = now;

        logHeartbeatSummary();

        long lastBoundary = lastBoundaryState.value() != null ? lastBoundaryState.value() : 0;
        long currentBoundary = (now / REBALANCE_BOUNDARY_MS) * REBALANCE_BOUNDARY_MS;

        Map<Integer, HeartbeatPayload> heartbeats = new HashMap<>();
        heartbeatState.entries().forEach(e -> heartbeats.put(e.getKey(), e.getValue()));

        if (currentBoundary > lastBoundary && !heartbeats.isEmpty()) {
            lastBoundaryState.update(currentBoundary);

            long routingVersion = routingVersionState.value() != null
                ? routingVersionState.value() + 1 : 1;
            routingVersionState.update(routingVersion);

            Map<String, Long> overloadTimes = new HashMap<>();
            overloadStartState.entries().forEach(e -> overloadTimes.put(e.getKey(), e.getValue()));

            List<RebalancePolicy.RebalanceDecision> decisions =
                policy.evaluate(heartbeats, overloadTimes, now, routingVersion);

            if (!decisions.isEmpty()) {
                log.info("Rebalance at boundary {}: {} decisions", currentBoundary, decisions.size());
                for (var decision : decisions) {
                    RoutingEntry entry = new RoutingEntry(
                        decision.site().siteId(),
                        decision.site().vbucketId(),
                        decision.newSlotShift(),
                        decision.targetSubtask(),
                        routingVersion,
                        now
                    );
                    routingState.put(decision.site().siteId(), entry);
                    out.collect(entry);
                    log.info("  Move {} subtask {}->{} shift={}",
                        entry.getSiteId(), decision.currentSubtask(),
                        decision.targetSubtask(), decision.newSlotShift());
                }
            }

            writeRoutingSnapshot(routingVersion, now);
        }

        ctx.timerService().registerProcessingTimeTimer(now + EVALUATION_INTERVAL_MS);
    }

    private double computeMedianEps() throws Exception {
        List<Double> epsList = new ArrayList<>();
        heartbeatState.values().forEach(hb -> epsList.add(hb.getEps()));
        if (epsList.isEmpty()) return 0;
        Collections.sort(epsList);
        int n = epsList.size();
        if (n % 2 == 0) return (epsList.get(n / 2 - 1) + epsList.get(n / 2)) / 2.0;
        return epsList.get(n / 2);
    }

    private void logHeartbeatSummary() throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("[Coordinator] Heartbeat summary: ");
        List<Map.Entry<Integer, HeartbeatPayload>> entries = new ArrayList<>();
        heartbeatState.entries().forEach(e -> entries.add(e));
        if (entries.isEmpty()) {
            sb.append("no heartbeats yet");
        } else {
            entries.sort(Map.Entry.comparingByKey());
            for (var e : entries) {
                sb.append(String.format("subtask=%d eps=%.1f ", e.getKey(), e.getValue().getEps()));
            }
            double median = computeMedianEps();
            double max = entries.stream().mapToDouble(v -> v.getValue().getEps()).max().orElse(0);
            double imbalance = median > 0 ? max / median : 1.0;
            sb.append(String.format("| median=%.1f imbalance=%.2f", median, imbalance));
        }
        log.info(sb.toString());
    }

    private void writeRoutingSnapshot(long routingVersion, long now) {
        try {
            Path dir = Paths.get(snapshotDir, "coordinator");
            Files.createDirectories(dir);
            Path file = dir.resolve("routing-snapshot.csv");

            try (PrintWriter pw = new PrintWriter(Files.newBufferedWriter(file,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING))) {
                pw.println("siteId,vbucketId,slotShift,assignedSubtask,routingVersion,decisionTs");
                List<Map.Entry<String, RoutingEntry>> entries = new ArrayList<>();
                routingState.entries().forEach(e -> entries.add(e));
                for (var entry : entries) {
                    pw.println(entry.getValue().toCsv());
                }
                pw.flush();
            }
            log.info("Wrote routing snapshot to {} (version {}, {} entries)",
                file, routingVersion, countRoutingEntries());
        } catch (Exception e) {
            log.error("Failed to write routing snapshot", e);
        }
    }

    private long countRoutingEntries() {
        try {
            long[] count = {0};
            routingState.entries().forEach(e -> count[0]++);
            return count[0];
        } catch (Exception e) {
            return -1;
        }
    }
}

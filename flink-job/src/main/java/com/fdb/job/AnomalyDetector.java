package com.fdb.job;

import com.fdb.common.avro.AnomalyEvent;
import com.fdb.common.avro.AnomalyType;
import com.fdb.common.avro.CmConfig;
import com.fdb.common.avro.Severity;
import com.fdb.common.geo.Geohash;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class AnomalyDetector
    extends KeyedProcessFunction<String, EnrichedChr, AnomalyEvent> {

    private static final Logger log = LoggerFactory.getLogger(AnomalyDetector.class);

    private static final float RSRP_THRESHOLD = -110f;
    private static final float SINR_THRESHOLD = -3f;
    private static final int ATTACH_FAIL_BURST_THRESHOLD = 10;
    private static final int ATTACH_FAIL_WINDOW_BUCKET_MS = 60_000;
    private static final double HO_FAILURE_RATE_THRESHOLD = 0.30;
    private static final int HO_MIN_ATTEMPTS = 20;
    private static final int HO_SLIDING_WINDOW_BUCKETS = 5;
    private static final int COVERAGE_HOLE_THRESHOLD = 50;
    private static final long COVERAGE_HOLE_WINDOW_MS = 300_000;

    // ── Rule 2: Attach failure burst ──
    private transient MapState<Long, Integer> attachFailWindow;

    // ── Rule 3: Handover failure pattern ──
    private transient MapState<Long, int[]> hoBucketState;

    // ── Rule 4: Config mismatch ──
    private transient ValueState<Boolean> configMismatchFlagged;

    // ── Rule 5: Coverage hole ──
    private transient MapState<Long, Integer> lowSignalBucket;
    private transient ValueState<Long> coverageHoleBucket;

    @Override
    public void open(Configuration parameters) {
        attachFailWindow = getRuntimeContext().getMapState(
            new MapStateDescriptor<>("attach-fail-window", Long.class, Integer.class));

        hoBucketState = getRuntimeContext().getMapState(
            new MapStateDescriptor<>("ho-bucket", Long.class, int[].class));

        configMismatchFlagged = getRuntimeContext().getState(
            new ValueStateDescriptor<>("config-mismatch-flagged", Boolean.class));

        lowSignalBucket = getRuntimeContext().getMapState(
            new MapStateDescriptor<>("low-signal-bucket", Long.class, Integer.class));

        coverageHoleBucket = getRuntimeContext().getState(
            new ValueStateDescriptor<>("coverage-hole-bucket", Long.class));
    }

    @Override
    public void processElement(EnrichedChr enriched, Context ctx, Collector<AnomalyEvent> out) throws Exception {
        var chr = enriched.chrEvent();
        String gridId = Geohash.encode(chr.getLatitude(), chr.getLongitude(), 6);

        // Rule 1: LOW_SIGNAL
        detectLowSignal(enriched, gridId, out);

        // Rule 2: ATTACH_FAILURE_BURST
        detectAttachFailureBurst(enriched, gridId, out);

        // Rule 3: HANDOVER_FAIL_PATTERN
        detectHandoverFailPattern(enriched, gridId, out);

        // Rule 4: CONFIG_MISMATCH
        detectConfigMismatch(enriched, gridId, out);

        // Rule 5: COVERAGE_HOLE
        detectCoverageHole(enriched, gridId, out);
    }

    // ──────────────────────────────────────────────
    // Rule 1: LOW_SIGNAL
    // ──────────────────────────────────────────────

    private void detectLowSignal(EnrichedChr enriched, String gridId, Collector<AnomalyEvent> out) throws Exception {
        var chr = enriched.chrEvent();
        if (chr.getRsrp() == null || chr.getSinr() == null) return;

        if (chr.getRsrp() < RSRP_THRESHOLD || chr.getSinr() < SINR_THRESHOLD) {
            out.collect(buildAnomaly(chr, gridId, AnomalyType.LOW_SIGNAL, Severity.LOW,
                String.format("{\"rsrp\":%s,\"sinr\":%s}", chr.getRsrp(), chr.getSinr())));
        }
    }

    // ──────────────────────────────────────────────
    // Rule 2: ATTACH_FAILURE_BURST
    // ──────────────────────────────────────────────

    private void detectAttachFailureBurst(EnrichedChr enriched, String gridId, Collector<AnomalyEvent> out) throws Exception {
        var chr = enriched.chrEvent();
        if (chr.getEventType() == null) return;
        if (!"ATTACH".equals(chr.getEventType().toString()) || chr.getResultCode() == 0) return;

        long bucket = chr.getEventTs() / ATTACH_FAIL_WINDOW_BUCKET_MS;
        Integer count = attachFailWindow.get(bucket);
        int newCount = (count == null ? 0 : count) + 1;
        attachFailWindow.put(bucket, newCount);

        if (newCount >= ATTACH_FAIL_BURST_THRESHOLD) {
            out.collect(buildAnomaly(chr, gridId, AnomalyType.ATTACH_FAILURE_BURST, Severity.HIGH,
                String.format("{\"failures_in_minute\":%d}", newCount)));
        }
    }

    // ──────────────────────────────────────────────
    // Rule 3: HANDOVER_FAIL_PATTERN
    // ──────────────────────────────────────────────

    private void detectHandoverFailPattern(EnrichedChr enriched, String gridId, Collector<AnomalyEvent> out) throws Exception {
        var chr = enriched.chrEvent();
        if (chr.getEventType() == null) return;
        if (!"HANDOVER".equals(chr.getEventType().toString())) return;

        long bucket = chr.getEventTs() / ATTACH_FAIL_WINDOW_BUCKET_MS;
        int[] bucketData = hoBucketState.get(bucket);
        if (bucketData == null) bucketData = new int[]{0, 0};
        // [attempts, failures]
        bucketData[0]++;
        if (chr.getResultCode() != 0) bucketData[1]++;
        hoBucketState.put(bucket, bucketData);

        // Clean old buckets and compute rate over sliding window
        long cutoff = bucket - HO_SLIDING_WINDOW_BUCKETS + 1;
        List<Long> staleBuckets = new ArrayList<>();
        int totalAttempts = 0;
        int totalFailures = 0;

        for (Map.Entry<Long, int[]> entry : hoBucketState.entries()) {
            long b = entry.getKey();
            if (b < cutoff) {
                staleBuckets.add(b);
            } else {
                int[] d = entry.getValue();
                totalAttempts += d[0];
                totalFailures += d[1];
            }
        }
        for (Long stale : staleBuckets) hoBucketState.remove(stale);

        if (totalAttempts >= HO_MIN_ATTEMPTS) {
            double rate = (double) totalFailures / totalAttempts;
            if (rate > HO_FAILURE_RATE_THRESHOLD) {
                out.collect(buildAnomaly(chr, gridId, AnomalyType.HANDOVER_FAIL_PATTERN, Severity.MEDIUM,
                    String.format("{\"failure_rate\":%.2f,\"attempts\":%d,\"failures\":%d}", rate, totalAttempts, totalFailures)));
            }
        }
    }

    // ──────────────────────────────────────────────
    // Rule 4: CONFIG_MISMATCH
    // ──────────────────────────────────────────────

    private void detectConfigMismatch(EnrichedChr enriched, String gridId, Collector<AnomalyEvent> out) throws Exception {
        var chr = enriched.chrEvent();
        CmConfig cm = enriched.cmConfig();
        if (cm == null) return;

        Boolean alreadyFlagged = configMismatchFlagged.value();
        if (Boolean.TRUE.equals(alreadyFlagged)) return;

        List<String> mismatches = new ArrayList<>();
        if (chr.getTac() != cm.getTac()) mismatches.add("tac");
        if (chr.getPci() != cm.getPci()) mismatches.add("pci");
        if (chr.getEci() != cm.getEci()) mismatches.add("eci");
        if (chr.getArfcn() != null && (int) chr.getArfcn() != cm.getArfcn()) mismatches.add("arfcn");

        if (!mismatches.isEmpty()) {
            configMismatchFlagged.update(true);
            out.collect(buildAnomaly(chr, gridId, AnomalyType.CONFIG_MISMATCH, Severity.MEDIUM,
                String.format("{\"mismatch_fields\":\"%s\"}", String.join(",", mismatches))));
            log.info("CONFIG_MISMATCH cell={} mismatches={}", chr.getCellId(), mismatches);
        }
    }

    // ──────────────────────────────────────────────
    // Rule 5: COVERAGE_HOLE
    // ──────────────────────────────────────────────

    private void detectCoverageHole(EnrichedChr enriched, String gridId, Collector<AnomalyEvent> out) throws Exception {
        var chr = enriched.chrEvent();
        if (chr.getRsrp() == null) return;
        if (!(chr.getRsrp() < RSRP_THRESHOLD)) return;

        long bucket = chr.getEventTs() / COVERAGE_HOLE_WINDOW_MS;
        Long alreadyEmitted = coverageHoleBucket.value();
        if (alreadyEmitted != null && alreadyEmitted == bucket) return;

        Integer count = lowSignalBucket.get(bucket);
        int newCount = (count == null ? 0 : count) + 1;
        lowSignalBucket.put(bucket, newCount);

        if (newCount >= COVERAGE_HOLE_THRESHOLD) {
            coverageHoleBucket.update(bucket);
            out.collect(buildAnomaly(chr, gridId, AnomalyType.COVERAGE_HOLE, Severity.HIGH,
                String.format("{\"low_signal_count\":%d,\"window_ms\":%d}",
                    newCount, COVERAGE_HOLE_WINDOW_MS)));
            log.info("COVERAGE_HOLE cell={} grid={} count={}", chr.getCellId(), gridId, newCount);
        }
    }

    // ──────────────────────────────────────────────
    // Helper
    // ──────────────────────────────────────────────

    private AnomalyEvent buildAnomaly(
            com.fdb.common.avro.ChrEvent chr, String gridId,
            AnomalyType type, Severity severity, String contextJson) {
        return AnomalyEvent.newBuilder()
            .setDetectionTs(System.currentTimeMillis())
            .setEventTs(chr.getEventTs())
            .setImsi(chr.getImsi().toString())
            .setSiteId(chr.getSiteId().toString())
            .setCellId(chr.getCellId().toString())
            .setLatitude(chr.getLatitude())
            .setLongitude(chr.getLongitude())
            .setGridId(gridId)
            .setAnomalyType(type)
            .setSeverity(severity)
            .setRuleVersion("v1.0")
            .setContextJson(contextJson)
            .build();
    }
}

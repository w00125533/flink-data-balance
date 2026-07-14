package com.fdb.job;

import com.fdb.common.avro.AnomalyEvent;
import com.fdb.common.avro.AnomalyType;
import com.fdb.common.avro.CfgConfig;
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

    private final RuleConfig rules;
    private static final int ATTACH_FAIL_WINDOW_BUCKET_MS = 60_000;
    private static final double HO_FAILURE_RATE_THRESHOLD = 0.30;
    private static final int HO_MIN_ATTEMPTS = 20;
    private static final int HO_SLIDING_WINDOW_BUCKETS = 5;

    // Rule 2: Attach failure burst
    private transient MapState<Long, Integer> attachFailWindow;

    // Rule 3: Handover failure pattern
    private transient MapState<Long, int[]> hoBucketState;

    // Rule 4: Config mismatch
    private transient ValueState<Boolean> configMismatchFlagged;

    // Rule 5: Coverage hole

    public AnomalyDetector() { this(RuleConfig.defaults()); }

    public AnomalyDetector(RuleConfig rules) { this.rules = rules; }

    @Override
    public void open(Configuration parameters) {
        attachFailWindow = getRuntimeContext().getMapState(
            new MapStateDescriptor<>("attach-fail-window", Long.class, Integer.class));

        hoBucketState = getRuntimeContext().getMapState(
            new MapStateDescriptor<>("ho-bucket", Long.class, int[].class));

        configMismatchFlagged = getRuntimeContext().getState(
            new ValueStateDescriptor<>("config-mismatch-flagged", Boolean.class));

    }

    @Override
    public void processElement(EnrichedChr enriched, Context ctx, Collector<AnomalyEvent> out) throws Exception {
        var chr = enriched.chrEvent();
        String gridId = Geohash.encode(chr.getLatitude(), chr.getLongitude(), 6);

        // Rule 1: LOW_SIGNAL
        detectLowSignal(enriched, gridId, out);

    // Rule 2: Attach failure burst
        detectAttachFailureBurst(enriched, gridId, out);

    // Rule 3: Handover failure pattern
        detectHandoverFailPattern(enriched, gridId, out);

    // Rule 4: Config mismatch
        detectConfigMismatch(enriched, gridId, out);

    }

    // ──────────────────────────────────────────────
    // Rule 1: LOW_SIGNAL
    // ──────────────────────────────────────────────

    private void detectLowSignal(EnrichedChr enriched, String gridId, Collector<AnomalyEvent> out) throws Exception {
        var chr = enriched.chrEvent();
        if (chr.getRsrp() == null || chr.getSinr() == null) return;

        if (chr.getRsrp() < rules.rsrpThreshold() || chr.getSinr() < rules.sinrThreshold()) {
            out.collect(buildAnomaly(chr, gridId, AnomalyType.LOW_SIGNAL, Severity.LOW,
                String.format("{\"rsrp\":%s,\"sinr\":%s}", chr.getRsrp(), chr.getSinr())));
        }
    }

    // ──────────────────────────────────────────────
    // Rule 2: Attach failure burst
    // ──────────────────────────────────────────────

    private void detectAttachFailureBurst(EnrichedChr enriched, String gridId, Collector<AnomalyEvent> out) throws Exception {
        var chr = enriched.chrEvent();
        if (chr.getEventType() == null) return;
        if (!"ATTACH".equals(chr.getEventType().toString()) || chr.getResultCode() == 0) return;

        long bucket = chr.getEventTs() / ATTACH_FAIL_WINDOW_BUCKET_MS;
        Integer count = attachFailWindow.get(bucket);
        int newCount = (count == null ? 0 : count) + 1;
        attachFailWindow.put(bucket, newCount);
        List<Long> staleBuckets = new ArrayList<>();
        for (Long existing : attachFailWindow.keys()) if (existing < bucket - 1) staleBuckets.add(existing);
        for (Long stale : staleBuckets) attachFailWindow.remove(stale);

        if (newCount >= rules.attachFailBurstThreshold()) {
            out.collect(buildAnomaly(chr, gridId, AnomalyType.ATTACH_FAILURE_BURST, Severity.HIGH,
                String.format("{\"failures_in_minute\":%d}", newCount)));
        }
    }

    // ──────────────────────────────────────────────
    // Rule 3: Handover failure pattern
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
    // Rule 4: Config mismatch
    // ──────────────────────────────────────────────

    private void detectConfigMismatch(EnrichedChr enriched, String gridId, Collector<AnomalyEvent> out) throws Exception {
        var chr = enriched.chrEvent();
        CfgConfig cfg = enriched.cfgConfig();
        if (cfg == null) return;

        Boolean alreadyFlagged = configMismatchFlagged.value();
        if (Boolean.TRUE.equals(alreadyFlagged)) return;

        List<String> mismatches = new ArrayList<>();
        if (chr.getTac() != cfg.getTac()) mismatches.add("tac");
        if (chr.getPci() != cfg.getPci()) mismatches.add("pci");
        if (chr.getEci() != cfg.getEci()) mismatches.add("eci");
        if (chr.getArfcn() != null && (int) chr.getArfcn() != cfg.getArfcn()) mismatches.add("arfcn");

        if (!mismatches.isEmpty()) {
            configMismatchFlagged.update(true);
            out.collect(buildAnomaly(chr, gridId, AnomalyType.CONFIG_MISMATCH, Severity.HIGH,
                String.format("{\"mismatch_fields\":\"%s\"}", String.join(",", mismatches))));
            log.info("CONFIG_MISMATCH cell={} mismatches={}", chr.getCellId(), mismatches);
        }
    }

    // ──────────────────────────────────────────────
    // Helper
    // ──────────────────────────────────────────────

    static AnomalyEvent buildAnomaly(
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

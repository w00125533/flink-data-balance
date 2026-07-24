package com.fdb.job.metrics;

import com.fdb.common.avro.AnomalyEvent;
import com.fdb.common.avro.CellKpi;
import com.fdb.common.metrics.StageMetricSample;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Locale;

public final class SinkLatencyProbe<T> extends ProcessFunction<T, T> {

    private static final Logger log = LoggerFactory.getLogger(SinkLatencyProbe.class);

    private final String stageId;
    private final String displayName;
    private final String sinkType;
    private final String dataset;
    private final String windowKind;
    private final long emitEveryRecords;
    private final MetricRuntimeConfig metricConfig;
    private long records;
    private long approxBytes;
    private long startedAtNanos = -1L;
    private final LatencyStats latencyStats = new LatencyStats();
    private transient MetricSamplePublisher metricPublisher;
    private int subtaskIndex = -1;

    public SinkLatencyProbe(String stageId, String displayName, String sinkType, String dataset,
                            String windowKind, long emitEveryRecords) {
        this(stageId, displayName, sinkType, dataset, windowKind, emitEveryRecords,
            MetricRuntimeConfig.fromEnvironment());
    }

    public SinkLatencyProbe(String stageId, String displayName, String sinkType, String dataset,
                            String windowKind, long emitEveryRecords, MetricRuntimeConfig metricConfig) {
        this.stageId = stageId;
        this.displayName = displayName;
        this.sinkType = sinkType;
        this.dataset = dataset;
        this.windowKind = windowKind;
        this.emitEveryRecords = emitEveryRecords > 0 ? emitEveryRecords : 100L;
        this.metricConfig = metricConfig;
    }

    @Override
    public void open(Configuration parameters) {
        metricPublisher = new MetricSamplePublisher(metricConfig.metricsEnabled());
        subtaskIndex = runtimeSubtaskIndex();
    }

    @Override
    public void processElement(T value, Context ctx, Collector<T> out) {
        long nowMs = ctx.timerService().currentProcessingTime();
        record(value, nowMs);
        publishMetric(nowMs);
        out.collect(value);
    }

    @Override
    public void close() {
        if (metricPublisher != null) {
            metricPublisher.close();
        }
    }

    T record(T value) {
        return record(value, System.currentTimeMillis());
    }

    T record(T value, long nowMs) {
        if (startedAtNanos < 0L) {
            startedAtNanos = System.nanoTime();
        }
        records++;
        approxBytes += estimateBytes(value);
        latencyStats.recordObservedAt(nowMs, latencyBaseTimestamp(value));
        if (shouldEmit()) {
            log.info(summaryLine());
        }
        return value;
    }

    long records() {
        return records;
    }

    long approxBytes() {
        return approxBytes;
    }

    StageMetricSample metricSample(long nowMs) {
        LatencyStats.Snapshot latency = latencyStats.snapshotAndReset();
        return StageMetricSample.sinkLatency(stageId, displayName, "healthy", sinkType, dataset, windowKind,
            records, approxBytes, durationMs(), latency.p50Ms(), latency.p95Ms(), latency.p99Ms(), 0L, "", -1L, nowMs)
            .withRunMetadata(metricConfig.runId(), metricConfig.resultSink(), metricConfig.parallelism(), subtaskIndex);
    }

    String summaryLine() {
        return String.format(Locale.ROOT,
            "[summary-code] stage=%s sink_type=%s dataset=%s window_kind=%s records=%d approx_bytes=%d records_per_sec=%.2f",
            stageId, sinkType, dataset, windowKind, records, approxBytes, recordsPerSecond());
    }

    private void publishMetric(long nowMs) {
        if (metricPublisher == null || records == 0L || !shouldEmit()) {
            return;
        }
        try {
            metricPublisher.publish(metricSample(nowMs));
        } catch (RuntimeException e) {
            log.warn("Failed to publish sink latency metric for stage={}", stageId, e);
        }
    }

    private boolean shouldEmit() {
        return records == 1L || records % emitEveryRecords == 0L;
    }

    private int runtimeSubtaskIndex() {
        try {
            return getRuntimeContext().getIndexOfThisSubtask();
        } catch (IllegalStateException ignored) {
            return -1;
        }
    }

    private long durationMs() {
        if (startedAtNanos < 0L) {
            return 0L;
        }
        return Math.max(0L, (System.nanoTime() - startedAtNanos) / 1_000_000L);
    }

    private double recordsPerSecond() {
        if (records == 0L || startedAtNanos < 0L) {
            return 0.0d;
        }
        double elapsedSeconds = Math.max((System.nanoTime() - startedAtNanos) / 1_000_000_000.0d, 0.001d);
        return records / elapsedSeconds;
    }

    private static long estimateBytes(Object value) {
        if (value instanceof CellKpi kpi) {
            return estimateCellKpiBytes(kpi);
        }
        if (value instanceof AnomalyEvent anomaly) {
            return estimateAnomalyBytes(anomaly);
        }
        return value == null ? 0L : utf8Bytes(value.toString());
    }

    private static long latencyBaseTimestamp(Object value) {
        if (value instanceof CellKpi kpi) {
            return kpi.getSourceEventTsAvg() > 0L ? kpi.getSourceEventTsAvg() : Long.MIN_VALUE;
        }
        if (value instanceof AnomalyEvent anomaly) {
            return anomaly.getSourceEventTsAvg() > 0L ? anomaly.getSourceEventTsAvg() : Long.MIN_VALUE;
        }
        return Long.MIN_VALUE;
    }

    private static long estimateCellKpiBytes(CellKpi value) {
        return 8L * 11L
            + 4L * 7L
            + utf8Bytes(value.getSiteId())
            + utf8Bytes(value.getCellId())
            + utf8Bytes(value.getGridId())
            + utf8Bytes(value.getWindowKind());
    }

    private static long estimateAnomalyBytes(AnomalyEvent value) {
        return 8L * 8L
            + utf8Bytes(value.getImsi())
            + utf8Bytes(value.getSiteId())
            + utf8Bytes(value.getCellId())
            + utf8Bytes(value.getGridId())
            + utf8Bytes(value.getAnomalyType())
            + utf8Bytes(value.getSeverity())
            + utf8Bytes(value.getRuleVersion())
            + utf8Bytes(value.getContextJson());
    }

    private static int utf8Bytes(Object value) {
        return value == null ? 0 : value.toString().getBytes(StandardCharsets.UTF_8).length;
    }

}

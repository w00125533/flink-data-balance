package com.fdb.job.metrics;

import com.fdb.common.metrics.StageMetricSample;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class StageMetricsProbe<T> extends ProcessFunction<T, T> implements CheckpointedFunction {
    private static final Logger log = LoggerFactory.getLogger(StageMetricsProbe.class);

    private final String stageId;
    private final String displayName;
    private final String status;
    private final long emitIntervalMs;
    private final MetricRuntimeConfig metricConfig;
    private final LatencyTimestampExtractor<T> latencyTimestampExtractor;

    private transient MetricSamplePublisher metricPublisher;
    private transient Counter eventCounter;
    private final LatencyStats latencyStats = new LatencyStats();
    private long eventsSinceLastEmit;
    private long lastEmitAtMs = -1L;
    private double lastEps;
    private long latestWatermarkLagMs;
    private int subtaskIndex = -1;

    public StageMetricsProbe(String stageId, String displayName, String status, long emitIntervalMs) {
        this(stageId, displayName, status, emitIntervalMs, MetricRuntimeConfig.fromEnvironment());
    }

    public StageMetricsProbe(String stageId, String displayName, String status, long emitIntervalMs,
                             MetricRuntimeConfig metricConfig) {
        this(stageId, displayName, status, emitIntervalMs, metricConfig, null);
    }

    public StageMetricsProbe(String stageId, String displayName, String status, long emitIntervalMs,
                             MetricRuntimeConfig metricConfig,
                             LatencyTimestampExtractor<T> latencyTimestampExtractor) {
        this.stageId = stageId;
        this.displayName = displayName;
        this.status = status;
        this.emitIntervalMs = emitIntervalMs > 0 ? emitIntervalMs : 5_000L;
        this.metricConfig = metricConfig;
        this.latencyTimestampExtractor = latencyTimestampExtractor;
    }

    @Override
    public void open(Configuration parameters) {
        eventCounter = getRuntimeContext().getMetricGroup()
            .addGroup("fdb")
            .addGroup("stage", stageId)
            .counter("records_total");
        getRuntimeContext().getMetricGroup()
            .addGroup("fdb")
            .addGroup("stage", stageId)
            .gauge("eps", (Gauge<Double>) () -> lastEps);

        metricPublisher = new MetricSamplePublisher(metricConfig.metricsEnabled());
        subtaskIndex = runtimeSubtaskIndex();
    }

    @Override
    public void processElement(T value, Context ctx, Collector<T> out) {
        long nowMs = ctx.timerService().currentProcessingTime();
        record(value, nowMs, ctx.timerService().currentWatermark());
        publish(drainDueSamples(nowMs));
        out.collect(value);
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) {
        publish(drainPendingSamples(System.currentTimeMillis()));
    }

    @Override
    public void initializeState(FunctionInitializationContext context) {
    }

    @Override
    public void close() {
        if (metricPublisher != null) {
            metricPublisher.close();
        }
    }

    T record(T value, long nowMs) {
        return record(value, nowMs, Long.MIN_VALUE);
    }

    T record(T value, long nowMs, long currentWatermarkMs) {
        if (lastEmitAtMs < 0L) {
            lastEmitAtMs = nowMs;
        }
        eventsSinceLastEmit++;
        if (eventCounter != null) {
            eventCounter.inc();
        }
        if (shouldSample(eventsSinceLastEmit, metricConfig.stageSampleEveryRecords())) {
            recordLatency(value, nowMs);
        }
        if (currentWatermarkMs != Long.MIN_VALUE && currentWatermarkMs != Long.MAX_VALUE) {
            latestWatermarkLagMs = Math.max(0L, nowMs - currentWatermarkMs);
        }
        return value;
    }

    List<StageMetricSample> drainDueSamples(long nowMs) {
        return drainSamples(nowMs, false);
    }

    List<StageMetricSample> drainPendingSamples(long nowMs) {
        return drainSamples(nowMs, true);
    }

    private List<StageMetricSample> drainSamples(long nowMs, boolean force) {
        if (lastEmitAtMs < 0L || nowMs - lastEmitAtMs < emitIntervalMs) {
            if (!force) {
                return List.of();
            }
        }
        if (eventsSinceLastEmit <= 0L) {
            lastEmitAtMs = nowMs;
            return List.of();
        }
        double elapsedSeconds = Math.max((nowMs - lastEmitAtMs) / 1000.0d, 0.001d);
        lastEps = eventsSinceLastEmit / elapsedSeconds;
        LatencyStats.Snapshot latency = latencyStats.snapshotAndReset();
        StageMetricSample sample = StageMetricSample.stageLatency(stageId, displayName, status,
            lastEps, lastEps, latency.p50Ms(), latency.p95Ms(), latency.p99Ms(), latestWatermarkLagMs, 0L, nowMs)
            .withRunMetadata(metricConfig.runId(), metricConfig.resultSink(), metricConfig.parallelism(), subtaskIndex);
        eventsSinceLastEmit = 0L;
        lastEmitAtMs = nowMs;
        List<StageMetricSample> samples = new ArrayList<>();
        samples.add(sample);
        return samples;
    }

    private void recordLatency(T value, long nowMs) {
        if (latencyTimestampExtractor == null) {
            return;
        }
        try {
            latencyStats.recordObservedAt(nowMs, latencyTimestampExtractor.extractTimestampMs(value));
        } catch (RuntimeException e) {
            log.debug("Failed to extract latency timestamp for stage={}", stageId, e);
        }
    }

    private void publish(List<StageMetricSample> samples) {
        if (metricPublisher == null || samples.isEmpty()) {
            return;
        }
        for (StageMetricSample sample : samples) {
            try {
                metricPublisher.publish(sample);
            } catch (Exception e) {
                log.warn("Failed to publish stage metric sample for {}", sample.stageId(), e);
            }
        }
    }

    private static boolean shouldSample(long records, long everyRecords) {
        long effectiveEveryRecords = everyRecords > 0L ? everyRecords : 1L;
        return records % effectiveEveryRecords == 0L;
    }

    private int runtimeSubtaskIndex() {
        try {
            return getRuntimeContext().getIndexOfThisSubtask();
        } catch (IllegalStateException ignored) {
            return -1;
        }
    }

}

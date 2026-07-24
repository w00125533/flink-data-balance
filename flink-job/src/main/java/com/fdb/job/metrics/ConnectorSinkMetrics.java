package com.fdb.job.metrics;

import com.fdb.common.metrics.StageMetricSample;
import java.io.Serializable;

public final class ConnectorSinkMetrics implements AutoCloseable, Serializable {
    private static final long NANOSECONDS_PER_MILLISECOND = 1_000_000L;
    private static final NanoClock SYSTEM_CLOCK = System::nanoTime;

    private final String stageId;
    private final String displayName;
    private final String sinkType;
    private final String dataset;
    private final String windowKind;
    private final long emitEveryRecords;
    private final MetricRuntimeConfig metricConfig;
    private final NanoClock clock;
    private final LatencyStats writeStats = new LatencyStats();
    private final LatencyStats commitStats = new LatencyStats();
    private long writeRecords;
    private long commitRecords;
    private long writeFailures;
    private long commitFailures;
    private long latestCommitCheckpointId = -1L;
    private long writeStartedAtNanos = -1L;
    private long commitStartedAtNanos = -1L;
    private int subtaskIndex = -1;
    private transient MetricSamplePublisher metricPublisher;

    public ConnectorSinkMetrics(String stageId, String displayName, String sinkType, String dataset,
                                String windowKind, long emitEveryRecords, MetricRuntimeConfig metricConfig) {
        this(stageId, displayName, sinkType, dataset, windowKind, emitEveryRecords, metricConfig, SYSTEM_CLOCK);
    }

    ConnectorSinkMetrics(String stageId, String displayName, String sinkType, String dataset,
                         String windowKind, long emitEveryRecords, MetricRuntimeConfig metricConfig, NanoClock clock) {
        this.stageId = stageId;
        this.displayName = displayName;
        this.sinkType = sinkType;
        this.dataset = dataset;
        this.windowKind = windowKind;
        this.emitEveryRecords = emitEveryRecords > 0 ? emitEveryRecords : 100L;
        this.metricConfig = metricConfig;
        this.clock = clock;
    }

    static NanoClock systemClock() {
        return SYSTEM_CLOCK;
    }

    public void open(int subtaskIndex) {
        this.subtaskIndex = subtaskIndex;
        this.metricPublisher = new MetricSamplePublisher(metricConfig.metricsEnabled());
    }

    void recordWriteDurationNanos(long durationNanos) {
        if (writeStartedAtNanos < 0L) {
            writeStartedAtNanos = clock.nanoTime();
        }
        writeRecords++;
        writeStats.recordLatency(nanosToMillis(durationNanos));
    }

    void recordWriteFailure() {
        writeFailures++;
    }

    void recordCommitDurationNanos(long durationNanos, long checkpointId) {
        if (commitStartedAtNanos < 0L) {
            commitStartedAtNanos = clock.nanoTime();
        }
        commitRecords++;
        if (checkpointId >= 0L) {
            latestCommitCheckpointId = Math.max(latestCommitCheckpointId, checkpointId);
        }
        commitStats.recordLatency(nanosToMillis(durationNanos));
    }

    void recordCommitFailure() {
        commitFailures++;
    }

    void publishWriteIfDue() {
        if (metricPublisher == null || !metricPublisher.enabled()) {
            return;
        }
        if (writeRecords == 0L || writeRecords % emitEveryRecords != 0L) {
            return;
        }
        publish(writeSample(System.currentTimeMillis()));
    }

    void publishCommit() {
        if (metricPublisher == null || !metricPublisher.enabled()) {
            return;
        }
        if (commitRecords == 0L) {
            return;
        }
        publish(commitSample(System.currentTimeMillis()));
    }

    StageMetricSample writeSample(long nowMs) {
        LatencyStats.Snapshot latency = writeStats.snapshotAndReset();
        return StageMetricSample.sinkLatency(
            stageId + ".connector-write",
            displayName + " Connector Write",
            writeFailures == 0L ? "healthy" : "warning",
            sinkType,
            dataset,
            windowKind,
            writeRecords,
            0L,
            durationMs(writeStartedAtNanos),
            latency.p50Ms(),
            latency.p95Ms(),
            latency.p99Ms(),
            writeFailures,
            "",
            -1L,
            nowMs)
            .withRunMetadata(metricConfig.runId(), metricConfig.resultSink(), metricConfig.parallelism(),
                subtaskIndex);
    }

    StageMetricSample commitSample(long nowMs) {
        LatencyStats.Snapshot latency = commitStats.snapshotAndReset();
        return StageMetricSample.sinkLatency(
            stageId + ".connector-commit",
            displayName + " Connector Commit",
            commitFailures == 0L ? "healthy" : "warning",
            sinkType,
            dataset,
            windowKind,
            commitRecords,
            0L,
            durationMs(commitStartedAtNanos),
            latency.p50Ms(),
            latency.p95Ms(),
            latency.p99Ms(),
            commitFailures,
            "",
            latestCommitCheckpointId,
            nowMs)
            .withRunMetadata(metricConfig.runId(), metricConfig.resultSink(), metricConfig.parallelism(),
                subtaskIndex);
    }

    private void publish(StageMetricSample sample) {
        if (metricPublisher == null) {
            return;
        }
        try {
            metricPublisher.publish(sample);
        } catch (RuntimeException ignored) {
            // Connector metrics are best effort and must not affect sink reliability.
        }
    }

    private long durationMs(long startedAtNanos) {
        if (startedAtNanos < 0L) {
            return 0L;
        }
        return Math.max(0L, (clock.nanoTime() - startedAtNanos) / NANOSECONDS_PER_MILLISECOND);
    }

    private static long nanosToMillis(long nanos) {
        return Math.max(0L, nanos / NANOSECONDS_PER_MILLISECOND);
    }

    @Override
    public void close() {
        if (metricPublisher != null) {
            metricPublisher.close();
        }
    }

    @FunctionalInterface
    public interface NanoClock extends Serializable {
        long nanoTime();
    }
}

package com.fdb.job.metrics;

import com.fdb.common.metrics.StageMetricSample;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public final class WindowMaterializationProbe<T> extends ProcessFunction<T, T> {
    private static final Logger log = LoggerFactory.getLogger(WindowMaterializationProbe.class);
    private static final String SINK_TYPE = "window-materialization";
    private static final long FLUSH_INTERVAL_MS = 2_000L;

    private final String stageId;
    private final String displayName;
    private final String dataset;
    private final String windowKind;
    private final WindowEndTimestampExtractor<T> windowEndTimestampExtractor;
    private final MetricRuntimeConfig metricConfig;

    private transient MetricSamplePublisher metricPublisher;
    private transient ScheduledExecutorService flushExecutor;
    private transient Map<Long, Long> recordsByWindowEnd;
    private transient Map<Long, Long> emittedRecordsByWindowEnd;
    private long highestWindowEndTs = Long.MIN_VALUE;
    private int subtaskIndex = -1;

    public WindowMaterializationProbe(
        String stageId,
        String displayName,
        String dataset,
        String windowKind,
        WindowEndTimestampExtractor<T> windowEndTimestampExtractor,
        MetricRuntimeConfig metricConfig) {
        this.stageId = stageId;
        this.displayName = displayName;
        this.dataset = dataset;
        this.windowKind = windowKind;
        this.windowEndTimestampExtractor = windowEndTimestampExtractor;
        this.metricConfig = metricConfig;
    }

    @Override
    public void open(Configuration parameters) {
        metricPublisher = new MetricSamplePublisher(metricConfig.metricsEnabled());
        ensureState();
        subtaskIndex = runtimeSubtaskIndex();
        startFlushExecutor();
    }

    @Override
    public void processElement(T value, Context ctx, Collector<T> out) {
        long windowEndTs = windowEndTimestampExtractor.extractWindowEndTimestampMs(value);
        long nowMs = ctx.timerService().currentProcessingTime();
        recordWindowMaterialization(windowEndTs, nowMs)
            .forEach(this::publish);
        out.collect(value);
    }

    @Override
    public void close() {
        if (flushExecutor != null) {
            flushExecutor.shutdownNow();
            flushExecutor = null;
        }
        flushWindowMaterializationSamples(System.currentTimeMillis()).forEach(this::publish);
        if (metricPublisher != null) {
            metricPublisher.close();
        }
    }

    synchronized List<StageMetricSample> recordWindowMaterialization(long windowEndTs, long nowMs) {
        ensureState();
        recordsByWindowEnd.merge(windowEndTs, 1L, Long::sum);
        if (windowEndTs > highestWindowEndTs) {
            highestWindowEndTs = windowEndTs;
        }
        return samplesBefore(highestWindowEndTs, nowMs);
    }

    synchronized List<StageMetricSample> flushWindowMaterializationSamples(long nowMs) {
        ensureState();
        return samplesBefore(Long.MAX_VALUE, nowMs);
    }

    static StageMetricSample sample(
        String stageId,
        String displayName,
        String dataset,
        String windowKind,
        long windowEndTs,
        long records,
        MetricRuntimeConfig metricConfig,
        int subtaskIndex,
        long nowMs) {
        return StageMetricSample.sinkLatency(
                stageId,
                displayName,
                "healthy",
                SINK_TYPE,
                dataset,
                windowKind + "@" + windowEndTs,
                records,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                "",
                -1L,
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
        } catch (Exception e) {
            log.warn("Failed to publish window materialization metric for {}", sample.stageId(), e);
        }
    }

    private void startFlushExecutor() {
        if (!metricConfig.metricsEnabled()) {
            return;
        }
        flushExecutor = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, stageId + "-window-materialization-flush");
            thread.setDaemon(true);
            return thread;
        });
        flushExecutor.scheduleAtFixedRate(() -> {
            try {
                flushWindowMaterializationSamples(System.currentTimeMillis()).forEach(this::publish);
            } catch (RuntimeException e) {
                log.warn("Failed to flush window materialization metrics for {}", stageId, e);
            }
        }, FLUSH_INTERVAL_MS, FLUSH_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    private int runtimeSubtaskIndex() {
        try {
            return getRuntimeContext().getIndexOfThisSubtask();
        } catch (IllegalStateException ignored) {
            return -1;
        }
    }

    private void ensureState() {
        if (recordsByWindowEnd == null) {
            recordsByWindowEnd = new HashMap<>();
        }
        if (emittedRecordsByWindowEnd == null) {
            emittedRecordsByWindowEnd = new HashMap<>();
        }
    }

    private List<StageMetricSample> samplesBefore(long exclusiveWindowEndTs, long nowMs) {
        List<StageMetricSample> samples = new ArrayList<>();
        recordsByWindowEnd.entrySet().stream()
            .filter(entry -> entry.getKey() < exclusiveWindowEndTs)
            .sorted(Comparator.comparingLong(Map.Entry::getKey))
            .forEach(entry -> {
                long emittedRecords = emittedRecordsByWindowEnd.getOrDefault(entry.getKey(), -1L);
                if (emittedRecords == entry.getValue()) {
                    return;
                }
                emittedRecordsByWindowEnd.put(entry.getKey(), entry.getValue());
                samples.add(sample(stageId, displayName, dataset, windowKind, entry.getKey(), entry.getValue(),
                    metricConfig, subtaskIndex, nowMs));
            });
        return samples;
    }

    @FunctionalInterface
    public interface WindowEndTimestampExtractor<T> extends Serializable {
        long extractWindowEndTimestampMs(T value);
    }
}

package com.fdb.job;

import com.fdb.common.avro.CellKpi;
import com.fdb.common.metrics.StageMetricSample;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Locale;

public final class SinkPerformanceProbe extends ProcessFunction<CellKpi, CellKpi> {

    private static final Logger log = LoggerFactory.getLogger(SinkPerformanceProbe.class);
    private final String sinkName;
    private final long emitEveryRecords;
    private long records;
    private long approxBytes;
    private long firstRecordTs = -1L;
    private long latestRecordTs = -1L;
    private long startedAtNanos = -1L;
    private transient MetricSamplePublisher metricPublisher;

    public SinkPerformanceProbe(String sinkName, long emitEveryRecords) {
        this.sinkName = sinkName;
        this.emitEveryRecords = emitEveryRecords > 0 ? emitEveryRecords : 100L;
    }

    @Override
    public void open(Configuration parameters) {
        metricPublisher = new MetricSamplePublisher();
    }

    @Override
    public void processElement(CellKpi value, Context ctx, Collector<CellKpi> out) {
        record(value);
        publishMetric(ctx.timerService().currentProcessingTime());
        out.collect(value);
    }

    @Override
    public void close() {
        if (metricPublisher != null) {
            metricPublisher.close();
        }
    }

    CellKpi record(CellKpi value) {
        if (startedAtNanos < 0L) {
            startedAtNanos = System.nanoTime();
            firstRecordTs = value.getWindowStartTs();
        }
        latestRecordTs = value.getWindowStartTs();
        records++;
        approxBytes += estimateBytes(value);
        if (records == 1L || records % emitEveryRecords == 0L) {
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

    String summaryLine() {
        return String.format(Locale.ROOT,
            "[summary-code] sink=%s records=%d approx_bytes=%d first_record_ts=%d latest_record_ts=%d records_per_sec=%.2f",
            sinkName, records, approxBytes, firstRecordTs, latestRecordTs, recordsPerSecond());
    }

    private void publishMetric(long nowMs) {
        if (metricPublisher == null || records == 0L) {
            return;
        }
        if (records == 1L || records % emitEveryRecords == 0L) {
            metricPublisher.publish(StageMetricSample.sink(stageId(), displayName(), "healthy",
                sinkKind(), windowLabel(), records, approximateLatencyP95Ms(), nowMs));
        }
    }

    private String stageId() {
        if (sinkName.startsWith("hive-")) {
            return "hive-sink";
        }
        if (sinkName.startsWith("iceberg-")) {
            return "iceberg-sink";
        }
        if (sinkName.startsWith("mysql-")) {
            return "mysql-sink";
        }
        return sinkName;
    }

    private String displayName() {
        return switch (stageId()) {
            case "hive-sink" -> "Hive Sink";
            case "iceberg-sink" -> "Iceberg Sink";
            case "mysql-sink" -> "MySQL Sink";
            default -> sinkName;
        };
    }

    private String sinkKind() {
        return stageId().replace("-sink", "");
    }

    private String windowLabel() {
        if (sinkName.endsWith("-5m")) {
            return "5m";
        }
        if (sinkName.endsWith("-1m")) {
            return "1m";
        }
        return "anomaly";
    }

    private long approximateLatencyP95Ms() {
        if (records == 0L || startedAtNanos < 0L) {
            return 0L;
        }
        return Math.max(1L, Math.round(1000.0d / Math.max(recordsPerSecond(), 0.001d)));
    }

    private double recordsPerSecond() {
        if (records == 0L || startedAtNanos < 0L) {
            return 0.0d;
        }
        double elapsedSeconds = Math.max((System.nanoTime() - startedAtNanos) / 1_000_000_000.0d, 0.001d);
        return records / elapsedSeconds;
    }

    private static long estimateBytes(CellKpi value) {
        return 8L * 4L
            + 4L * 7L
            + utf8Bytes(value.getSiteId())
            + utf8Bytes(value.getCellId())
            + utf8Bytes(value.getGridId())
            + utf8Bytes(value.getWindowKind().toString());
    }

    private static int utf8Bytes(CharSequence value) {
        return value.toString().getBytes(StandardCharsets.UTF_8).length;
    }
}

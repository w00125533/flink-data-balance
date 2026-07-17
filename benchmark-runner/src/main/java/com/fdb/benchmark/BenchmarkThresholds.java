package com.fdb.benchmark;

import java.util.Map;

public record BenchmarkThresholds(
    double maxBackpressureRatio,
    long maxCheckpointDurationMs,
    int maxConsecutiveCheckpointFailures,
    long maxKpiAvailabilityP95Ms,
    long maxSinkP95Ms,
    long maxWatermarkLagMs,
    double minProducerDeliveryRatio,
    long maxSourceBacklogRecords) {

  public BenchmarkThresholds(
      double maxBackpressureRatio,
      long maxCheckpointDurationMs,
      int maxConsecutiveCheckpointFailures,
      long maxKpiAvailabilityP95Ms,
      long maxSinkP95Ms,
      long maxWatermarkLagMs) {
    this(
        maxBackpressureRatio,
        maxCheckpointDurationMs,
        maxConsecutiveCheckpointFailures,
        maxKpiAvailabilityP95Ms,
        maxSinkP95Ms,
        maxWatermarkLagMs,
        0.98,
        0);
  }

  public static BenchmarkThresholds from(Map<String, String> env) {
    return new BenchmarkThresholds(
        doubleValue(env, "FDB_BENCHMARK_MAX_BACKPRESSURE_RATIO", 0.2),
        longValue(env, "FDB_BENCHMARK_MAX_CHECKPOINT_DURATION_MS", 120_000),
        intValue(env, "FDB_BENCHMARK_MAX_CONSECUTIVE_CHECKPOINT_FAILURES", 2),
        longValue(env, "FDB_BENCHMARK_MAX_KPI_AVAILABILITY_P95_MS", 180_000),
        longValue(env, "FDB_BENCHMARK_MAX_SINK_P95_MS", 180_000),
        longValue(env, "FDB_BENCHMARK_MAX_WATERMARK_LAG_MS", 180_000),
        doubleValue(env, "FDB_BENCHMARK_MIN_PRODUCER_DELIVERY_RATIO", 0.98),
        longValue(env, "FDB_BENCHMARK_MAX_SOURCE_BACKLOG_RECORDS", 0));
  }

  static double doubleValue(Map<String, String> env, String key, double defaultValue) {
    String value = env.get(key);
    return value == null || value.isBlank() ? defaultValue : Double.parseDouble(value.trim());
  }

  static long longValue(Map<String, String> env, String key, long defaultValue) {
    String value = env.get(key);
    return value == null || value.isBlank() ? defaultValue : Long.parseLong(value.trim());
  }

  static int intValue(Map<String, String> env, String key, int defaultValue) {
    String value = env.get(key);
    return value == null || value.isBlank() ? defaultValue : Integer.parseInt(value.trim());
  }
}

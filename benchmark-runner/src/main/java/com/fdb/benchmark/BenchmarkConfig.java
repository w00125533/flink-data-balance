package com.fdb.benchmark;

import java.net.URI;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public record BenchmarkConfig(
    String target,
    String benchmarkId,
    List<BenchmarkSink> sinks,
    List<Integer> cellLevels,
    double chrEpsPerCell,
    double pmEpsPerCell,
    double anomalyInjectionRatio,
    long warmupSec,
    long durationSec,
    long pollIntervalSec,
    boolean earlyStopOnUnstable,
    long checkpointIntervalMs,
    URI flinkRestUrl,
    URI observabilityApiUrl,
    Path outputRoot,
    BenchmarkThresholds thresholds) {

  private static final Pattern SPLIT_PATTERN = Pattern.compile("[,\\s]+");
  private static final DateTimeFormatter BENCHMARK_ID_FORMATTER =
      DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss").withZone(ZoneOffset.UTC);

  public static BenchmarkConfig from(String target, Map<String, String> env) {
    return from(target, env, Clock.systemUTC());
  }

  static BenchmarkConfig from(String target, Map<String, String> env, Clock clock) {
    if (!"local".equals(target) && !"external-yarn".equals(target)) {
      throw new IllegalArgumentException("unsupported benchmark target: " + target);
    }
    List<BenchmarkSink> sinks = parseSinks(valueOrDefault(env, "FDB_BENCHMARK_SINKS",
        "none starrocks kafka hive iceberg"));
    List<Integer> cellLevels = parseCellLevels(valueOrDefault(env, "FDB_BENCHMARK_CELL_LEVELS",
        "10000 20000 40000"));
    String defaultId = "benchmark-" + BENCHMARK_ID_FORMATTER.format(Instant.now(clock));
    return new BenchmarkConfig(
        target,
        sanitize(valueOrDefault(env, "FDB_BENCHMARK_ID", defaultId)),
        List.copyOf(sinks),
        List.copyOf(cellLevels),
        finiteNonNegativeDouble(env, "FDB_BENCHMARK_CHR_EPS_PER_CELL", 30.0),
        finitePositiveDouble(env, "FDB_BENCHMARK_PM_EPS_PER_CELL", 1.0),
        boundedRatio(env, "FDB_BENCHMARK_ANOMALY_INJECTION_RATIO", 0.05),
        BenchmarkThresholds.longValue(env, "FDB_BENCHMARK_WARMUP_SEC", 60),
        BenchmarkThresholds.longValue(env, "FDB_BENCHMARK_DURATION_SEC", 300),
        BenchmarkThresholds.longValue(env, "FDB_BENCHMARK_POLL_INTERVAL_SEC", 10),
        booleanValue(env, "FDB_BENCHMARK_EARLY_STOP_ON_UNSTABLE", true),
        positiveLong(env, "FDB_FLINK_CHECKPOINT_INTERVAL_MS", 30_000),
        URI.create(valueOrDefault(env, "FDB_FLINK_REST_URL", "http://localhost:8081")),
        URI.create(valueOrDefault(env, "FDB_OBSERVABILITY_API_URL", "http://localhost:18080")),
        BenchmarkPaths.outputRoot(),
        BenchmarkThresholds.from(env));
  }

  public long targetChrTotalEps(int cellLevel) {
    return Math.round(cellLevel * chrEpsPerCell);
  }

  public long targetPmTotalEps(int cellLevel) {
    return Math.round(cellLevel * pmEpsPerCell);
  }

  static String sanitize(String raw) {
    String value = raw == null ? "" : raw.trim().replaceAll("[^A-Za-z0-9._:-]", "-");
    if (value.isBlank()) {
      throw new IllegalArgumentException("benchmark id became empty after sanitization");
    }
    return value;
  }

  private static String valueOrDefault(Map<String, String> env, String key, String defaultValue) {
    String value = env.get(key);
    return value == null || value.isBlank() ? defaultValue : value;
  }

  private static List<BenchmarkSink> parseSinks(String raw) {
    List<BenchmarkSink> sinks = new ArrayList<>();
    for (String token : SPLIT_PATTERN.split(raw.trim())) {
      if (!token.isBlank()) {
        sinks.add(BenchmarkSink.parse(token));
      }
    }
    if (sinks.isEmpty()) {
      throw new IllegalArgumentException("benchmark sinks must not be empty");
    }
    return sinks;
  }

  private static List<Integer> parseCellLevels(String raw) {
    List<Integer> levels = new ArrayList<>();
    for (String token : SPLIT_PATTERN.split(raw.trim())) {
      if (!token.isBlank()) {
        int level = Integer.parseInt(token);
        if (level <= 0) {
          throw new IllegalArgumentException("cell levels must be positive");
        }
        levels.add(level);
      }
    }
    if (levels.isEmpty()) {
      throw new IllegalArgumentException("cell levels must not be empty");
    }
    return levels;
  }

  private static double boundedRatio(Map<String, String> env, String key, double defaultValue) {
    double ratio = BenchmarkThresholds.doubleValue(env, key, defaultValue);
    if (!Double.isFinite(ratio) || ratio < 0.0d || ratio > 1.0d) {
      throw new IllegalArgumentException(key + " must be between 0 and 1");
    }
    return ratio;
  }

  private static double finiteNonNegativeDouble(Map<String, String> env, String key, double defaultValue) {
    double value = BenchmarkThresholds.doubleValue(env, key, defaultValue);
    if (!Double.isFinite(value) || value < 0.0d) {
      throw new IllegalArgumentException(key + " must be a finite non-negative number");
    }
    return value;
  }

  private static double finitePositiveDouble(Map<String, String> env, String key, double defaultValue) {
    double value = BenchmarkThresholds.doubleValue(env, key, defaultValue);
    if (!Double.isFinite(value) || value <= 0.0d) {
      throw new IllegalArgumentException(key + " must be a finite positive number");
    }
    return value;
  }

  private static long positiveLong(Map<String, String> env, String key, long defaultValue) {
    long value = BenchmarkThresholds.longValue(env, key, defaultValue);
    return value > 0 ? value : defaultValue;
  }

  private static boolean booleanValue(Map<String, String> env, String key, boolean defaultValue) {
    String value = env.get(key);
    return value == null || value.isBlank() ? defaultValue : Boolean.parseBoolean(value.trim());
  }

}

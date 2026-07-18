package com.fdb.benchmark;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class BenchmarkConfigTest {
  @Test
  void parses_sink_levels_and_threshold_defaults() {
    BenchmarkConfig config = BenchmarkConfig.from("local", Map.of(
        "FDB_BENCHMARK_SINKS", "none starrocks,kafka",
        "FDB_BENCHMARK_CELL_LEVELS", "1000 3000",
        "FDB_BENCHMARK_CHR_EPS_PER_CELL", "0.25",
        "FDB_BENCHMARK_PM_EPS_PER_CELL", "1.5",
        "FDB_BENCHMARK_WARMUP_SEC", "30",
        "FDB_BENCHMARK_DURATION_SEC", "120"));

    assertThat(config.target()).isEqualTo("local");
    assertThat(config.sinks()).containsExactly(BenchmarkSink.NONE, BenchmarkSink.STARROCKS, BenchmarkSink.KAFKA);
    assertThat(config.cellLevels()).containsExactly(1000, 3000);
    assertThat(config.chrEpsPerCell()).isEqualTo(0.25);
    assertThat(config.pmEpsPerCell()).isEqualTo(1.5);
    assertThat(config.anomalyInjectionRatio()).isEqualTo(0.05);
    assertThat(config.targetChrTotalEps(3000)).isEqualTo(750);
    assertThat(config.targetPmTotalEps(3000)).isEqualTo(4500);
    assertThat(config.warmupSec()).isEqualTo(30);
    assertThat(config.durationSec()).isEqualTo(120);
    assertThat(config.thresholds().maxCheckpointDurationMs()).isEqualTo(120_000L);
    assertThat(config.thresholds().minProducerDeliveryRatio()).isEqualTo(0.98);
    assertThat(config.thresholds().maxSourceBacklogRecords()).isEqualTo(0L);
    assertThat(config.outputRoot()).isEqualTo(Path.of("benchmark-runner/output/benchmark-runs"));
  }

  @Test
  void defaults_to_high_pressure_chr_and_pm_per_cell_rates() {
    BenchmarkConfig config = BenchmarkConfig.from("local", Map.of());

    assertThat(config.chrEpsPerCell()).isEqualTo(10.0);
    assertThat(config.pmEpsPerCell()).isEqualTo(1.0);
    assertThat(config.targetChrTotalEps(1000)).isEqualTo(10_000);
    assertThat(config.targetPmTotalEps(1000)).isEqualTo(1_000);
  }

  @Test
  void parses_anomaly_injection_ratio_from_env() {
    BenchmarkConfig config = BenchmarkConfig.from("local", Map.of(
        "FDB_BENCHMARK_ANOMALY_INJECTION_RATIO", "0.05"));

    assertThat(config.anomalyInjectionRatio()).isEqualTo(0.05);
  }

  @Test
  void allows_zero_and_one_anomaly_injection_ratio() {
    assertThat(BenchmarkConfig.from("local", Map.of(
        "FDB_BENCHMARK_ANOMALY_INJECTION_RATIO", "0")).anomalyInjectionRatio()).isZero();
    assertThat(BenchmarkConfig.from("local", Map.of(
        "FDB_BENCHMARK_ANOMALY_INJECTION_RATIO", "1")).anomalyInjectionRatio()).isEqualTo(1.0);
  }

  @Test
  void rejects_out_of_range_anomaly_injection_ratio() {
    assertThatThrownBy(() -> BenchmarkConfig.from("local", Map.of(
        "FDB_BENCHMARK_ANOMALY_INJECTION_RATIO", "-0.01")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("FDB_BENCHMARK_ANOMALY_INJECTION_RATIO");

    assertThatThrownBy(() -> BenchmarkConfig.from("local", Map.of(
        "FDB_BENCHMARK_ANOMALY_INJECTION_RATIO", "1.01")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("FDB_BENCHMARK_ANOMALY_INJECTION_RATIO");
  }

  @Test
  void rejects_non_finite_anomaly_injection_ratio() {
    for (String ratio : List.of("NaN", "Infinity", "-Infinity")) {
      assertThatThrownBy(() -> BenchmarkConfig.from("local", Map.of(
          "FDB_BENCHMARK_ANOMALY_INJECTION_RATIO", ratio)))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("FDB_BENCHMARK_ANOMALY_INJECTION_RATIO");
    }
  }

  @Test
  void rejects_invalid_pm_eps_per_cell() {
    for (String value : List.of("0", "-0.01", "NaN", "Infinity", "-Infinity")) {
      assertThatThrownBy(() -> BenchmarkConfig.from("local", Map.of(
          "FDB_BENCHMARK_PM_EPS_PER_CELL", value)))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("FDB_BENCHMARK_PM_EPS_PER_CELL");
    }
  }

  @Test
  void ignores_removed_topology_cells_per_site_for_chr_eps_estimate() {
    BenchmarkConfig config = BenchmarkConfig.from("local", Map.of(
        "FDB_BENCHMARK_CELL_LEVELS", "1000",
        "FDB_BENCHMARK_CHR_EPS_PER_CELL", "0.3",
        "FDB_BENCHMARK_TOPOLOGY_CELLS_PER_SITE", "4.5",
        "FDB_BENCHMARK_MIN_PRODUCER_DELIVERY_RATIO", "0.97",
        "FDB_BENCHMARK_MAX_SOURCE_BACKLOG_RECORDS", "123"));

    assertThat(config.targetChrTotalEps(1000)).isEqualTo(300);
    assertThat(config.thresholds().minProducerDeliveryRatio()).isEqualTo(0.97);
    assertThat(config.thresholds().maxSourceBacklogRecords()).isEqualTo(123L);
  }

  @Test
  void rejects_output_dir_override() {
    BenchmarkConfig config = BenchmarkConfig.from("local", Map.of(
        "FDB_BENCHMARK_OUTPUT_DIR", "/tmp/not-used"));

    assertThat(config.outputRoot()).isEqualTo(Path.of("benchmark-runner/output/benchmark-runs"));
  }

  @Test
  void blank_benchmark_id_uses_generated_default() {
    BenchmarkConfig config = BenchmarkConfig.from("local", Map.of("FDB_BENCHMARK_ID", ""));

    assertThat(config.benchmarkId()).startsWith("benchmark-");
  }

  @Test
  void rejects_invalid_sink_and_non_positive_level() {
    assertThatThrownBy(() -> BenchmarkConfig.from("local", Map.of("FDB_BENCHMARK_SINKS", "bad")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("unsupported benchmark sink");

    assertThatThrownBy(() -> BenchmarkConfig.from("local", Map.of("FDB_BENCHMARK_CELL_LEVELS", "1000 0")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("cell levels must be positive");
  }
}

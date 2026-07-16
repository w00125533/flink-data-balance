package com.fdb.benchmark;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Test;

class BenchmarkConfigTest {
  @Test
  void parses_sink_levels_and_threshold_defaults() {
    BenchmarkConfig config = BenchmarkConfig.from("local", Map.of(
        "FDB_BENCHMARK_SINKS", "none starrocks,kafka",
        "FDB_BENCHMARK_CELL_LEVELS", "1000 3000",
        "FDB_BENCHMARK_CHR_EPS_PER_CELL", "0.25",
        "FDB_BENCHMARK_WARMUP_SEC", "30",
        "FDB_BENCHMARK_DURATION_SEC", "120"));

    assertThat(config.target()).isEqualTo("local");
    assertThat(config.sinks()).containsExactly(BenchmarkSink.NONE, BenchmarkSink.STARROCKS, BenchmarkSink.KAFKA);
    assertThat(config.cellLevels()).containsExactly(1000, 3000);
    assertThat(config.chrEpsPerCell()).isEqualTo(0.25);
    assertThat(config.targetChrEps(3000)).isEqualTo(750);
    assertThat(config.warmupSec()).isEqualTo(30);
    assertThat(config.durationSec()).isEqualTo(120);
    assertThat(config.thresholds().maxCheckpointDurationMs()).isEqualTo(120_000L);
    assertThat(config.outputRoot()).isEqualTo(Path.of("benchmark-runner/output/benchmark-runs"));
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

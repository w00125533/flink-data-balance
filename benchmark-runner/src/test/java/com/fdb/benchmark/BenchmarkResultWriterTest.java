package com.fdb.benchmark;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class BenchmarkResultWriterTest {
  @TempDir Path tempDir;

  @Test
  void writes_config_results_and_csv() throws Exception {
    BenchmarkConfig config = BenchmarkConfig.from("local", Map.of(
        "FDB_BENCHMARK_ID", "bench-a",
        "FDB_BENCHMARK_SINKS", "none",
        "FDB_BENCHMARK_CELL_LEVELS", "1000",
        "FDB_BENCHMARK_CHR_EPS_PER_CELL", "0.3",
        "FDB_BENCHMARK_PM_EPS_PER_CELL", "0.1"));
    BenchmarkRunResult result = sampleResult(config);

    new BenchmarkResultWriter(tempDir).write(config, List.of(result));

    assertThat(tempDir.resolve("bench-a/benchmark-config.json")).exists();
    assertThat(tempDir.resolve("bench-a/benchmark-results.json")).exists();
    Path sourceMetrics = tempDir.resolve("bench-a/runs/bench-a-none-cells1000-chr-eps0.3/source-metrics.json");
    assertThat(sourceMetrics).exists();
    assertThat(Files.readString(sourceMetrics))
        .contains("\"present\" : true")
        .contains("\"chrTargetEps\" : 300")
        .contains("\"chrPublished\" : 600")
        .contains("\"chrDurationMs\" : 2000")
        .contains("\"chrBacklogRecords\" : 12")
        .contains("\"chrUndeliveredRecords\" : 12")
        .contains("\"pmTargetEps\" : 100")
        .contains("\"cfgTargetEps\" : 250");
    String csv = Files.readString(tempDir.resolve("bench-a/benchmark-summary.csv"));
    assertThat(csv).contains("sink,cellLevel,targetChrEpsPerCell,targetChrTotalEps,targetPmEpsPerCell,targetPmTotalEps,status");
    assertThat(csv).contains("none,1000,0.3,300,0.1,100,STABLE");
  }

  static BenchmarkRunResult sampleResult(BenchmarkConfig config) {
    BenchmarkRunPlan plan = BenchmarkMatrix.expand(config).get(0);
    return new BenchmarkRunResult(plan, BenchmarkStatus.STABLE, "all thresholds healthy",
        new FlinkSnapshot("RUNNING", 0, 10_000, 0, 300, 300, 1_200, 1_100, 1, 4, List.of(
            new FlinkOperatorSnapshot("source-1", "Source: chr-source -> chr-source-metrics", 4,
                300, 300, 600, 600, 8_192, 8_192, 0.2, 0.8, 0.0),
            new FlinkOperatorSnapshot("kpi-1m", "kpi-1m-full-join -> kpi-1m-metrics", 4,
                300, 100, 600, 200, 8_192, 2_048, 0.4, 0.6, 0.0),
            new FlinkOperatorSnapshot("sink-1", "starrocks-kpi-1m -> Sink: cell-kpi-starrocks-connector-sink", 4,
                100, 100, 200, 200, 2_048, 2_048, 0.3, 0.7, 0.0)),
            List.of(
                new FlinkOperatorEdge("source-1", "kpi-1m"),
                new FlinkOperatorEdge("kpi-1m", "sink-1"))),
        new FdbMetricsSnapshot(1, 2, 3, -1, 0, 5),
        new StorageSnapshot(true, "healthy", 10, 0, 0),
        new TopologyMetricsSnapshot(true, 1000, 200, 3, 50, 100, 180, 1000, 0,
            39.7d, 40.2d, 116.0d, 116.8d),
        new SourceMetricsSnapshot(true, 300, 600, 294.0, 2_000, 12, 12,
            100, 1000, 100.0, 10_000, 0, 0,
            250, 1000, 1000.0, 0, 0, 0));
  }
}

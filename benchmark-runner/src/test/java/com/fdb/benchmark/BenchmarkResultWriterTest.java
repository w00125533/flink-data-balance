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
        "FDB_BENCHMARK_CELL_LEVELS", "1000"));
    BenchmarkRunResult result = sampleResult(config);

    new BenchmarkResultWriter(tempDir).write(config, List.of(result));

    assertThat(tempDir.resolve("bench-a/benchmark-config.json")).exists();
    assertThat(tempDir.resolve("bench-a/benchmark-results.json")).exists();
    assertThat(tempDir.resolve("bench-a/runs/" + result.plan().runId() + "/topology-metrics.json")).exists();
    String csv = Files.readString(tempDir.resolve("bench-a/benchmark-summary.csv"));
    assertThat(csv).contains("sink,cellLevel,targetChrEps,status");
    assertThat(csv).contains("none,1000,300,STABLE");
  }

  static BenchmarkRunResult sampleResult(BenchmarkConfig config) {
    BenchmarkRunPlan plan = BenchmarkMatrix.expand(config).get(0);
    return new BenchmarkRunResult(plan, BenchmarkStatus.STABLE, "all thresholds healthy",
        new FlinkSnapshot("RUNNING", 0.025, 10_000, 0, 300, 300, 30_000, 29_000, 1, 4, List.of(
            new FlinkOperatorSnapshot("v1", "kpi-1m", 4, 300, 300, 20_000, 19_000, 8192, 4096, 0.8, 0.175, 0.025),
            new FlinkOperatorSnapshot("v2", "sink-starrocks", 4, 300, 300, 10_000, 10_000, 8192, 4096, 0.8, 0.175, 0.025)),
            List.of(new FlinkOperatorEdge("v1", "v2"))),
        new FdbMetricsSnapshot(1, 2, 3, 4, 0, 5, List.of(
            new StageLatencySnapshot("kpi-1m", 1, 2, 9, 5),
            new StageLatencySnapshot("kpi-5m", 2, 3, 10, 4)),
            List.of(
                new SinkLatencySnapshot("starrocks-kpi-1m", 100, 4096, 2, 4, 12_000, 0),
                new SinkLatencySnapshot("iceberg-kpi-1m", 100, 4096, 2, 4, 12_000, 0),
                new SinkLatencySnapshot("metrics-output", 10, 512, 1, 2, 3, 0),
                new SinkLatencySnapshot("enrichment-late-sink", 3, 128, 1, 2, 3, 0))),
        new StorageSnapshot(true, "healthy", 10, 0, 0),
        new TopologyMetricsSnapshot(true, 6000, 1000, 4, 120, 450, 570, 6000, 0,
            30.1, 31.2, 120.3, 121.4));
  }
}

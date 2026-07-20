package com.fdb.benchmark;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class HtmlReportWriterTest {
  @TempDir Path tempDir;

  @Test
  void writes_index_and_run_report_with_links_and_recommendations() throws Exception {
    BenchmarkConfig config = BenchmarkConfig.from("local", Map.of(
        "FDB_BENCHMARK_ID", "bench-a",
        "FDB_BENCHMARK_SINKS", "none",
        "FDB_BENCHMARK_CELL_LEVELS", "1000",
        "FDB_FLINK_CHECKPOINT_INTERVAL_MS", "60000",
        "FDB_BENCHMARK_MAX_SOURCE_BACKLOG_RECORDS", "20"));
    BenchmarkRunResult result = BenchmarkResultWriterTest.sampleResult(config);

    new HtmlReportWriter(tempDir).write(config, List.of(result));

    Path index = tempDir.resolve("bench-a/index.html");
    Path report = tempDir.resolve("bench-a/runs/" + result.plan().runId() + "/report.html");
    assertThat(index).exists();
    assertThat(report).exists();
    String html = Files.readString(index);
    assertThat(html).contains("Sink Upper-Bound Benchmark");
    assertThat(html).contains("Stable upper bounds");
    assertThat(html).contains("runs/" + result.plan().runId() + "/report.html");
    assertThat(html).contains("Recommendations");
    assertThat(Files.readString(report))
        .contains("Run Summary")
        .contains("Stability Gates")
        .contains("<th>Metric</th><th>Observed</th><th>Threshold</th><th>Health</th>")
        .contains("<td>Source Throughput Attainment</td><td>98.00%</td><td>&gt;= 98.00%</td><td><span class=\"health health-ok\">HEALTHY</span></td>")
        .contains("<td>Source Operator Backlog</td><td>0 records</td><td>&lt;= 20 records</td><td><span class=\"health health-ok\">HEALTHY</span></td>")
        .contains("<td>Sink P95</td><td>N/A</td><td>&lt;= 180000 ms</td><td><span class=\"health health-na\">N/A</span></td>")
        .contains("Source Density")
        .contains("CHR total")
        .contains("PM total")
        .contains("CFG total")
        .contains("Source Throughput Attainment")
        .contains("Source Backlog")
        .contains("<tr><th>Source Backlog</th><td>12 CHR records</td></tr>")
        .contains("Published Topology Records")
        .contains("Checkpoint Interval")
        .contains("<tr><th>Target CHR EPS</th><td>30 records/cell/s</td></tr>")
        .contains("<tr><th>Global CHR EPS</th><td>30000 records/s</td></tr>")
        .contains("<tr><th>Target PM EPS</th><td>1 records/cell/s</td></tr>")
        .contains("<tr><th>Global PM EPS</th><td>1000 records/s</td></tr>")
        .contains("<tr><th>Checkpoint Interval</th><td>60000 ms</td></tr>")
        .contains("<tr><th>Checkpoint Duration</th><td>10000 ms</td></tr>")
        .contains("Latency")
        .contains("N/A")
        .contains("Flink Resources")
        .contains("Operator Flow")
        .contains("operator-flow")
        .contains("flow-node")
        .contains("flow-edge")
        .contains("data-operator-id=\"source-1\"")
        .contains("operator-row-source-1")
        .contains("Operator Throughput")
        .contains("Sink &amp; Storage")
        .contains("Raw Artifacts")
        .contains("run.json")
        .contains("flink-snapshot.json")
        .contains("fdb-metrics-snapshot.json")
        .contains("source-metrics.json")
        .contains("topology-metrics.json");
  }

  @Test
  void marks_unhealthy_thresholds_in_the_run_report() throws Exception {
    BenchmarkConfig config = BenchmarkConfig.from("local", Map.of(
        "FDB_BENCHMARK_ID", "bench-b",
        "FDB_BENCHMARK_SINKS", "none",
        "FDB_BENCHMARK_CELL_LEVELS", "1000"));
    BenchmarkRunPlan plan = BenchmarkMatrix.expand(config).get(0);
    BenchmarkRunResult result = new BenchmarkRunResult(plan, BenchmarkStatus.UNSTABLE,
        "watermark lag 181000 ms",
        new FlinkSnapshot("RUNNING", 0, 10_000, 0, 300, 300, 1_200, 1_100, 1, 4),
        new FdbMetricsSnapshot(1, 2, 3, -1, 0, 181_000),
        new StorageSnapshot(true, "healthy", 10, 0, 0),
        TopologyMetricsSnapshot.empty(),
        new SourceMetricsSnapshot(true, 300, 600, 294.0, 2_000, 0, 0,
            100, 1000, 100.0, 10_000, 0, 0,
            250, 1000, 1000.0, 0, 0, 0));

    new HtmlReportWriter(tempDir).write(config, List.of(result));

    String html = Files.readString(tempDir.resolve("bench-b/runs/" + result.plan().runId() + "/report.html"));
    assertThat(html)
        .contains("<td>Watermark Lag</td><td>181000 ms</td><td>&lt;= 180000 ms</td><td><span class=\"health health-warn\">UNSTABLE</span></td>");
  }

  @Test
  void report_labels_sampled_rates_missing_latency_and_starrocks_rows() throws Exception {
    BenchmarkConfig config = BenchmarkConfig.from("local", Map.of(
        "FDB_BENCHMARK_ID", "bench-c",
        "FDB_BENCHMARK_SINKS", "starrocks",
        "FDB_BENCHMARK_CELL_LEVELS", "1000"));
    BenchmarkRunPlan plan = BenchmarkMatrix.expand(config).get(0);
    BenchmarkRunResult result = new BenchmarkRunResult(plan, BenchmarkStatus.UNSTABLE,
        "KPI availability p95 over threshold",
        new FlinkSnapshot("RUNNING", 0, 10_000, 0, 300, 300, 1_200, 1_100, 1, 4, List.of(
            new FlinkOperatorSnapshot("kpi-1m", "kpi-1m-full-join", 4,
                300, 100, 600, 200, 8_192, 2_048, 0.4, 0.6, 0.0))),
        new FdbMetricsSnapshot(1, 2, 3, -1, 0, 5, List.of(
            new StageLatencySnapshot("kpi-1m", 10, 20, -1, 5)),
            List.of(new SinkLatencySnapshot("starrocks-kpi-1m", 200, 20_000, 10, 20, -1, 0))),
        new StorageSnapshot(true, "starrocks rows=12345", 12_345, 0, 0),
        TopologyMetricsSnapshot.empty(),
        new SourceMetricsSnapshot(true, 300, 600, 294.0, 2_000, 12, 12,
            100, 1000, 100.0, 10_000, 0, 0,
            250, 1000, 1000.0, 0, 0, 0));

    new HtmlReportWriter(tempDir).write(config, List.of(result));

    String html = Files.readString(tempDir.resolve("bench-c/runs/" + plan.runId() + "/report.html"));
    assertThat(html)
        .contains("Sampled Avg Out EPS")
        .contains("<tr><th>Sampled Avg Records In/s</th><td>300</td></tr>")
        .contains("<tr><th>Operator Aggregate Records In Total</th><td>1200</td></tr>")
        .contains("<tr><td>kpi-1m</td><td>10 ms</td><td>20 ms</td><td>N/A</td><td>5 ms</td></tr>")
        .contains("<tr><th>CFG total</th><td>1000</td><td>1000.00</td><td>1.00</td><td>N/A (one-time init)</td></tr>")
        .contains("<tr><td>Storage Rows</td><td>12345</td><td>-</td><td colspan=\"5\">starrocks rows=12345</td></tr>")
        .doesNotContain("Storage Files</td><td>12345");
  }
}

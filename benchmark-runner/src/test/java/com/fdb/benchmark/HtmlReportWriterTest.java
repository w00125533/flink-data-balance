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
        .contains("Operator Flow &amp; Metrics")
        .contains("Cumulative Business Age P95")
        .contains("Flink Marker P95")
        .contains("Column Guide")
        .contains("Abnormal Value Analysis")
        .contains("latency marker unavailable")
        .contains("N/A")
        .contains("Flink Resources")
        .contains("operator-flow")
        .contains("flow-node")
        .contains("flow-edge")
        .contains("data-operator-id=\"source-1\"")
        .contains("operator-row-source-1")
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
        new FdbMetricsSnapshot(1, 2, 3, 20, 10, 33, 0, 5, List.of(
            new StageLatencySnapshot("kpi-1m", 10, 20, -1, 5)),
            List.of(
                new SinkLatencySnapshot("starrocks-kpi-1m", 200, 20_000, 10, 20, -1, 0),
                new SinkLatencySnapshot("starrocks-kpi-1m.connector-write", 200, 0, 4, 10, 12, 0),
                new SinkLatencySnapshot("starrocks-kpi-1m.connector-commit", 4, 0, 20, 33, 40, 0))),
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
        .contains("<th>Cumulative Business Age P50</th><th>Cumulative Business Age P95</th><th>Cumulative Business Age P99</th>")
        .contains("<td>kpi-1m-full-join</td><td>kpi</td><td>4</td><td>300</td><td>100</td>")
        .contains("<td>10 ms</td><td>20 ms</td><td>N/A</td><td>5 ms</td><td>stage:kpi-1m</td><td>output-age</td>")
        .contains("<tr><th>CFG total</th><td>1000</td><td>1000.00</td><td>1.00</td><td>N/A (one-time init)</td></tr>")
        .contains("Cumulative Business Age P95 high while Flink Marker P95 is normal")
        .contains("Flink Marker P95 high")
        .contains("Connector Write P95")
        .contains("Connector Commit P95")
        .contains("<td>connector-commit</td><td>starrocks-kpi-1m.connector-commit</td>")
        .contains("Latency Confidence")
        .contains("<tr><td>Storage Rows</td><td>storage</td><td>starrocks</td><td>12345</td><td>-</td><td colspan=\"5\">starrocks rows=12345</td></tr>")
        .doesNotContain("Storage Files</td><td>12345");
  }

  @Test
  void report_marks_untrusted_operator_metrics_as_unavailable() throws Exception {
    BenchmarkConfig config = BenchmarkConfig.from("local", Map.of(
        "FDB_BENCHMARK_ID", "bench-missing-operator-metrics",
        "FDB_BENCHMARK_SINKS", "none",
        "FDB_BENCHMARK_CELL_LEVELS", "1000"));
    BenchmarkRunPlan plan = BenchmarkMatrix.expand(config).get(0);
    BenchmarkRunResult result = new BenchmarkRunResult(plan, BenchmarkStatus.STABLE,
        "all thresholds healthy",
        new FlinkSnapshot("RUNNING", 0, 10_000, 0, 0, 0, 0, 0, 1, 4, List.of(
            new FlinkOperatorSnapshot("source-1", "Source: chr-source", 4,
                0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, -1, -1, -1, List.of(), false))),
        new FdbMetricsSnapshot(1, 2, -1, -1, 0, 0, List.of(), List.of()),
        new StorageSnapshot(true, "healthy", 0, 0, 0),
        TopologyMetricsSnapshot.empty(),
        new SourceMetricsSnapshot(true, 30_000, 90_000, 30_000.0, 3_000, 0, 0,
            1_000, 3_000, 1_000.0, 3_000, 0, 0,
            1_000, 1_000, 0.0, 0, 0, 0));

    new HtmlReportWriter(tempDir).write(config, List.of(result));

    String html = Files.readString(tempDir.resolve(
        "bench-missing-operator-metrics/runs/" + plan.runId() + "/report.html"));
    assertThat(html)
        .contains("<tr><th>Operator Metrics Validity</th><td>unavailable")
        .contains("<td>Source: chr-source</td><td>source</td><td>4</td><td>N/A</td><td>N/A</td><td>N/A</td><td>N/A</td>")
        .contains("<td>Business Latency Mapping</td><td>0/1 operators</td><td>shown</td>")
        .contains("unmapped business probe")
        .contains("no probe")
        .doesNotContain("P95 N/A")
        .contains("out/s=N/A")
        .contains("bp=N/A");
  }

  @Test
  void operator_table_hides_all_empty_optional_columns_and_reports_metric_coverage() throws Exception {
    BenchmarkConfig config = BenchmarkConfig.from("local", Map.of(
        "FDB_BENCHMARK_ID", "bench-optional-coverage",
        "FDB_BENCHMARK_SINKS", "none",
        "FDB_BENCHMARK_CELL_LEVELS", "1000"));
    BenchmarkRunPlan plan = BenchmarkMatrix.expand(config).get(0);
    BenchmarkRunResult result = new BenchmarkRunResult(plan, BenchmarkStatus.STABLE,
        "all thresholds healthy",
        new FlinkSnapshot("RUNNING", 0, 10_000, 0, 300, 300, 1_200, 1_100, 1, 4, List.of(
            new FlinkOperatorSnapshot("source-1", "Source: chr-source", 4,
                300, 300, 600, 600, 8_192, 8_192, 0.2, 0.8, 0.0,
                0, 123456L, -1L))),
        new FdbMetricsSnapshot(1, 2, -1, -1, 0, 0, List.of(
            new StageLatencySnapshot("chr-source", 1, 2, 3, 4)), List.of()),
        new StorageSnapshot(true, "healthy", 0, 0, 0),
        TopologyMetricsSnapshot.empty(),
        new SourceMetricsSnapshot(true, 300, 600, 294.0, 2_000, 0, 0,
            100, 1000, 100.0, 10_000, 0, 0,
            250, 1000, 1000.0, 0, 0, 0));

    new HtmlReportWriter(tempDir).write(config, List.of(result));

    String html = Files.readString(tempDir.resolve(
        "bench-optional-coverage/runs/" + plan.runId() + "/report.html"));
    assertThat(html)
        .contains("Metric Coverage")
        .contains("<td>Flink Operator Metrics</td><td>1/1 operators</td><td>shown</td>")
        .contains("<td>Input Watermark</td><td>1/1 operators</td><td>shown</td>")
        .contains("<td>Output Watermark</td><td>0/1 operators</td><td>hidden")
        .contains("<td>Flink Marker P95</td><td>0/1 operators</td><td>hidden")
        .contains("<th>Input Watermark</th>")
        .doesNotContain("<th>Input Watermark</th><th>Output Watermark</th><th>Backpressure</th>")
        .doesNotContain("<th>Watermark Lag</th><th>Flink Marker P95</th>");
  }

  @Test
  void run_report_includes_taskmanager_cpu_and_operator_pressure_ms_per_second() throws Exception {
    BenchmarkConfig config = BenchmarkConfig.from("local", Map.of(
        "FDB_BENCHMARK_ID", "bench-cpu-pressure",
        "FDB_BENCHMARK_SINKS", "none",
        "FDB_BENCHMARK_CELL_LEVELS", "1000"));
    BenchmarkRunPlan plan = BenchmarkMatrix.expand(config).get(0);
    BenchmarkRunResult result = new BenchmarkRunResult(plan, BenchmarkStatus.STABLE,
        "all thresholds healthy",
        new FlinkSnapshot("RUNNING", 0.25, 10_000, 0, 300, 300, 1_200, 1_100, 1, 4,
            0.73, List.of(
                new FlinkOperatorSnapshot("source-1", "Source: chr-source", 4,
                    300, 300, 600, 600, 8_192, 8_192, 0.8, 0.15, 0.05))),
        new FdbMetricsSnapshot(1, 2, -1, -1, 0, 0, List.of(
            new StageLatencySnapshot("chr-source", 1, 2, 3, 4)), List.of()),
        new StorageSnapshot(true, "healthy", 0, 0, 0),
        TopologyMetricsSnapshot.empty(),
        new SourceMetricsSnapshot(true, 300, 600, 294.0, 2_000, 0, 0,
            100, 1000, 100.0, 10_000, 0, 0,
            250, 1000, 1000.0, 0, 0, 0));

    new HtmlReportWriter(tempDir).write(config, List.of(result));

    String html = Files.readString(tempDir.resolve(
        "bench-cpu-pressure/runs/" + plan.runId() + "/report.html"));
    assertThat(html)
        .contains("<tr><th>TaskManager CPU Load</th><td>73.00%</td></tr>")
        .contains("<th>Busy ms/s</th>")
        .contains("<th>Backpressure ms/s</th>")
        .contains("<td>80.00%</td><td>800 ms/s</td><td>15.00%</td><td>5.00%</td><td>50 ms/s</td>");
  }

  @Test
  void sink_storage_table_marks_low_latency_sample_confidence() throws Exception {
    BenchmarkConfig config = BenchmarkConfig.from("local", Map.of(
        "FDB_BENCHMARK_ID", "bench-low-confidence",
        "FDB_BENCHMARK_SINKS", "starrocks",
        "FDB_BENCHMARK_CELL_LEVELS", "1000"));
    BenchmarkRunPlan plan = BenchmarkMatrix.expand(config).get(0);
    BenchmarkRunResult result = new BenchmarkRunResult(plan, BenchmarkStatus.STABLE,
        "all thresholds healthy",
        new FlinkSnapshot("RUNNING", 0, 10_000, 0, 300, 300, 1_200, 1_100, 1, 4),
        new FdbMetricsSnapshot(1, 2, -1, 12, 0, 0, List.of(),
            List.of(new SinkLatencySnapshot("starrocks-cell-anomaly", 6, 1_024, 10, 12, 12, 0))),
        new StorageSnapshot(true, "starrocks rows=6", 6, 0, 0),
        TopologyMetricsSnapshot.empty(),
        new SourceMetricsSnapshot(true, 300, 600, 294.0, 2_000, 0, 0,
            100, 1000, 100.0, 10_000, 0, 0,
            250, 1000, 1000.0, 0, 0, 0));

    new HtmlReportWriter(tempDir).write(config, List.of(result));

    String html = Files.readString(tempDir.resolve("bench-low-confidence/runs/" + plan.runId() + "/report.html"));
    assertThat(html)
        .contains("Latency Confidence")
        .contains("<td>low (&lt;30 records)</td>");
  }

  @Test
  void operator_flow_marks_high_latency_nodes_with_value_and_color() throws Exception {
    BenchmarkConfig config = BenchmarkConfig.from("local", Map.of(
        "FDB_BENCHMARK_ID", "bench-latency-flow",
        "FDB_BENCHMARK_SINKS", "none",
        "FDB_BENCHMARK_CELL_LEVELS", "1000",
        "FDB_BENCHMARK_MAX_KPI_AVAILABILITY_P95_MS", "15"));
    BenchmarkRunPlan plan = BenchmarkMatrix.expand(config).get(0);
    BenchmarkRunResult result = new BenchmarkRunResult(plan, BenchmarkStatus.UNSTABLE,
        "KPI availability p95 over threshold",
        new FlinkSnapshot("RUNNING", 0, 10_000, 0, 300, 100, 600, 200, 1, 4, List.of(
            new FlinkOperatorSnapshot("source-1", "Source: chr-source", 4,
                300, 300, 600, 600, 8_192, 8_192, 0.2, 0.8, 0.0),
            new FlinkOperatorSnapshot("map-1", "normal-map", 4,
                300, 300, 600, 600, 8_192, 8_192, 0.2, 0.8, 0.0),
            new FlinkOperatorSnapshot("kpi-1m", "kpi-1m-full-join", 4,
                300, 100, 600, 200, 8_192, 2_048, 0.4, 0.6, 0.0))),
        new FdbMetricsSnapshot(1, 2, 20, -1, 0, 5, List.of(
            new StageLatencySnapshot("chr-source", 2, 5, 7, 3),
            new StageLatencySnapshot("kpi-1m", 10, 20, 30, 5)),
            List.of()),
        new StorageSnapshot(true, "healthy", 0, 0, 0),
        TopologyMetricsSnapshot.empty(),
        new SourceMetricsSnapshot(true, 300, 600, 294.0, 2_000, 0, 0,
            100, 1000, 100.0, 10_000, 0, 0,
            250, 1000, 1000.0, 0, 0, 0));

    new HtmlReportWriter(tempDir).write(config, List.of(result));

    String html = Files.readString(tempDir.resolve("bench-latency-flow/runs/" + plan.runId() + "/report.html"));
    assertThat(html)
        .contains("flow-node flow-node-warn")
        .contains("<text class=\"flow-latency\" x=\"10\" y=\"62\">P95 20 ms</text>")
        .contains("flow-node flow-node-ok")
        .contains("flow-node flow-node-na")
        .contains("data-latency-health=\"UNSTABLE\"");
  }

  @Test
  void operator_flow_shows_marker_latency_on_edges_and_details_as_operator_rows() throws Exception {
    BenchmarkConfig config = BenchmarkConfig.from("local", Map.of(
        "FDB_BENCHMARK_ID", "bench-marker-flow",
        "FDB_BENCHMARK_SINKS", "none",
        "FDB_BENCHMARK_CELL_LEVELS", "1000",
        "FDB_BENCHMARK_MAX_WATERMARK_LAG_MS", "100"));
    BenchmarkRunPlan plan = BenchmarkMatrix.expand(config).get(0);
    BenchmarkRunResult result = new BenchmarkRunResult(plan, BenchmarkStatus.UNSTABLE,
        "Flink marker p95 over threshold",
        new FlinkSnapshot("RUNNING", 0, 10_000, 0, 300, 100, 600, 200, 1, 4, List.of(
            new FlinkOperatorSnapshot("chr-source", "Source: chr-source", 4,
                300, 300, 600, 600, 8_192, 8_192, 0.2, 0.8, 0.0),
            new FlinkOperatorSnapshot("pm-source", "Source: pm-source", 4,
                10, 10, 20, 20, 1_024, 1_024, 0.1, 0.9, 0.0),
            new FlinkOperatorSnapshot("kpi-join", "kpi-1m-full-join", 4,
                310, 100, 620, 200, 8_192, 2_048, 0.4, 0.6, 0.0,
                0, -1, -1, 230, List.of(
                    new FlinkMarkerLatencySnapshot("chr-source", "kpi-join", 230,
                        "latency.chr-source.kpi-join.latency_p95"),
                    new FlinkMarkerLatencySnapshot("pm-source", "kpi-join", 18,
                        "latency.pm-source.kpi-join.latency_p95")))),
            List.of(
                new FlinkOperatorEdge("chr-source", "kpi-join"),
                new FlinkOperatorEdge("pm-source", "kpi-join"))),
        new FdbMetricsSnapshot(1, 2, 20, -1, 0, 5, List.of(), List.of()),
        new StorageSnapshot(true, "healthy", 0, 0, 0),
        TopologyMetricsSnapshot.empty(),
        new SourceMetricsSnapshot(true, 300, 600, 294.0, 2_000, 0, 0,
            100, 1000, 100.0, 10_000, 0, 0,
            250, 1000, 1000.0, 0, 0, 0));

    new HtmlReportWriter(tempDir).write(config, List.of(result));

    String html = Files.readString(tempDir.resolve("bench-marker-flow/runs/" + plan.runId() + "/report.html"));
    assertThat(html)
        .contains("data-source-id=\"chr-source\" data-target-id=\"kpi-join\"")
        .contains("flow-edge-marker-warn")
        .contains("<text class=\"flow-edge-marker-label\"")
        .contains(">P95 230 ms</text>")
        .contains("marker: chr-source -> kpi-join")
        .contains("marker: pm-source -> kpi-join")
        .contains("<td>Flink marker</td><td>transport</td>")
        .doesNotContain("Marker P95 230 ms");
  }

  @Test
  void run_report_includes_one_minute_window_diagnostics() throws Exception {
    BenchmarkConfig config = BenchmarkConfig.from("local", Map.of(
        "FDB_BENCHMARK_ID", "bench-d",
        "FDB_BENCHMARK_SINKS", "hive",
        "FDB_BENCHMARK_CELL_LEVELS", "10000"));
    BenchmarkRunPlan plan = BenchmarkMatrix.expand(config).get(0);
    BenchmarkRunResult result = new BenchmarkRunResult(plan, BenchmarkStatus.UNSTABLE,
        "1m window materialization lag: expected >= 4 closed minutes, CHR=0, PM=4, KPI=0",
        new FlinkSnapshot("RUNNING", 0, 30_000, 0, 310_000, 310_000,
            126_000_000, 126_000_000, 1, 6, List.of(
                new FlinkOperatorSnapshot("chr-1m", "chr-1m-fact -> to-chr-minute-fact-env", 6,
                    50_000, 0, 17_422_480, 0, 8_192, 0, 0.2, 0.8, 0.0),
                new FlinkOperatorSnapshot("pm-1m", "pm-1m-fact -> to-pm-minute-fact-env", 6,
                    10_000, 10_000, 3_260_000, 40_000, 8_192, 2_048, 0.2, 0.8, 0.0),
                new FlinkOperatorSnapshot("kpi-1m", "kpi-1m-full-join -> kpi-1m-metrics", 6,
                    10_000, 0, 50_000, 0, 8_192, 0, 0.2, 0.8, 0.0))),
        new FdbMetricsSnapshot(1, -1, -1, -1, 0, 0, List.of(), List.of(
            windowMaterialization("window-pm-1m", "pm-1m", "MIN_1@60000", 10_000),
            windowMaterialization("window-pm-1m", "pm-1m", "MIN_1@120000", 10_000),
            windowMaterialization("window-pm-1m", "pm-1m", "MIN_1@180000", 10_000),
            windowMaterialization("window-pm-1m", "pm-1m", "MIN_1@240000", 10_000))),
        new StorageSnapshot(true, "hive in-progress files=12", 0, 0, 12),
        TopologyMetricsSnapshot.empty(),
        new SourceMetricsSnapshot(true, 300_000, 90_000_000, 300_000.0, 343_000, 0, 0,
            10_000, 3_430_000, 10_000.0, 343_000, 0, 0,
            10_000, 10_000, 0.0, 0, 0, 0));

    new HtmlReportWriter(tempDir).write(config, List.of(result));

    String html = Files.readString(tempDir.resolve("bench-d/runs/" + plan.runId() + "/report.html"));
    assertThat(html)
        .contains("1m Window Diagnostics")
        .contains("<td>1m Window Materialization</td><td>CHR=0, PM=4, KPI=0 closed minutes</td><td>&gt;= 4 closed minutes</td><td><span class=\"health health-warn\">UNSTABLE</span></td>")
        .contains("<thead><tr><th>Stage</th><th>Records In Total</th><th>Records Out Total</th><th>Closed Minutes</th><th>Expected Closed Minutes</th><th>Health</th></tr></thead>")
        .contains("<tr><td>CHR 1m</td><td>0</td><td>0</td><td>0</td><td>4</td>")
        .contains("<tr><td>PM 1m</td><td>0</td><td>40000</td><td>4</td><td>4</td>")
        .contains("<tr><td>KPI 1m</td><td>0</td><td>0</td><td>0</td><td>4</td>");
  }

  private static SinkLatencySnapshot windowMaterialization(
      String sinkName, String dataset, String windowKind, long records) {
    return new SinkLatencySnapshot(
        sinkName, "window-materialization", dataset, windowKind, records, 0, 0, 0, 0, 0);
  }
}

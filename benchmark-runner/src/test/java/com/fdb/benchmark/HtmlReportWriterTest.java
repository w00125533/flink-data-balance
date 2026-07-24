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
    assertThat(html).contains("Sink 上限压测");
    assertThat(html).contains("稳定上限");
    assertThat(html).contains("runs/" + result.plan().runId() + "/report.html");
    assertThat(html).contains("建议");
    assertThat(Files.readString(report))
        .contains("运行摘要")
        .contains("稳定性门禁")
        .contains("<th>指标</th><th>观测值</th><th>门限</th><th>健康状态</th>")
        .contains("<td>Source 吞吐达成率</td><td>98.00%</td><td>&gt;= 98.00%</td><td><span class=\"health health-ok\">HEALTHY</span></td>")
        .contains("<td>Source 算子积压</td><td>0 条</td><td>&lt;= 20 条</td><td><span class=\"health health-ok\">HEALTHY</span></td>")
        .contains("<td>Sink P95</td><td>N/A</td><td>&lt;= 180000 ms</td><td><span class=\"health health-na\">N/A</span></td>")
        .contains("Source 密度")
        .contains("CHR 总量")
        .contains("PM 总量")
        .contains("CFG 总量")
        .contains("Source 吞吐达成率")
        .contains("Source 积压")
        .contains("<tr><th>Source 积压</th><td>12 条 CHR</td></tr>")
        .contains("已发布拓扑记录")
        .contains("Checkpoint 间隔")
        .contains("<tr><th>目标 CHR EPS</th><td>30 条/小区/s</td></tr>")
        .contains("<tr><th>全局 CHR EPS</th><td>30000 条/s</td></tr>")
        .contains("<tr><th>目标 PM EPS</th><td>1 条/小区/s</td></tr>")
        .contains("<tr><th>全局 PM EPS</th><td>1000 条/s</td></tr>")
        .contains("<tr><th>Checkpoint 间隔</th><td>60000 ms</td></tr>")
        .contains("<tr><th>Checkpoint 耗时</th><td>10000 ms</td></tr>")
        .contains("算子流程与指标")
        .contains("累计延迟")
        .contains("算子延迟")
        .contains("延迟摘要")
        .contains("延迟探针")
        .contains("Probe类型")
        .contains("列说明")
        .contains("异常值分析")
        .contains("传输延迟不可用")
        .doesNotContain("Cumulative Business Age P95")
        .doesNotContain("Flink Marker P95")
        .contains("N/A")
        .contains("Flink 资源")
        .contains("operator-flow")
        .contains("flow-node")
        .contains("flow-edge")
        .contains("data-operator-id=\"source-1\"")
        .contains("operator-row-source-1")
        .contains("Sink 与存储")
        .contains("原始文件")
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
        .contains("<td>Watermark 滞后</td><td>181000 ms</td><td>&lt;= 180000 ms</td><td><span class=\"health health-warn\">UNSTABLE</span></td>");
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
                300, 100, 600, 200, 8_192, 2_048, 0.4, 0.6, 0.0),
            new FlinkOperatorSnapshot("sink-kpi-1m", "starrocks-kpi-1m -> Sink: cell-kpi-starrocks-connector-sink", 4,
                100, 100, 200, 200, 2_048, 2_048, 0.3, 0.7, 0.0)),
            List.of(new FlinkOperatorEdge("kpi-1m", "sink-kpi-1m"))),
        new FdbMetricsSnapshot(1, 2, 3, 35, 10, 33, 0, 5, List.of(
            new StageLatencySnapshot("kpi-1m", 10, 20, -1, 5)),
            List.of(
                new SinkLatencySnapshot("starrocks-kpi-1m", 200, 20_000, 15, 35, 40, 0),
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
        .contains("采样输出 EPS")
        .contains("<tr><th>采样平均输入 Records/s</th><td>300</td></tr>")
        .contains("<tr><th>算子聚合输入总记录</th><td>1200</td></tr>")
        .contains("<th>算子</th><th>角色</th><th>并行度</th>")
        .contains("<th>累计延迟</th><th>算子延迟</th><th>延迟摘要</th><th>延迟探针</th><th>Probe类型</th>")
        .contains("<td>kpi-1m-full-join</td><td>kpi</td><td>4</td><td>300</td><td>100</td>")
        .contains("<td>20 ms</td><td>20 ms</td><td>累计=20 ms | 算子=20 ms</td><td>stage:kpi-1m</td><td>输出累计</td>")
        .contains("<td>starrocks-kpi-1m -&gt; Sink: cell-kpi-starrocks-connector-sink</td><td>sink</td><td>4</td>")
        .contains("<td>35 ms</td><td>15 ms</td><td>累计=35 ms | 算子=15 ms | 写入=10 ms | 提交=33 ms</td><td>sink:starrocks-kpi-1m</td><td>Sink入口累计</td>")
        .contains("<tr><th>CFG 总量</th><td>1000</td><td>1000.00</td><td>1.00</td><td>N/A (一次性初始化)</td></tr>")
        .contains("累计延迟高但传输延迟正常")
        .contains("传输延迟高")
        .contains("Connector 写入 P95")
        .contains("Connector 提交 P95")
        .contains("<td>connector-commit</td><td>starrocks-kpi-1m.connector-commit</td>")
        .contains("延迟样本可信度")
        .contains("<tr><td>存储行数</td><td>storage</td><td>starrocks</td><td>12345</td><td>-</td><td colspan=\"5\">starrocks rows=12345</td></tr>")
        .doesNotContain("存储文件</td><td>12345");
  }

  @Test
  void operator_table_maps_anomaly_detector_latency_probes() throws Exception {
    BenchmarkConfig config = BenchmarkConfig.from("local", Map.of(
        "FDB_BENCHMARK_ID", "bench-anomaly-probes",
        "FDB_BENCHMARK_SINKS", "starrocks",
        "FDB_BENCHMARK_CELL_LEVELS", "1000"));
    BenchmarkRunPlan plan = BenchmarkMatrix.expand(config).get(0);
    BenchmarkRunResult result = new BenchmarkRunResult(plan, BenchmarkStatus.STABLE,
        "all thresholds healthy",
        new FlinkSnapshot("RUNNING", 0, 10_000, 0, 300, 300, 1_200, 1_200, 1, 4, List.of(
            new FlinkOperatorSnapshot("user-anomaly-op", "user-event-anomaly-detect-dedup -> user-anomaly-metrics", 4,
                100, 20, 1_000, 200, 2_048, 512, 0.1, 0.9, 0.0),
            new FlinkOperatorSnapshot("grid-anomaly-op", "coverage-hole-detector -> grid-anomaly-metrics", 4,
                100, 10, 1_000, 100, 2_048, 512, 0.1, 0.9, 0.0),
            new FlinkOperatorSnapshot("cell-anomaly-op", "cell-kpi-anomaly-detect-dedup -> cell-anomaly-metrics", 4,
                100, 5, 1_000, 50, 2_048, 512, 0.1, 0.9, 0.0))),
        new FdbMetricsSnapshot(1, 2, -1, -1, 0, 0, List.of(
            new StageLatencySnapshot("user-anomaly", 40, 70, 90, 0),
            new StageLatencySnapshot("grid-anomaly", 50, 80, 100, 0),
            new StageLatencySnapshot("cell-anomaly", 60, 90, 110, 0)), List.of()),
        new StorageSnapshot(true, "starrocks rows=350", 350, 0, 0),
        TopologyMetricsSnapshot.empty(),
        new SourceMetricsSnapshot(true, 30_000, 90_000, 30_000.0, 3_000, 0, 0,
            1_000, 3_000, 1_000.0, 3_000, 0, 0,
            1_000, 1_000, 0.0, 0, 0, 0));

    new HtmlReportWriter(tempDir).write(config, List.of(result));

    String html = Files.readString(tempDir.resolve("bench-anomaly-probes/runs/" + plan.runId() + "/report.html"));
    assertThat(html)
        .contains("<td>user-event-anomaly-detect-dedup -&gt; user-anomaly-metrics</td>")
        .contains("<td>70 ms</td><td>70 ms</td>")
        .contains("<td>stage:user-anomaly</td>")
        .contains("<td>coverage-hole-detector -&gt; grid-anomaly-metrics</td>")
        .contains("<td>80 ms</td><td>80 ms</td>")
        .contains("<td>stage:grid-anomaly</td>")
        .contains("<td>cell-kpi-anomaly-detect-dedup -&gt; cell-anomaly-metrics</td>")
        .contains("<td>90 ms</td><td>90 ms</td>")
        .contains("<td>stage:cell-anomaly</td>");
  }

  @Test
  void operator_table_maps_stage_latency_when_operator_name_groups_downstream_vertices() throws Exception {
    BenchmarkConfig config = BenchmarkConfig.from("local", Map.of(
        "FDB_BENCHMARK_ID", "bench-grouped-stage-probes",
        "FDB_BENCHMARK_SINKS", "starrocks",
        "FDB_BENCHMARK_CELL_LEVELS", "1000"));
    BenchmarkRunPlan plan = BenchmarkMatrix.expand(config).get(0);
    BenchmarkRunResult result = new BenchmarkRunResult(plan, BenchmarkStatus.STABLE,
        "all thresholds healthy",
        new FlinkSnapshot("RUNNING", 0, 10_000, 0, 300, 300, 1_200, 1_200, 1, 4, List.of(
            new FlinkOperatorSnapshot("enrichment-op",
                "enrichment -> (enrichment-metrics -> Flat Map -> user-event-anomaly-evaluations)", 4,
                100, 100, 1_000, 1_000, 2_048, 2_048, 0.1, 0.9, 0.0),
            new FlinkOperatorSnapshot("kpi5-op", "kpi-5m-rollup -> kpi-5m-metrics", 4,
                10, 10, 100, 100, 512, 512, 0.1, 0.9, 0.0))),
        new FdbMetricsSnapshot(1, 2, -1, -1, 0, 0, List.of(
            new StageLatencySnapshot("enrichment", 40, 70, 90, 0),
            new StageLatencySnapshot("kpi-5m", 120, 150, 160, 0)), List.of()),
        new StorageSnapshot(true, "starrocks rows=350", 350, 0, 0),
        TopologyMetricsSnapshot.empty(),
        new SourceMetricsSnapshot(true, 30_000, 90_000, 30_000.0, 3_000, 0, 0,
            1_000, 3_000, 1_000.0, 3_000, 0, 0,
            1_000, 1_000, 0.0, 0, 0, 0));

    new HtmlReportWriter(tempDir).write(config, List.of(result));

    String html = Files.readString(tempDir.resolve("bench-grouped-stage-probes/runs/" + plan.runId()
        + "/report.html"));
    assertThat(html)
        .contains("<td>enrichment -&gt; (enrichment-metrics -&gt; Flat Map -&gt; user-event-anomaly-evaluations)</td>")
        .contains("<td>70 ms</td><td>70 ms</td>")
        .contains("<td>stage:enrichment</td>")
        .contains("<td>kpi-5m-rollup -&gt; kpi-5m-metrics</td>")
        .contains("<td>150 ms</td><td>150 ms</td>")
        .contains("<td>stage:kpi-5m</td>");
  }

  @Test
  void kpi5_note_does_not_count_connector_commit_as_business_sink_output() throws Exception {
    BenchmarkConfig config = BenchmarkConfig.from("local", Map.of(
        "FDB_BENCHMARK_ID", "bench-kpi5-note",
        "FDB_BENCHMARK_SINKS", "starrocks",
        "FDB_BENCHMARK_CELL_LEVELS", "1000"));
    BenchmarkRunPlan plan = BenchmarkMatrix.expand(config).get(0);
    BenchmarkRunResult result = new BenchmarkRunResult(plan, BenchmarkStatus.STABLE,
        "all thresholds healthy",
        new FlinkSnapshot("RUNNING", 0, 10_000, 0, 300, 300, 1_200, 1_200, 1, 4, List.of(
            new FlinkOperatorSnapshot("kpi5-op", "kpi-5m-rollup -> kpi-5m-metrics", 4,
                10, 0, 8_000, 0, 512, 1, 0.1, 0.9, 0.0),
            new FlinkOperatorSnapshot("kpi5-sink", "starrocks-kpi-5m -> Sink: cell-kpi-5m-starrocks-connector-sink", 4,
                0, 0, 0, 0, 1, 0, 0.0, 1.0, 0.0))),
        new FdbMetricsSnapshot(1, 2, -1, -1, 0, 0, List.of(),
            List.of(new SinkLatencySnapshot("starrocks-kpi-5m.connector-commit", 144, 0, 0, 0, 0, 0))),
        new StorageSnapshot(true, "starrocks rows=350", 350, 0, 0),
        TopologyMetricsSnapshot.empty(),
        new SourceMetricsSnapshot(true, 30_000, 90_000, 30_000.0, 3_000, 0, 0,
            1_000, 3_000, 1_000.0, 3_000, 0, 0,
            1_000, 1_000, 0.0, 0, 0, 0));

    new HtmlReportWriter(tempDir).write(config, List.of(result));

    String html = Files.readString(tempDir.resolve("bench-kpi5-note/runs/" + plan.runId() + "/report.html"));
    assertThat(html)
        .contains("KPI 5m rollup")
        .doesNotContain("KPI 5m 已采到 sink");
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
        .contains("<tr><th>算子指标有效性</th><td>不可用")
        .contains("<td>Source: chr-source</td><td>source</td><td>4</td><td>N/A</td><td>N/A</td><td>N/A</td><td>N/A</td>")
        .contains("<td>延迟探针映射</td><td>0/1 算子</td><td>展示</td>")
        .doesNotContain("unmapped business probe")
        .doesNotContain("no probe")
        .doesNotContain("P95 N/A")
        .contains("输出/s=N/A")
        .contains("反压=N/A");
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
        .contains("指标覆盖")
        .contains("<td>Flink 算子指标</td><td>1/1 算子</td><td>展示</td>")
        .contains("<td>输入 Watermark</td><td>1/1 算子</td><td>展示</td>")
        .contains("<td>输出 Watermark</td><td>0/1 算子</td><td>隐藏")
        .contains("<td>延迟探针映射</td><td>1/1 算子</td><td>展示</td>")
        .doesNotContain("<td>Flink Marker P95</td>")
        .contains("<th>输入 Watermark</th>")
        .doesNotContain("<th>输入 Watermark</th><th>输出 Watermark</th><th>反压</th>")
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
        .contains("<tr><th>TaskManager CPU 负载</th><td>73.00%</td></tr>")
        .contains("<th>忙碌 ms/s</th>")
        .contains("<th>反压 ms/s</th>")
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
        .contains("延迟样本可信度")
        .contains("<td>低（少于 30 条）</td>");
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
        .contains("<text class=\"flow-latency\" x=\"10\" y=\"62\">累计 20 ms</text>")
        .contains("flow-node flow-node-ok")
        .contains("flow-node flow-node-na")
        .contains("data-latency-health=\"UNSTABLE\"");
  }

  @Test
  void operator_flow_shows_marker_latency_on_edges_only() throws Exception {
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
        .contains(">传输 230 ms</text>")
        .doesNotContain("marker: chr-source -> kpi-join")
        .doesNotContain("marker: pm-source -> kpi-join")
        .doesNotContain("<td>Flink marker</td><td>transport</td>")
        .doesNotContain("Flink Marker P95");
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
        .contains("1m 窗口诊断")
        .contains("<td>1m 窗口物化</td><td>CHR=0, PM=4, KPI=0 closed minutes</td><td>&gt;= 4 closed minutes</td><td><span class=\"health health-warn\">UNSTABLE</span></td>")
        .contains("<thead><tr><th>阶段</th><th>输入总记录</th><th>输出总记录</th><th>已关闭分钟窗口</th><th>预期关闭分钟窗口</th><th>健康状态</th></tr></thead>")
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

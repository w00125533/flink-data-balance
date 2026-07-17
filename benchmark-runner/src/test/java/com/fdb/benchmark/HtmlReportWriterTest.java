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
        "FDB_BENCHMARK_SINKS", "starrocks",
        "FDB_BENCHMARK_CELL_LEVELS", "1000"));
    BenchmarkRunResult result = BenchmarkResultWriterTest.sampleResult(config);

    new HtmlReportWriter(tempDir).write(config, List.of(result));

    Path index = tempDir.resolve("bench-a/index.html");
    Path report = tempDir.resolve("bench-a/runs/" + result.plan().runId() + "/report.html");
    assertThat(index).exists();
    assertThat(report).exists();
    String html = Files.readString(index);
    assertThat(html).contains("Sink Upper-Bound Benchmark");
    assertThat(html).contains("Total Runs");
    assertThat(html).contains("Stable Runs");
    assertThat(html).contains("Unstable/Failed");
    assertThat(html).contains("Best Stable Bound");
    assertThat(html).contains("Stable upper bounds");
    assertThat(html).contains("Checkpoint ms");
    assertThat(html).contains("Backpressure");
    assertThat(html).contains("Watermark Lag");
    assertThat(html).contains("Storage");
    assertThat(html).contains("runs/" + result.plan().runId() + "/report.html");
    assertThat(html).contains("Recommendations");
    assertThat(Files.readString(report))
        .contains("Run Summary")
        .contains("Run Notes")
        .contains("Flink Resources")
        .contains("Operator Flow")
        .contains("operator-flow")
        .contains("data-operator-id=\"v1\"")
        .contains("data-source-id=\"v1\" data-target-id=\"v2\"")
        .contains("highlightOperator")
        .doesNotContain("scrollIntoView")
        .contains("Operator Throughput")
        .contains("Records In Total")
        .contains("Records Out Total")
        .contains("Topology Generation")
        .contains("Latency")
        .contains("Sink &amp; Storage")
        .contains("Raw Artifacts")
        .contains("run.json")
        .contains("flink-snapshot.json")
        .contains("fdb-metrics-snapshot.json")
        .contains("storage-snapshot.json");
    assertThat(Files.readString(report))
        .contains("Generated Records")
        .contains("6000")
        .contains("Topology Generation Duration")
        .contains("Kafka Publish Duration")
        .contains("Publish Failures")
        .contains("topology-metrics.json");
    assertThat(Files.readString(report))
        .contains("sink-starrocks")
        .contains("20000")
        .contains("19000")
        .contains("8192")
        .contains("4096");
    assertThat(Files.readString(report))
        .contains("kpi-5m")
        .contains("starrocks-kpi-1m")
        .contains("metrics-output")
        .contains("enrichment-late-sink")
        .doesNotContain("iceberg-kpi-1m")
        .contains("12000 ms");
  }
}

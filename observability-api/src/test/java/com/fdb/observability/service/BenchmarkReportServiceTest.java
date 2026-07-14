package com.fdb.observability.service;

import static org.assertj.core.api.Assertions.assertThat;

import com.fdb.common.metrics.StageMetricSample;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class BenchmarkReportServiceTest {
  @TempDir
  Path tempDir;

  @Test
  void generatesReportFromSinkLatencySamples() throws Exception {
    MetricsHistoryService history = new MetricsHistoryService(tempDir, true);
    history.append(StageMetricSample.sinkLatency(
        "starrocks-kpi-1m", "Cell KPI 1m StarRocks Sink", "healthy", "starrocks", "kpi_1m", "MIN_1",
        123, 4_096, 250, 20, 45, 80, 0, "", 7, 1_717_400_000_000L)
        .withRunMetadata("run-a", "starrocks", 4));

    Path report = new BenchmarkReportService(tempDir).generate("run-a");

    assertThat(report).exists().hasFileName("report.md");
    String markdown = Files.readString(report);
    assertThat(markdown).contains("# Benchmark Report: run-a");
    assertThat(markdown).contains("starrocks-kpi-1m");
    assertThat(markdown).contains("123");
    assertThat(markdown).contains("Result sink: starrocks");
    assertThat(markdown).contains("Parallelism: 4");
  }

  @Test
  void generatesClearReportForEmptyRun() throws Exception {
    Path report = new BenchmarkReportService(tempDir).generate("empty-run");

    assertThat(report).exists();
    assertThat(Files.readString(report))
        .contains("# Benchmark Report: empty-run")
        .contains("Samples: 0");
  }

  @Test
  void countsInvalidJsonLinesAndStillGeneratesReport() throws Exception {
    Path runDir = tempDir.resolve("run-a");
    Files.createDirectories(runDir);
    Files.writeString(runDir.resolve("metrics.jsonl"), "not-json\n"
        + StageMetricSample.sinkLatency(
            "starrocks-kpi-1m", "Cell KPI 1m StarRocks Sink", "healthy", "starrocks", "kpi_1m", "MIN_1",
            3, 100, 20, 10, 20, 30, 0, "", 1, 1_717_400_000_000L)
            .withRunMetadata("run-a", "starrocks", 4).toJson()
        + "\n");

    Path report = new BenchmarkReportService(tempDir).generate("run-a");

    assertThat(Files.readString(report))
        .contains("Samples: 1")
        .contains("Invalid sample lines: 1");
  }

  @Test
  void truncatesReportInputAtConfiguredSampleLimit() throws Exception {
    Path runDir = tempDir.resolve("run-a");
    Files.createDirectories(runDir);
    Files.writeString(runDir.resolve("metrics.jsonl"), String.join("\n",
        StageMetricSample.sinkLatency(
            "starrocks-kpi-1m", "Cell KPI 1m StarRocks Sink", "healthy", "starrocks", "kpi_1m", "MIN_1",
            1, 100, 20, 10, 20, 30, 0, "", 1, 1_717_400_000_000L)
            .withRunMetadata("run-a", "starrocks", 4).toJson(),
        StageMetricSample.sinkLatency(
            "starrocks-kpi-5m", "Cell KPI 5m StarRocks Sink", "healthy", "starrocks", "kpi_5m", "MIN_5",
            2, 100, 20, 10, 20, 30, 0, "", 1, 1_717_400_001_000L)
            .withRunMetadata("run-a", "starrocks", 4).toJson(),
        StageMetricSample.sinkLatency(
            "starrocks-cell-anomaly", "Cell Anomaly StarRocks Sink", "healthy", "starrocks",
            "cell_anomaly_events", "ANOMALY",
            3, 100, 20, 10, 20, 30, 0, "", 1, 1_717_400_002_000L)
            .withRunMetadata("run-a", "starrocks", 4).toJson()));

    Path report = new BenchmarkReportService(tempDir, 2).generate("run-a");

    assertThat(Files.readString(report))
        .contains("Samples: 2")
        .contains("Samples truncated: true")
        .doesNotContain("starrocks-cell-anomaly");
  }

  @Test
  void sinkMetricsUseLatestSamplePerStageAndWindow() throws Exception {
    Path runDir = tempDir.resolve("run-a");
    Files.createDirectories(runDir);
    Files.writeString(runDir.resolve("metrics.jsonl"), String.join("\n",
        StageMetricSample.sinkLatency(
            "starrocks-kpi-1m", "Cell KPI 1m StarRocks Sink", "healthy", "starrocks", "kpi_1m", "MIN_1",
            1, 100, 20, 10, 20, 30, 0, "", 1, 1_717_400_000_000L)
            .withRunMetadata("run-a", "starrocks", 4).toJson(),
        StageMetricSample.sinkLatency(
            "starrocks-kpi-1m", "Cell KPI 1m StarRocks Sink", "healthy", "starrocks", "kpi_1m", "MIN_1",
            9, 900, 90, 40, 80, 100, 0, "", 2, 1_717_400_001_000L)
            .withRunMetadata("run-a", "starrocks", 4).toJson()));

    String markdown = Files.readString(new BenchmarkReportService(tempDir).generate("run-a"));

    assertThat(markdown).contains("| starrocks-kpi-1m | starrocks | 1m | 9 |");
    assertThat(markdown).doesNotContain("| starrocks-kpi-1m | starrocks | 1m | 1 |");
  }
}

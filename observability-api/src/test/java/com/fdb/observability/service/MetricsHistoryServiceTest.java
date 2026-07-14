package com.fdb.observability.service;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fdb.common.metrics.StageMetricSample;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class MetricsHistoryServiceTest {
  private static final ObjectMapper JSON = new ObjectMapper();

  @TempDir
  Path tempDir;

  @Test
  void appendsSampleAndWritesRunMetadataWhenEnabled() throws Exception {
    MetricsHistoryService service = new MetricsHistoryService(tempDir, true);
    StageMetricSample sample = StageMetricSample.stage("chr-source", "CHR Source", "healthy",
        1.0, 2.0, 10, 20, 0, 1_717_400_000_000L)
        .withRunMetadata("run-a", "starrocks", 4);

    service.append(sample);

    Path metricsFile = tempDir.resolve("run-a").resolve("metrics.jsonl");
    assertThat(metricsFile).exists();
    List<String> lines = Files.readAllLines(metricsFile);
    assertThat(lines).hasSize(1);
    assertThat(JSON.readTree(lines.get(0)).get("stageId").asText()).isEqualTo("chr-source");

    JsonNode run = JSON.readTree(tempDir.resolve("run-a").resolve("run.json").toFile());
    assertThat(run.get("runId").asText()).isEqualTo("run-a");
    assertThat(run.get("resultSink").asText()).isEqualTo("starrocks");
    assertThat(run.get("parallelism").asInt()).isEqualTo(4);
  }

  @Test
  void doesNotWriteFilesWhenDisabled() {
    MetricsHistoryService service = new MetricsHistoryService(tempDir, false);
    StageMetricSample sample = StageMetricSample.stage("chr-source", "CHR Source", "healthy",
        1.0, 2.0, 10, 20, 0, 1_717_400_000_000L)
        .withRunMetadata("run-a", "starrocks", 4);

    service.append(sample);

    assertThat(tempDir).isEmptyDirectory();
  }

  @Test
  void sanitizesRunIdBeforeUsingItAsDirectoryName() {
    MetricsHistoryService service = new MetricsHistoryService(tempDir, true);
    StageMetricSample sample = StageMetricSample.stage("chr-source", "CHR Source", "healthy",
        1.0, 2.0, 10, 20, 0, 1_717_400_000_000L)
        .withRunMetadata("run/../a", "starrocks", 4);

    service.append(sample);

    assertThat(tempDir.resolve("run_.._a").resolve("metrics.jsonl")).exists();
    assertThat(tempDir.resolve("a")).doesNotExist();
  }
}

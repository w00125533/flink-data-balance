package com.fdb.observability.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fdb.common.metrics.StageMetricSample;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;

public final class MetricsHistoryService implements MetricHistoryAppender {
  private static final ObjectMapper JSON = new ObjectMapper();
  private static final String UNKNOWN_RUN = "unknown-run";

  private final Path runsRoot;
  private final boolean enabled;

  public MetricsHistoryService(Path runsRoot, boolean enabled) {
    this.runsRoot = runsRoot;
    this.enabled = enabled;
  }

  @Override
  public void append(StageMetricSample sample) {
    if (!enabled) {
      return;
    }
    try {
      Path runDir = runsRoot.resolve(sanitizeRunId(sample.runId())).normalize();
      if (!runDir.startsWith(runsRoot.normalize())) {
        throw new IOException("Resolved run directory escaped runs root");
      }
      Files.createDirectories(runDir);
      writeRunMetadata(runDir, sample);
      Files.writeString(runDir.resolve("metrics.jsonl"), sample.toJson() + System.lineSeparator(),
          StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static String sanitizeRunId(String runId) {
    if (runId == null || runId.isBlank()) {
      return UNKNOWN_RUN;
    }
    String sanitized = runId.trim().replaceAll("[^A-Za-z0-9._-]", "_");
    return sanitized.isBlank() ? UNKNOWN_RUN : sanitized;
  }

  private static void writeRunMetadata(Path runDir, StageMetricSample sample) throws IOException {
    Path runFile = runDir.resolve("run.json");
    if (Files.exists(runFile)) {
      return;
    }
    Map<String, Object> metadata = Map.of(
        "runId", sample.runId(),
        "resultSink", sample.resultSink(),
        "parallelism", sample.parallelism());
    JSON.writeValue(runFile.toFile(), metadata);
  }
}

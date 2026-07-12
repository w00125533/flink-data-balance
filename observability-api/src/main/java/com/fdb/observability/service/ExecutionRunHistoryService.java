package com.fdb.observability.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fdb.observability.model.ExecutionMetric;
import com.fdb.observability.model.ExecutionRunDetail;
import com.fdb.observability.model.ExecutionRunSummary;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public final class ExecutionRunHistoryService {
  private static final ObjectMapper JSON = new ObjectMapper();
  private static final Pattern SUMMARY_LINE = Pattern.compile("^\\[summary\\]\\s+(.+?)\\s+\\|\\s+(.+?)\\s+\\|\\s+(.*)$");
  private final Path runsRoot;

  public ExecutionRunHistoryService(Path runsRoot) {
    this.runsRoot = runsRoot;
  }

  public List<ExecutionRunSummary> listRuns() {
    if (!Files.isDirectory(runsRoot)) {
      return List.of();
    }
    try (Stream<Path> stream = Files.list(runsRoot)) {
      return stream
          .filter(Files::isDirectory)
          .map(this::readSummary)
          .flatMap(Optional::stream)
          .sorted(Comparator.comparing(ExecutionRunSummary::startedAt,
              Comparator.nullsLast(Comparator.reverseOrder())))
          .toList();
    } catch (IOException ignored) {
      return List.of();
    }
  }

  public Optional<ExecutionRunDetail> runDetail(String runId) {
    if (!isSafeRunId(runId)) {
      return Optional.empty();
    }
    Path runDir = runsRoot.resolve(runId).normalize();
    if (!runDir.startsWith(runsRoot.normalize()) || !Files.isDirectory(runDir)) {
      return Optional.empty();
    }

    RunMeta meta = readMeta(runDir, runId);
    List<String> rawLines = readSummaryLines(runDir.resolve(meta.summaryFile()));
    List<ExecutionMetric> metrics = parseMetrics(rawLines);
    return Optional.of(new ExecutionRunDetail(
        meta.runId(),
        meta.status(),
        meta.startedAt(),
        meta.completedAt(),
        metrics,
        rawLines));
  }

  private Optional<ExecutionRunSummary> readSummary(Path runDir) {
    RunMeta meta = readMeta(runDir, runDir.getFileName().toString());
    List<ExecutionMetric> metrics = parseMetrics(readSummaryLines(runDir.resolve(meta.summaryFile())));
    String summary = metrics.stream()
        .filter(metric -> "Execution".equals(metric.section()) && "status".equals(metric.metric()))
        .map(ExecutionMetric::value)
        .findFirst()
        .orElseGet(() -> metrics.stream()
            .filter(metric -> "StarRocks KPI".equals(metric.section()) || "Hive KPI".equals(metric.section()))
            .findFirst()
            .map(metric -> metric.metric() + "=" + metric.value())
            .orElse("no summary"));
    return Optional.of(new ExecutionRunSummary(
        meta.runId(),
        meta.status(),
        meta.startedAt(),
        meta.completedAt(),
        metrics.size(),
        summary));
  }

  private RunMeta readMeta(Path runDir, String fallbackRunId) {
    Path metaFile = runDir.resolve("meta.json");
    if (!Files.isRegularFile(metaFile)) {
      return new RunMeta(fallbackRunId, "unknown", null, null, "logs-summary.log");
    }
    try {
      JsonNode node = JSON.readTree(Files.readString(metaFile, StandardCharsets.UTF_8));
      return new RunMeta(
          text(node, "runId", fallbackRunId),
          text(node, "status", "unknown"),
          text(node, "startedAt", null),
          text(node, "completedAt", null),
          text(node, "summaryFile", "logs-summary.log"));
    } catch (IOException ignored) {
      return new RunMeta(fallbackRunId, "unknown", null, null, "logs-summary.log");
    }
  }

  private List<String> readSummaryLines(Path file) {
    if (!Files.isRegularFile(file)) {
      return List.of();
    }
    try {
      return Files.readAllLines(file, StandardCharsets.UTF_8);
    } catch (IOException ignored) {
      return List.of();
    }
  }

  private List<ExecutionMetric> parseMetrics(List<String> lines) {
    List<ExecutionMetric> metrics = new ArrayList<>();
    for (String line : lines) {
      Matcher matcher = SUMMARY_LINE.matcher(line);
      if (matcher.matches() && !matcher.group(1).startsWith("===")) {
        metrics.add(new ExecutionMetric(matcher.group(1), matcher.group(2), matcher.group(3)));
      }
    }
    return metrics;
  }

  private static String text(JsonNode node, String field, String fallback) {
    JsonNode value = node.get(field);
    if (value == null || value.isNull()) {
      return fallback;
    }
    return value.asText();
  }

  private static boolean isSafeRunId(String runId) {
    return runId != null && runId.matches("[A-Za-z0-9._-]+");
  }

  private record RunMeta(String runId, String status, String startedAt, String completedAt, String summaryFile) {
  }
}

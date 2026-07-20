package com.fdb.observability.service;

import com.fdb.common.metrics.StageMetricSample;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.function.ToLongFunction;
import java.util.stream.Stream;

public final class BenchmarkReportService {
  static final int MAX_REPORT_SAMPLES = 100_000;

  private final Path runsRoot;
  private final int maxReportSamples;

  public BenchmarkReportService(Path runsRoot) {
    this(runsRoot, MAX_REPORT_SAMPLES);
  }

  BenchmarkReportService(Path runsRoot, int maxReportSamples) {
    this.runsRoot = runsRoot;
    this.maxReportSamples = maxReportSamples;
  }

  public Path generate(String runId) {
    String safeRunId = MetricsHistoryService.sanitizeRunId(runId);
    Path runDir = runsRoot.resolve(safeRunId).normalize();
    if (!runDir.startsWith(runsRoot.normalize())) {
      throw new IllegalArgumentException("Unsafe runId");
    }
    try {
      Files.createDirectories(runDir);
      ReadResult result = readSamples(runDir.resolve("metrics.jsonl"), maxReportSamples);
      Path report = runDir.resolve("report.md");
      Files.writeString(report, render(runId == null || runId.isBlank() ? safeRunId : runId, result),
          StandardCharsets.UTF_8);
      return report;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static ReadResult readSamples(Path metricsFile, int maxSamples) throws IOException {
    if (!Files.isRegularFile(metricsFile)) {
      return new ReadResult(List.of(), 0, false);
    }
    List<StageMetricSample> samples = new ArrayList<>();
    int invalidLines = 0;
    boolean truncated = false;
    try (Stream<String> lines = Files.lines(metricsFile, StandardCharsets.UTF_8)) {
      var iterator = lines.iterator();
      while (iterator.hasNext()) {
        String line = iterator.next();
        if (line.isBlank()) {
          continue;
        }
        try {
          samples.add(StageMetricSample.fromJson(line));
          if (samples.size() >= maxSamples) {
            truncated = iterator.hasNext();
            break;
          }
        } catch (RuntimeException e) {
          invalidLines++;
        }
      }
    }
    return new ReadResult(samples, invalidLines, truncated);
  }

  private static String render(String runId, ReadResult result) {
    List<StageMetricSample> samples = result.samples();
    String resultSink = samples.stream()
        .map(StageMetricSample::resultSink)
        .filter(value -> !value.isBlank())
        .findFirst()
        .orElse("unknown");
    String parallelism = samples.stream()
        .map(StageMetricSample::parallelism)
        .filter(value -> value > 0)
        .findFirst()
        .map(String::valueOf)
        .orElse("unknown");

    StringBuilder out = new StringBuilder();
    out.append("# Benchmark Report: ").append(runId).append("\n\n");
    out.append("## Summary\n\n");
    out.append("- Samples: ").append(samples.size()).append('\n');
    out.append("- Samples truncated: ").append(result.truncated()).append('\n');
    out.append("- Invalid sample lines: ").append(result.invalidLines()).append('\n');
    out.append("- Result sink: ").append(resultSink).append('\n');
    out.append("- Parallelism: ").append(parallelism).append("\n\n");
    appendStageLatencyMetrics(out, samples);
    appendSinkMetrics(out, samples);
    appendBottleneckCandidates(out, samples);
    return out.toString();
  }

  private static void appendStageLatencyMetrics(StringBuilder out, List<StageMetricSample> samples) {
    out.append("## Stage Latency Metrics\n\n");
    List<StageMetricSample> stageSamples = aggregateLatestByStage(samples).values().stream()
        .filter(sample -> sample.sink().isBlank())
        .filter(sample -> sample.latencyP50Ms() > 0L
            || sample.latencyP95Ms() > 0L
            || sample.latencyP99Ms() > 0L
            || sample.watermarkLagMs() > 0L)
        .sorted(Comparator.comparing(StageMetricSample::stageId))
        .toList();
    if (stageSamples.isEmpty()) {
      out.append("No stage latency metrics were collected.\n\n");
      return;
    }
    out.append("| Stage | Display | EPS | P50 ms | P95 ms | P99 ms | Watermark lag ms |\n");
    out.append("| --- | --- | ---: | ---: | ---: | ---: | ---: |\n");
    for (StageMetricSample sample : stageSamples) {
      out.append("| ").append(sample.stageId())
          .append(" | ").append(sample.displayName())
          .append(" | ").append(String.format(Locale.ROOT, "%.2f", sample.outEps()))
          .append(" | ").append(formatLatencyMs(sample.latencyP50Ms()))
          .append(" | ").append(formatLatencyMs(sample.latencyP95Ms()))
          .append(" | ").append(formatLatencyMs(sample.latencyP99Ms()))
          .append(" | ").append(sample.watermarkLagMs())
          .append(" |\n");
    }
    out.append('\n');
  }

  private static void appendSinkMetrics(StringBuilder out, List<StageMetricSample> samples) {
    out.append("## Sink Metrics\n\n");
    List<StageMetricSample> sinkSamples = aggregateLatestSinkSamples(samples).values().stream()
        .filter(sample -> !sample.sinkType().isBlank() || !sample.sink().isBlank())
        .sorted(Comparator.comparing(StageMetricSample::stageId)
            .thenComparing(StageMetricSample::window))
        .toList();
    if (sinkSamples.isEmpty()) {
      out.append("No sink metrics were collected.\n\n");
      return;
    }
    out.append("| Stage | Sink | Window | Records | P50 ms | P95 ms | P99 ms | Failures |\n");
    out.append("| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |\n");
    for (StageMetricSample sample : sinkSamples) {
      out.append("| ").append(sample.stageId())
          .append(" | ").append(displaySink(sample))
          .append(" | ").append(sample.window())
          .append(" | ").append(sample.records())
          .append(" | ").append(formatLatencyMs(sample.latencyP50Ms()))
          .append(" | ").append(formatLatencyMs(sample.latencyP95Ms()))
          .append(" | ").append(formatLatencyMs(sample.latencyP99Ms()))
          .append(" | ").append(sample.failureCount())
          .append(" |\n");
    }
    out.append('\n');
  }

  private static void appendBottleneckCandidates(StringBuilder out, List<StageMetricSample> samples) {
    out.append("## Bottleneck Candidates\n\n");
    List<StageMetricSample> candidates = aggregateLatestByMetric(samples).values().stream()
        .filter(sample -> sample.latencyP95Ms() > 0 || sample.failureCount() > 0)
        .sorted(Comparator.comparingLong(StageMetricSample::latencyP95Ms).reversed())
        .limit(5)
        .toList();
    if (candidates.isEmpty()) {
      out.append("No bottleneck candidates were detected from collected samples.\n");
      return;
    }
    for (StageMetricSample candidate : candidates) {
      out.append("- ").append(candidate.stageId())
          .append(": p95=").append(candidate.latencyP95Ms()).append(" ms")
          .append(", failures=").append(candidate.failureCount()).append('\n');
    }
  }

  private static String displaySink(StageMetricSample sample) {
    return sample.sinkType().isBlank() ? sample.sink() : sample.sinkType();
  }

  private static Map<String, StageMetricSample> aggregateLatestSinkSamples(List<StageMetricSample> samples) {
    return aggregateLatestBy(samples,
        sample -> sample.stageId() + ":" + sample.sink() + ":" + sample.window(),
        sample -> !sample.sink().isBlank());
  }

  private static Map<String, StageMetricSample> aggregateLatestByStage(List<StageMetricSample> samples) {
    return aggregateLatestBy(samples, StageMetricSample::stageId, sample -> true);
  }

  private static Map<String, StageMetricSample> aggregateLatestByMetric(List<StageMetricSample> samples) {
    return aggregateLatestBy(samples,
        sample -> sample.sink().isBlank()
            ? sample.stageId()
            : sample.stageId() + ":" + sample.sink() + ":" + sample.window(),
        sample -> true);
  }

  private static Map<String, StageMetricSample> aggregateLatestBy(
      List<StageMetricSample> samples,
      Function<StageMetricSample, String> logicalKey,
      java.util.function.Predicate<StageMetricSample> filter) {
    Map<String, StageMetricSample> latestPhysicalSamples = new LinkedHashMap<>();
    samples.stream()
        .filter(filter)
        .sorted(Comparator.comparingLong(StageMetricSample::updatedAtEpochMs))
        .forEach(sample -> latestPhysicalSamples.put(physicalKey(logicalKey.apply(sample), sample), sample));

    Map<String, List<StageMetricSample>> grouped = new LinkedHashMap<>();
    latestPhysicalSamples.values().forEach(sample ->
        grouped.computeIfAbsent(logicalKey.apply(sample), ignored -> new ArrayList<>()).add(sample));

    Map<String, StageMetricSample> aggregated = new LinkedHashMap<>();
    grouped.forEach((key, group) -> aggregated.put(key, aggregateSamples(group)));
    return aggregated;
  }

  private static String physicalKey(String logicalKey, StageMetricSample sample) {
    return sample.subtaskIndex() >= 0 ? logicalKey + ":subtask-" + sample.subtaskIndex() : logicalKey;
  }

  private static StageMetricSample aggregateSamples(List<StageMetricSample> group) {
    List<StageMetricSample> effective = effectiveSamples(group);
    StageMetricSample latest = effective.stream()
        .max(Comparator.comparingLong(StageMetricSample::updatedAtEpochMs))
        .orElse(group.get(0));
    return new StageMetricSample(
        latest.stageId(),
        latest.displayName(),
        combinedStatus(effective),
        effective.stream().mapToDouble(StageMetricSample::inEps).sum(),
        effective.stream().mapToDouble(StageMetricSample::outEps).sum(),
        maxAvailable(effective, StageMetricSample::latencyP95Ms),
        effective.stream().mapToLong(StageMetricSample::watermarkLagMs).max().orElse(0L),
        effective.stream().mapToLong(StageMetricSample::errorCount).sum(),
        effective.stream().mapToLong(StageMetricSample::rowsWritten).sum(),
        effective.stream().mapToLong(StageMetricSample::rebalanceTotal).sum(),
        latest.source(),
        latest.sink(),
        latest.window(),
        latest.sinkType(),
        latest.dataset(),
        latest.windowKind(),
        effective.stream().mapToLong(StageMetricSample::records).sum(),
        effective.stream().mapToLong(StageMetricSample::bytes).sum(),
        effective.stream().mapToLong(StageMetricSample::durationMs).max().orElse(0L),
        maxAvailable(effective, StageMetricSample::latencyP50Ms),
        maxAvailable(effective, StageMetricSample::latencyP99Ms),
        effective.stream().mapToLong(StageMetricSample::failureCount).sum(),
        latestNonBlankError(effective),
        effective.stream().mapToLong(StageMetricSample::checkpointId).max().orElse(-1L),
        latest.runId(),
        latest.resultSink(),
        effective.stream().mapToInt(StageMetricSample::parallelism).max().orElse(-1),
        -1,
        effective.stream().mapToLong(StageMetricSample::updatedAtEpochMs).max().orElse(latest.updatedAtEpochMs()));
  }

  private static List<StageMetricSample> effectiveSamples(List<StageMetricSample> group) {
    List<StageMetricSample> nonSeed = group.stream()
        .filter(sample -> !"unknown".equals(sample.status()))
        .toList();
    return nonSeed.isEmpty() ? group : nonSeed;
  }

  private static String combinedStatus(List<StageMetricSample> group) {
    if (group.stream().anyMatch(sample -> "failed".equalsIgnoreCase(sample.status()))) {
      return "failed";
    }
    if (group.stream().anyMatch(sample -> "critical".equalsIgnoreCase(sample.status()))) {
      return "critical";
    }
    if (group.stream().anyMatch(sample -> "warning".equalsIgnoreCase(sample.status())
        || "degraded".equalsIgnoreCase(sample.status())
        || "unhealthy".equalsIgnoreCase(sample.status()))) {
      return "warning";
    }
    if (group.stream().anyMatch(sample -> "healthy".equalsIgnoreCase(sample.status()))) {
      return "healthy";
    }
    return group.stream()
        .max(Comparator.comparingLong(StageMetricSample::updatedAtEpochMs))
        .map(StageMetricSample::status)
        .orElse("unknown");
  }

  private static long maxAvailable(List<StageMetricSample> group, ToLongFunction<StageMetricSample> value) {
    return group.stream()
        .mapToLong(value)
        .filter(candidate -> candidate >= 0L)
        .max()
        .orElse(-1L);
  }

  private static String latestNonBlankError(List<StageMetricSample> group) {
    return group.stream()
        .filter(sample -> !sample.errorMessage().isBlank())
        .max(Comparator.comparingLong(StageMetricSample::updatedAtEpochMs))
        .map(StageMetricSample::errorMessage)
        .orElse("");
  }

  private static String formatLatencyMs(long value) {
    return value < 0 ? "N/A" : String.valueOf(value);
  }

  private record ReadResult(List<StageMetricSample> samples, int invalidLines, boolean truncated) {
  }
}

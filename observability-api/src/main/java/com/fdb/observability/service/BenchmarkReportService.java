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
    List<StageMetricSample> stageSamples = latestByStage(samples).values().stream()
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
          .append(" | ").append(sample.latencyP50Ms())
          .append(" | ").append(sample.latencyP95Ms())
          .append(" | ").append(sample.latencyP99Ms())
          .append(" | ").append(sample.watermarkLagMs())
          .append(" |\n");
    }
    out.append('\n');
  }

  private static void appendSinkMetrics(StringBuilder out, List<StageMetricSample> samples) {
    out.append("## Sink Metrics\n\n");
    List<StageMetricSample> sinkSamples = latestSinkSamples(samples).values().stream()
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
          .append(" | ").append(sample.latencyP50Ms())
          .append(" | ").append(sample.latencyP95Ms())
          .append(" | ").append(sample.latencyP99Ms())
          .append(" | ").append(sample.failureCount())
          .append(" |\n");
    }
    out.append('\n');
  }

  private static void appendBottleneckCandidates(StringBuilder out, List<StageMetricSample> samples) {
    out.append("## Bottleneck Candidates\n\n");
    Map<String, StageMetricSample> latestByStage = new LinkedHashMap<>();
    samples.stream()
        .sorted(Comparator.comparingLong(StageMetricSample::updatedAtEpochMs))
        .forEach(sample -> latestByStage.put(sample.stageId(), sample));
    List<StageMetricSample> candidates = latestByStage.values().stream()
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

  private static Map<String, StageMetricSample> latestSinkSamples(List<StageMetricSample> samples) {
    Map<String, StageMetricSample> latest = new LinkedHashMap<>();
    samples.stream()
        .sorted(Comparator.comparingLong(StageMetricSample::updatedAtEpochMs))
        .forEach(sample -> latest.put(sample.stageId() + ":" + sample.sink() + ":" + sample.window(), sample));
    return latest;
  }

  private static Map<String, StageMetricSample> latestByStage(List<StageMetricSample> samples) {
    Map<String, StageMetricSample> latest = new LinkedHashMap<>();
    samples.stream()
        .sorted(Comparator.comparingLong(StageMetricSample::updatedAtEpochMs))
        .forEach(sample -> latest.put(sample.stageId(), sample));
    return latest;
  }

  private record ReadResult(List<StageMetricSample> samples, int invalidLines, boolean truncated) {
  }
}

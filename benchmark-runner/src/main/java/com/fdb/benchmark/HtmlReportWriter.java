package com.fdb.benchmark;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public final class HtmlReportWriter {
  private final Path outputRoot;

  public HtmlReportWriter(Path outputRoot) {
    this.outputRoot = outputRoot;
  }

  public Path write(BenchmarkConfig config, List<BenchmarkRunResult> results) throws IOException {
    Path benchmarkDir = outputRoot.resolve(config.benchmarkId());
    Files.createDirectories(benchmarkDir);
    for (BenchmarkRunResult result : results) {
      Path report = benchmarkDir.resolve("runs").resolve(result.plan().runId()).resolve("report.html");
      Files.createDirectories(report.getParent());
      Files.writeString(report, runReport(config, result));
    }
    Files.writeString(benchmarkDir.resolve("index.html"), index(config, results));
    return benchmarkDir.resolve("index.html");
  }

  private static String index(BenchmarkConfig config, List<BenchmarkRunResult> results) {
    StringBuilder rows = new StringBuilder();
    for (BenchmarkRunResult result : results) {
      BenchmarkRunPlan plan = result.plan();
      rows.append("<tr>")
          .append("<td>").append(escape(plan.sink().value())).append("</td>")
          .append("<td>").append(plan.cellLevel()).append("</td>")
          .append("<td>").append(plan.targetChrEps()).append("</td>")
          .append("<td><span class=\"status ").append(statusClass(result.status())).append("\">")
          .append(result.status()).append("</span></td>")
          .append("<td>").append(result.flink().recordsOutPerSec()).append("</td>")
          .append("<td>").append(result.fdb().kpi1mP95Ms()).append("</td>")
          .append("<td>").append(result.fdb().sinkP95Ms()).append("</td>")
          .append("<td>").append(escape(result.bottleneckReason())).append("</td>")
          .append("<td><a href=\"runs/").append(escape(plan.runId()))
          .append("/report.html\">report</a></td>")
          .append("</tr>\n");
    }

    return """
        <!doctype html>
        <html lang="en">
        <head>
          <meta charset="utf-8">
          <meta name="viewport" content="width=device-width, initial-scale=1">
          <title>Sink Upper-Bound Benchmark</title>
          <style>
            body { margin: 0; font-family: Arial, sans-serif; color: #1f2937; background: #f7f8fa; }
            main { max-width: 1180px; margin: 0 auto; padding: 32px 24px; }
            h1 { font-size: 28px; margin: 0 0 8px; }
            h2 { font-size: 18px; margin: 28px 0 12px; }
            .meta { color: #667085; margin-bottom: 24px; }
            table { width: 100%%; border-collapse: collapse; background: #fff; border: 1px solid #d9dee7; }
            th, td { padding: 10px 12px; border-bottom: 1px solid #e6eaf0; text-align: left; font-size: 14px; }
            th { background: #eef2f6; font-weight: 600; }
            .status { display: inline-block; padding: 3px 8px; border-radius: 4px; font-size: 12px; font-weight: 700; }
            .stable { background: #dff5e5; color: #186238; }
            .unstable { background: #fff1d6; color: #875400; }
            .failed { background: #fde2e1; color: #b42318; }
            .recommendations { background: #fff; border: 1px solid #d9dee7; padding: 14px 16px; }
            a { color: #1d4ed8; }
          </style>
        </head>
        <body>
          <main>
            <h1>Sink Upper-Bound Benchmark</h1>
            <div class="meta">Benchmark: %s | Target: %s | Warmup: %ss | Duration: %ss</div>
            <h2>Stable upper bounds</h2>
            %s
            <h2>Runs</h2>
            <table>
              <thead>
                <tr><th>Sink</th><th>Cells</th><th>CHR EPS</th><th>Status</th><th>Out EPS</th><th>KPI 1m P95 ms</th><th>Sink P95 ms</th><th>Reason</th><th>Report</th></tr>
              </thead>
              <tbody>
        """.formatted(escape(config.benchmarkId()), escape(config.target()), config.warmupSec(), config.durationSec(),
            stableBounds(results))
        + rows
        + """
              </tbody>
            </table>
            <h2>Recommendations</h2>
            <div class="recommendations">
              %s
            </div>
          </main>
        </body>
        </html>
        """.formatted(recommendations(results));
  }

  private static String runReport(BenchmarkConfig config, BenchmarkRunResult result) {
    BenchmarkRunPlan plan = result.plan();
    return """
        <!doctype html>
        <html lang="en">
        <head>
          <meta charset="utf-8">
          <meta name="viewport" content="width=device-width, initial-scale=1">
          <title>%s</title>
          <style>
            body { margin: 0; font-family: Arial, sans-serif; color: #1f2937; background: #f7f8fa; }
            main { max-width: 960px; margin: 0 auto; padding: 32px 24px; }
            h1 { font-size: 24px; margin: 0 0 8px; }
            h2 { font-size: 18px; margin: 24px 0 10px; }
            pre { background: #fff; border: 1px solid #d9dee7; padding: 14px; overflow: auto; }
            .status { display: inline-block; padding: 3px 8px; border-radius: 4px; font-size: 12px; font-weight: 700; }
            .stable { background: #dff5e5; color: #186238; }
            .unstable { background: #fff1d6; color: #875400; }
            .failed { background: #fde2e1; color: #b42318; }
          </style>
        </head>
        <body>
          <main>
            <a href="../../index.html">Back to benchmark</a>
            <h1>%s</h1>
            <p>Benchmark %s, sink %s, cells %s, CHR EPS %s, target %s.</p>
            <p><span class="status %s">%s</span> %s</p>
            <h2>Flink Snapshot</h2>
            <pre>%s</pre>
            <h2>FDB Metrics Snapshot</h2>
            <pre>%s</pre>
            <h2>Storage Snapshot</h2>
            <pre>%s</pre>
          </main>
        </body>
        </html>
        """.formatted(
            escape(plan.runId()),
            escape(plan.runLabel()),
            escape(config.benchmarkId()),
            escape(plan.sink().value()),
            plan.cellLevel(),
            plan.targetChrEps(),
            escape(config.target()),
            statusClass(result.status()),
            result.status(),
            escape(result.bottleneckReason()),
            escape(result.flink().toString()),
            escape(result.fdb().toString()),
            escape(result.storage().toString()));
  }

  private static String stableBounds(List<BenchmarkRunResult> results) {
    Map<BenchmarkSink, Integer> maxCells = results.stream()
        .filter(result -> result.status() == BenchmarkStatus.STABLE)
        .collect(Collectors.groupingBy(result -> result.plan().sink(),
            Collectors.collectingAndThen(
                Collectors.maxBy(Comparator.comparingInt(result -> result.plan().cellLevel())),
                optional -> optional.map(result -> result.plan().cellLevel()).orElse(0))));
    if (maxCells.isEmpty()) {
      return "<p>No stable upper bound was observed.</p>";
    }
    return "<ul>" + maxCells.entrySet().stream()
        .sorted(Map.Entry.comparingByKey())
        .map(entry -> "<li>" + escape(entry.getKey().value()) + ": " + entry.getValue() + " cells</li>")
        .collect(Collectors.joining()) + "</ul>";
  }

  private static String recommendations(List<BenchmarkRunResult> results) {
    return results.stream()
        .filter(result -> result.status() != BenchmarkStatus.STABLE)
        .findFirst()
        .map(result -> "<p>First bottleneck: " + escape(result.plan().sink().value()) + " at "
            + result.plan().cellLevel() + " cells. " + escape(result.bottleneckReason()) + ".</p>")
        .orElse("<p>All measured runs stayed within thresholds. Increase cell levels or EPS per cell for the next run.</p>");
  }

  private static String statusClass(BenchmarkStatus status) {
    return status.name().toLowerCase();
  }

  private static String escape(String value) {
    if (value == null) {
      return "";
    }
    return value.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace("\"", "&quot;")
        .replace("'", "&#39;");
  }
}

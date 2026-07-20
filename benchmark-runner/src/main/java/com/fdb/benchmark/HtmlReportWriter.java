package com.fdb.benchmark;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

public final class HtmlReportWriter {
  private static final long FILE_SINK_MAX_CHECKPOINT_INTERVAL_MS = 180_000L;

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
          .append("<td>").append(rateValue(plan.targetChrEpsPerCell())).append("</td>")
          .append("<td>").append(formatNumber(plan.targetChrTotalEps())).append("</td>")
          .append("<td><span class=\"status ").append(statusClass(result.status())).append("\">")
          .append(result.status()).append("</span></td>")
          .append("<td>").append(formatNumber(result.flink().recordsOutPerSec())).append("</td>")
          .append("<td>").append(formatMs(result.fdb().kpi1mP95Ms())).append("</td>")
          .append("<td>").append(formatMs(result.fdb().sinkP95Ms())).append("</td>")
          .append("<td>").append(formatMs(result.flink().checkpointDurationMs())).append("</td>")
          .append("<td>").append(formatRatio(result.flink().backpressureRatio())).append("</td>")
          .append("<td>").append(formatMs(result.fdb().watermarkLagMs())).append("</td>")
          .append("<td>").append(escape(result.storage().summary())).append("</td>")
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
            .cards { display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); gap: 10px; margin: 16px 0 24px; }
            .metric { background: #fff; border: 1px solid #d9dee7; padding: 12px; }
            .metric .label { color: #667085; font-size: 12px; text-transform: uppercase; }
            .metric .value { font-size: 20px; font-weight: 700; margin-top: 4px; }
            .metric .detail { color: #667085; font-size: 12px; margin-top: 4px; }
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
            <div class="cards">
              %s
              %s
              %s
              %s
            </div>
            <h2>Stable upper bounds</h2>
            %s
            <h2>Runs</h2>
            <table>
              <thead>
                <tr><th>Sink</th><th>Cells</th><th>CHR EPS/cell/s</th><th>Global CHR EPS</th><th>Status</th><th>Sampled Avg Out EPS</th><th>KPI 1m P95</th><th>Sink P95</th><th>Checkpoint</th><th>Backpressure</th><th>Watermark Lag</th><th>Storage</th><th>Reason</th><th>Report</th></tr>
              </thead>
              <tbody>
        """.formatted(escape(config.benchmarkId()), escape(config.target()), config.warmupSec(), config.durationSec(),
            metricCard("Total Runs", String.valueOf(results.size()), "All attempted levels"),
            metricCard("Stable Runs", String.valueOf(countStatus(results, BenchmarkStatus.STABLE)),
                "Healthy through measurement"),
            metricCard("Unstable/Failed", String.valueOf(countNonStable(results)), "Stopped escalation points"),
            metricCard("Best Stable Bound", bestStableBound(results), "Highest stable cell level"),
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
    FlinkSnapshot flink = result.flink();
    FdbMetricsSnapshot fdb = result.fdb();
    StorageSnapshot storage = result.storage();
    return """
        <!doctype html>
        <html lang="en">
        <head>
          <meta charset="utf-8">
          <meta name="viewport" content="width=device-width, initial-scale=1">
          <title>%s</title>
          <style>
            body { margin: 0; font-family: Arial, sans-serif; color: #1f2937; background: #f7f8fa; }
            main { max-width: 1120px; margin: 0 auto; padding: 32px 24px; }
            h1 { font-size: 24px; margin: 0 0 8px; }
            h2 { font-size: 18px; margin: 24px 0 10px; }
            .meta { color: #667085; margin: 0 0 18px; }
            .cards { display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); gap: 10px; margin: 16px 0 22px; }
            .metric { background: #fff; border: 1px solid #d9dee7; padding: 12px; }
            .metric .label { color: #667085; font-size: 12px; text-transform: uppercase; }
            .metric .value { font-size: 20px; font-weight: 700; margin-top: 4px; }
            .metric .detail { color: #667085; font-size: 12px; margin-top: 4px; }
            table { width: 100%%; border-collapse: collapse; background: #fff; border: 1px solid #d9dee7; }
            th, td { padding: 9px 10px; border-bottom: 1px solid #e6eaf0; text-align: left; font-size: 13px; }
            th { background: #eef2f6; font-weight: 600; }
            .status { display: inline-block; padding: 3px 8px; border-radius: 4px; font-size: 12px; font-weight: 700; }
            .stable { background: #dff5e5; color: #186238; }
            .unstable { background: #fff1d6; color: #875400; }
            .failed { background: #fde2e1; color: #b42318; }
            .health { display: inline-block; padding: 3px 8px; border-radius: 4px; font-size: 12px; font-weight: 700; }
            .health-ok { background: #dff5e5; color: #186238; }
            .health-warn { background: #fff1d6; color: #875400; }
            .health-fail { background: #fde2e1; color: #b42318; }
            .health-na { background: #eef2f6; color: #475467; }
            .raw-links { display: flex; flex-wrap: wrap; gap: 10px; }
            .raw-links a { background: #fff; border: 1px solid #d9dee7; padding: 8px 10px; color: #1d4ed8; }
            .flow-wrap { overflow-x: auto; background: #fff; border: 1px solid #d9dee7; padding: 12px; }
            .operator-flow { min-width: 100%%; }
            .flow-node { cursor: pointer; }
            .flow-node rect { fill: #ffffff; stroke: #8ea1bd; stroke-width: 1.4; }
            .flow-node text { fill: #1f2937; font-size: 12px; }
            .flow-node.active rect { fill: #e8f1ff; stroke: #1d4ed8; stroke-width: 2.2; }
            .flow-edge { stroke: #9aa8bb; stroke-width: 1.4; }
            .flow-edge.active { stroke: #1d4ed8; stroke-width: 2.4; }
            tr.active-row td { background: #e8f1ff; }
            a { color: #1d4ed8; }
          </style>
        </head>
        <body>
          <main>
            <a href="../../index.html">Back to benchmark</a>
            <h1>%s</h1>
            <p class="meta">Benchmark %s | Target %s | Sink %s | Cells %s | CHR EPS %s records/cell/s</p>
            <p><span class="status %s">%s</span> %s</p>
            <div class="cards">
              %s
              %s
              %s
              %s
              %s
              %s
            </div>
            <h2>Run Summary</h2>
            %s
            <h2>Stability Gates</h2>
            %s
            <h2>Source Density</h2>
            %s
            <h2>Run Notes</h2>
            %s
            <h2>Topology Generation</h2>
            %s
            <h2>Flink Resources</h2>
            %s
            <h2>Operator Flow</h2>
            %s
            <h2>Operator Throughput</h2>
            %s
            <h2>Latency</h2>
            %s
            <h2>Sink &amp; Storage</h2>
            %s
            <h2>Raw Artifacts</h2>
            <div class="raw-links">
              <a href="run.json">run.json</a>
              <a href="flink-snapshot.json">flink-snapshot.json</a>
              <a href="fdb-metrics-snapshot.json">fdb-metrics-snapshot.json</a>
              <a href="storage-snapshot.json">storage-snapshot.json</a>
              <a href="source-metrics.json">source-metrics.json</a>
              <a href="topology-metrics.json">topology-metrics.json</a>
            </div>
            <script>
              function highlightOperator(id) {
                document.querySelectorAll('.flow-node').forEach(function(node) {
                  node.classList.toggle('active', node.dataset.operatorId === id);
                });
                document.querySelectorAll('.flow-edge').forEach(function(edge) {
                  edge.classList.toggle('active', edge.dataset.sourceId === id || edge.dataset.targetId === id);
                });
                document.querySelectorAll('tr[data-operator-id]').forEach(function(row) {
                  row.classList.toggle('active-row', row.dataset.operatorId === id);
                });
              }
              document.querySelectorAll('.flow-node, tr[data-operator-id]').forEach(function(item) {
                item.addEventListener('click', function() {
                  highlightOperator(item.dataset.operatorId);
                });
              });
            </script>
          </main>
        </body>
        </html>
        """.formatted(
            escape(plan.runId()),
            escape(plan.runLabel()),
            escape(config.benchmarkId()),
            escape(config.target()),
            escape(plan.sink().value()),
            plan.cellLevel(),
            rateValue(plan.targetChrEpsPerCell()),
            statusClass(result.status()),
            result.status(),
            escape(result.bottleneckReason()),
            metricCard("Status", result.status().name(), result.bottleneckReason()),
            metricCard("Sampled Avg Out EPS", formatNumber(flink.recordsOutPerSec()),
                "Flink sampled window avg records/s"),
            metricCard("Checkpoint", formatMs(flink.checkpointDurationMs()),
                "Failures " + flink.consecutiveCheckpointFailures()),
            metricCard("Backpressure", formatRatio(flink.backpressureRatio()), "Job-level ratio"),
            metricCard("KPI 1m P95", formatMs(fdb.kpi1mP95Ms()), "KPI availability"),
            metricCard("Sink P95", formatMs(fdb.sinkP95Ms()), "Failures " + fdb.sinkFailures()),
            runSummaryTable(config, result),
            stabilityGateTable(config, result),
            sourceDensity(result.source(), plan.cellLevel()),
            runNotes(result),
            topologyTable(result.topology()),
            flinkResourcesTable(flink),
            operatorFlowDiagram(flink),
            operatorThroughputTable(flink),
            latencyTable(plan, fdb),
            storageTable(plan, fdb, storage));
  }

  private static String metricCard(String label, String value, String detail) {
    return """
        <div class="metric">
          <div class="label">%s</div>
          <div class="value">%s</div>
          <div class="detail">%s</div>
        </div>
        """.formatted(escape(label), escape(value), escape(detail));
  }

  private static String stabilityGateTable(BenchmarkConfig config, BenchmarkRunResult result) {
    BenchmarkThresholds thresholds = config.thresholds();
    FlinkSnapshot flink = result.flink();
    FdbMetricsSnapshot fdb = result.fdb();
    SourceMetricsSnapshot source = result.source();
    StorageSnapshot storage = result.storage();
    long kpiAvailabilityP95Ms = maxAvailableLatency(fdb.kpi1mP95Ms(), fdb.kpi5mP95Ms());

    List<HealthGateRow> rows = List.of(
        gate("Flink Job Status", flink.jobStatus(), "RUNNING",
            "RUNNING".equalsIgnoreCase(flink.jobStatus()) ? GateHealth.HEALTHY : GateHealth.FAILED),
        gate("Source Metrics", source.present() ? "present" : "missing", "present",
            source.present() ? GateHealth.HEALTHY : GateHealth.UNSTABLE),
        gate("CHR Source Metrics", source.hasChrMetrics() ? "present" : "missing", "present",
            source.hasChrMetrics() ? GateHealth.HEALTHY : GateHealth.UNSTABLE),
        gate("Source Throughput Attainment", source.hasChrMetrics() ? formatRatio(source.producerDeliveryRatio()) : "N/A",
            ">= " + formatRatio(thresholds.minProducerDeliveryRatio()),
            source.hasChrMetrics()
                ? health(source.producerDeliveryRatio() >= thresholds.minProducerDeliveryRatio())
                : GateHealth.NA),
        gate("Source Operator Backlog", flink.sourceBacklogRecords() + " records",
            "<= " + thresholds.maxSourceBacklogRecords() + " records",
            health(flink.sourceBacklogRecords() <= thresholds.maxSourceBacklogRecords())),
        gate("Backpressure Ratio", formatRatio(flink.backpressureRatio()),
            "<= " + formatRatio(thresholds.maxBackpressureRatio()),
            health(flink.backpressureRatio() <= thresholds.maxBackpressureRatio())),
        gate("Consecutive Checkpoint Failures", flink.consecutiveCheckpointFailures() + " failures",
            "< " + thresholds.maxConsecutiveCheckpointFailures() + " failures",
            health(flink.consecutiveCheckpointFailures() < thresholds.maxConsecutiveCheckpointFailures())),
        gate("Checkpoint Duration", formatMs(flink.checkpointDurationMs()),
            "<= " + formatMs(thresholds.maxCheckpointDurationMs()),
            health(flink.checkpointDurationMs() <= thresholds.maxCheckpointDurationMs())),
        gate("KPI Availability P95", formatMs(kpiAvailabilityP95Ms),
            "<= " + formatMs(thresholds.maxKpiAvailabilityP95Ms()),
            kpiAvailabilityP95Ms >= 0
                ? health(kpiAvailabilityP95Ms <= thresholds.maxKpiAvailabilityP95Ms())
                : GateHealth.NA),
        gate("Sink P95", formatMs(fdb.sinkP95Ms()),
            "<= " + formatMs(thresholds.maxSinkP95Ms()),
            fdb.sinkP95Ms() >= 0 ? health(fdb.sinkP95Ms() <= thresholds.maxSinkP95Ms()) : GateHealth.NA),
        gate("Sink Failures", fdb.sinkFailures() + " failures", "<= 0 failures",
            health(fdb.sinkFailures() <= 0)),
        gate("Watermark Lag", formatMs(fdb.watermarkLagMs()),
            "<= " + formatMs(thresholds.maxWatermarkLagMs()),
            health(fdb.watermarkLagMs() <= thresholds.maxWatermarkLagMs())),
        gate("Storage Health", storage.healthy() ? "healthy" : "unhealthy", "healthy",
            storage.healthy() ? GateHealth.HEALTHY : GateHealth.UNSTABLE));

    StringBuilder body = new StringBuilder();
    for (HealthGateRow row : rows) {
      body.append("<tr><td>").append(escape(row.metric())).append("</td><td>")
          .append(escape(row.observed())).append("</td><td>")
          .append(escape(row.threshold())).append("</td><td>")
          .append(healthBadge(row.health())).append("</td></tr>");
    }
    return """
        <table>
          <thead><tr><th>Metric</th><th>Observed</th><th>Threshold</th><th>Health</th></tr></thead>
          <tbody>%s</tbody>
        </table>
        """.formatted(body);
  }

  private static HealthGateRow gate(String metric, String observed, String threshold, GateHealth health) {
    return new HealthGateRow(metric, observed, threshold, health);
  }

  private static GateHealth health(boolean healthy) {
    return healthy ? GateHealth.HEALTHY : GateHealth.UNSTABLE;
  }

  private static String healthBadge(GateHealth health) {
    return "<span class=\"health " + health.cssClass() + "\">" + health.label() + "</span>";
  }

  private static String runSummaryTable(BenchmarkConfig config, BenchmarkRunResult result) {
    BenchmarkRunPlan plan = result.plan();
    FlinkSnapshot flink = result.flink();
    SourceMetricsSnapshot source = result.source();
    return twoColumnTable(List.of(
        row("Benchmark", config.benchmarkId()),
        row("Target", config.target()),
        row("Run ID", plan.runId()),
        row("Run Label", plan.runLabel()),
        row("Sink", plan.sink().value()),
        row("Cell Level", String.valueOf(plan.cellLevel())),
        row("Target CHR EPS", rateValue(plan.targetChrEpsPerCell()) + " records/cell/s"),
        row("Global CHR EPS", formatNumber(plan.targetChrTotalEps()) + " records/s"),
        row("Target PM EPS", rateValue(plan.targetPmEpsPerCell()) + " records/cell/s"),
        row("Global PM EPS", formatNumber(plan.targetPmTotalEps()) + " records/s"),
        row("Status", result.status().name()),
        row("Reason", result.bottleneckReason()),
        row("Source Throughput Attainment", formatRatio(source.producerDeliveryRatio())),
        row("Source Backlog", source.chrBacklogRecords() + " CHR records"),
        row("Checkpoint Interval", formatMs(effectiveCheckpointIntervalMs(plan.sink(), config.checkpointIntervalMs()))),
        row("Checkpoint Duration", formatMs(flink.checkpointDurationMs())),
        row("Checkpoint Failures", String.valueOf(flink.consecutiveCheckpointFailures()))));
  }

  private static String runNotes(BenchmarkRunResult result) {
    boolean hasKpi5mSinkRows = result.fdb().sinkLatencies().stream()
        .anyMatch(sink -> lower(sink.sinkName()).contains("kpi-5m") && sink.records() > 0);
    boolean hasKpi5mOperatorInput = result.flink().operators().stream()
        .anyMatch(operator -> lower(operator.name()).contains("kpi-5m") && operator.recordsInTotal() > 0);
    String fiveMinuteNote = hasKpi5mSinkRows
        ? "KPI 5m has sink samples in this run."
        : hasKpi5mOperatorInput
            ? "KPI 5m rollup received 1m input, but the 5-minute event-time window had not emitted sink rows when sampled."
            : "KPI 5m needs a 5-minute event-time window plus watermark progress; short or early-stopped runs may show zero rows.";
    return twoColumnTable(List.of(
        row("Topology Generation Duration",
            "Only local topology object generation time; Kafka publish and total duration are listed separately."),
        row("Terminal Sink Records Out",
            "Flink terminal sink operators commonly show Records Out Total as 0; use Records In Total and Sink & Storage records for write volume."),
        row("KPI 5m Output", fiveMinuteNote),
        row("Anomaly Output",
            "Rules emit only after consecutive abnormal evaluations; zero rows means no activation or no committed sink rows in this run.")));
  }

  private static String sourceDensity(SourceMetricsSnapshot source, int cellLevel) {
    return """
        <table>
          <thead>
            <tr><th>Source</th><th>Total</th><th>Records/s</th><th>Total/cell</th><th>Records/s/cell</th></tr>
          </thead>
          <tbody>
            %s
            %s
            %s
          </tbody>
        </table>
        """.formatted(
            densityRow("CHR total", source.chrPublished(), source.chrObservedEps(),
                source.chrTotalPerCell(cellLevel), source.chrPerSecondPerCell(cellLevel)),
            densityRow("PM total", source.pmPublished(), source.pmObservedEps(),
                source.pmTotalPerCell(cellLevel), source.pmPerSecondPerCell(cellLevel)),
            cfgDensityRow(source, cellLevel));
  }

  private static String densityRow(String label, long total, double recordsPerSecond, double totalPerCell,
      double recordsPerSecondPerCell) {
    return "<tr><th>" + escape(label) + "</th><td>" + total + "</td><td>" + formatDouble(recordsPerSecond)
        + "</td><td>" + formatDouble(totalPerCell) + "</td><td>" + formatDouble(recordsPerSecondPerCell)
        + "</td></tr>";
  }

  private static String cfgDensityRow(SourceMetricsSnapshot source, int cellLevel) {
    return "<tr><th>CFG total</th><td>" + source.cfgPublished() + "</td><td>"
        + formatDouble(source.cfgObservedEps()) + "</td><td>" + formatDouble(source.cfgTotalPerCell(cellLevel))
        + "</td><td>N/A (one-time init)</td></tr>";
  }

  private static String topologyTable(TopologyMetricsSnapshot topology) {
    if (topology == null || !topology.present()) {
      return twoColumnTable(List.of(row("Status", "No topology metrics file captured")));
    }
    return twoColumnTable(List.of(
        row("Generated Records", formatNumber(topology.generatedRecords())),
        row("Site Count", formatNumber(topology.siteCount())),
        row("Frequency Bands", formatNumber(topology.frequencyBandCount())),
        row("Topology Generation Duration", formatMs(topology.generationDurationMs())),
        row("Kafka Publish Duration", formatMs(topology.publishDurationMs())),
        row("Total Duration", formatMs(topology.totalDurationMs())),
        row("Published Topology Records", formatNumber(topology.publishedRecords())),
        row("Publish Failures", formatNumber(topology.publishFailures())),
        row("Latitude Range", formatRange(topology.minLatitude(), topology.maxLatitude())),
        row("Longitude Range", formatRange(topology.minLongitude(), topology.maxLongitude()))));
  }

  private static String flinkResourcesTable(FlinkSnapshot flink) {
    return twoColumnTable(List.of(
        row("Job Status", flink.jobStatus()),
        row("TaskManagers", String.valueOf(flink.taskManagers())),
        row("Slots", String.valueOf(flink.slots())),
        row("Sampled Avg Records In/s", formatNumber(flink.recordsInPerSec())),
        row("Sampled Avg Records Out/s", formatNumber(flink.recordsOutPerSec())),
        row("Operator Aggregate Records In Total", formatNumber(flink.recordsInTotal())),
        row("Operator Aggregate Records Out Total", formatNumber(flink.recordsOutTotal())),
        row("Backpressure", formatRatio(flink.backpressureRatio())),
        row("Checkpoint Duration", formatMs(flink.checkpointDurationMs())),
        row("Checkpoint Failures", String.valueOf(flink.consecutiveCheckpointFailures()))));
  }

  private static String operatorThroughputTable(FlinkSnapshot flink) {
    StringBuilder rows = new StringBuilder();
    if (flink.operators().isEmpty()) {
      rows.append("<tr><td>aggregate job</td><td>-</td><td>")
          .append(formatNumber(flink.recordsInPerSec())).append("</td><td>")
          .append(formatNumber(flink.recordsOutPerSec())).append("</td><td>")
          .append(formatNumber(flink.recordsInTotal())).append("</td><td>")
          .append(formatNumber(flink.recordsOutTotal())).append("</td><td>-</td><td>-</td><td>-</td><td>-</td><td>")
          .append(formatRatio(flink.backpressureRatio())).append("</td></tr>");
    } else {
      for (FlinkOperatorSnapshot operator : flink.operators()) {
        rows.append("<tr id=\"operator-row-").append(htmlId(operator.id())).append("\" data-operator-id=\"")
            .append(escape(operator.id())).append("\"><td>").append(escape(operator.name())).append("</td><td>")
            .append(operator.parallelism()).append("</td><td>")
            .append(formatNumber(operator.recordsInPerSec())).append("</td><td>")
            .append(formatNumber(operator.recordsOutPerSec())).append("</td><td>")
            .append(formatNumber(operator.recordsInTotal())).append("</td><td>")
            .append(formatNumber(operator.recordsOutTotal())).append("</td><td>")
            .append(formatNumber(operator.bytesInPerSec())).append("</td><td>")
            .append(formatNumber(operator.bytesOutPerSec())).append("</td><td>")
            .append(formatRatio(operator.busyRatio())).append("</td><td>")
            .append(formatRatio(operator.idleRatio())).append("</td><td>")
            .append(formatRatio(operator.backpressureRatio())).append("</td></tr>");
      }
    }
    return """
        <table>
          <thead><tr><th>Operator</th><th>Parallelism</th><th>Sampled Avg Records In/s</th><th>Sampled Avg Records Out/s</th><th>Records In Total</th><th>Records Out Total</th><th>Bytes In/s</th><th>Bytes Out/s</th><th>Busy</th><th>Idle</th><th>Backpressure</th></tr></thead>
          <tbody>%s</tbody>
        </table>
        """.formatted(rows);
  }

  private static String operatorFlowDiagram(FlinkSnapshot flink) {
    if (flink.operators().isEmpty()) {
      return "<p>No operator graph was captured for this run.</p>";
    }
    Map<String, Integer> levels = operatorLevels(flink.operators(), flink.operatorEdges());
    Map<String, FlowPosition> positions = operatorPositions(flink.operators(), levels);
    int maxLevel = levels.values().stream().max(Integer::compareTo).orElse(0);
    Map<Integer, Long> perLevel = levels.values().stream()
        .collect(Collectors.groupingBy(level -> level, Collectors.counting()));
    long maxRows = perLevel.values().stream().max(Long::compareTo).orElse(1L);
    int width = Math.max(760, 260 + (maxLevel * 260));
    int height = Math.max(180, 100 + ((int) maxRows * 110));

    StringBuilder edges = new StringBuilder();
    for (FlinkOperatorEdge edge : flink.operatorEdges()) {
      FlowPosition source = positions.get(edge.sourceId());
      FlowPosition target = positions.get(edge.targetId());
      if (source == null || target == null) {
        continue;
      }
      edges.append("<line class=\"flow-edge\" data-source-id=\"").append(escape(edge.sourceId()))
          .append("\" data-target-id=\"").append(escape(edge.targetId()))
          .append("\" x1=\"").append(source.x() + 190).append("\" y1=\"").append(source.y() + 34)
          .append("\" x2=\"").append(target.x() - 10).append("\" y2=\"").append(target.y() + 34)
          .append("\" marker-end=\"url(#arrow)\"></line>");
    }

    StringBuilder nodes = new StringBuilder();
    for (FlinkOperatorSnapshot operator : flink.operators()) {
      FlowPosition position = positions.get(operator.id());
      nodes.append("<g class=\"flow-node\" data-operator-id=\"").append(escape(operator.id()))
          .append("\" transform=\"translate(").append(position.x()).append(",").append(position.y()).append(")\">")
          .append("<rect width=\"180\" height=\"68\" rx=\"4\"></rect>")
          .append("<text x=\"10\" y=\"22\">").append(escape(shortLabel(operator.name(), 28))).append("</text>")
          .append("<text x=\"10\" y=\"43\">p=").append(operator.parallelism())
          .append(" out/s=").append(escape(formatNumber(operator.recordsOutPerSec()))).append("</text>")
          .append("<text x=\"10\" y=\"60\">bp=").append(escape(formatRatio(operator.backpressureRatio())))
          .append("</text></g>");
    }

    return """
        <div class="flow-wrap">
          <svg class="operator-flow" viewBox="0 0 %s %s" role="img" aria-label="Flink operator flow">
            <defs>
              <marker id="arrow" markerWidth="10" markerHeight="10" refX="9" refY="3" orient="auto" markerUnits="strokeWidth">
                <path d="M0,0 L0,6 L9,3 z" fill="#9aa8bb"></path>
              </marker>
            </defs>
            %s
            %s
          </svg>
        </div>
        """.formatted(width, height, edges, nodes);
  }

  private static Map<String, Integer> operatorLevels(List<FlinkOperatorSnapshot> operators,
      List<FlinkOperatorEdge> edges) {
    Map<String, Integer> levels = new LinkedHashMap<>();
    for (FlinkOperatorSnapshot operator : operators) {
      levels.put(operator.id(), 0);
    }
    for (int i = 0; i < operators.size(); i++) {
      boolean changed = false;
      for (FlinkOperatorEdge edge : edges) {
        if (!levels.containsKey(edge.sourceId()) || !levels.containsKey(edge.targetId())) {
          continue;
        }
        int nextLevel = levels.get(edge.sourceId()) + 1;
        if (nextLevel > levels.get(edge.targetId())) {
          levels.put(edge.targetId(), nextLevel);
          changed = true;
        }
      }
      if (!changed) {
        break;
      }
    }
    return levels;
  }

  private static Map<String, FlowPosition> operatorPositions(List<FlinkOperatorSnapshot> operators,
      Map<String, Integer> levels) {
    Map<Integer, Integer> indexes = new HashMap<>();
    Map<String, FlowPosition> positions = new HashMap<>();
    for (FlinkOperatorSnapshot operator : operators) {
      int level = levels.getOrDefault(operator.id(), 0);
      int index = indexes.getOrDefault(level, 0);
      indexes.put(level, index + 1);
      positions.put(operator.id(), new FlowPosition(40 + level * 260, 30 + index * 110));
    }
    return positions;
  }

  private record FlowPosition(int x, int y) {
  }

  private static String latencyTable(BenchmarkRunPlan plan, FdbMetricsSnapshot fdb) {
    List<StageLatencySnapshot> stages = fdb.stageLatencies().stream()
        .filter(stage -> isRelevantStage(plan.sink(), stage.stageId()))
        .toList();
    if (!stages.isEmpty()) {
      StringBuilder rows = new StringBuilder();
      for (StageLatencySnapshot stage : stages) {
        rows.append("<tr><td>").append(escape(stage.stageId())).append("</td><td>")
            .append(formatMs(stage.latencyP50Ms())).append("</td><td>")
            .append(formatMs(stage.latencyP95Ms())).append("</td><td>")
            .append(formatMs(stage.latencyP99Ms())).append("</td><td>")
            .append(formatMs(stage.watermarkLagMs())).append("</td></tr>");
      }
      return """
          <table>
            <thead><tr><th>Stage</th><th>P50</th><th>P95</th><th>P99</th><th>Watermark Lag</th></tr></thead>
            <tbody>%s</tbody>
          </table>
          """.formatted(rows);
    }
    return """
        <table>
          <thead><tr><th>Metric</th><th>P95</th><th>Notes</th></tr></thead>
          <tbody>
            <tr><td>Source Delay</td><td>%s</td><td>End-to-end source arrival delay</td></tr>
            <tr><td>KPI 1m Availability</td><td>%s</td><td>One-minute KPI availability latency</td></tr>
            <tr><td>KPI 5m Availability</td><td>%s</td><td>Five-minute KPI availability latency</td></tr>
            <tr><td>Sink Latency</td><td>%s</td><td>Business sink write latency</td></tr>
            <tr><td>Watermark Lag</td><td>%s</td><td>Maximum observed stage watermark lag</td></tr>
          </tbody>
        </table>
        """.formatted(
            formatMs(fdb.sourceDelayP95Ms()),
            formatMs(fdb.kpi1mP95Ms()),
            formatMs(fdb.kpi5mP95Ms()),
            formatMs(fdb.sinkP95Ms()),
            formatMs(fdb.watermarkLagMs()));
  }

  private static String storageTable(BenchmarkRunPlan plan, FdbMetricsSnapshot fdb, StorageSnapshot storage) {
    List<SinkLatencySnapshot> currentSinks = fdb.sinkLatencies().stream()
        .filter(sink -> isCurrentBusinessSink(plan.sink(), sink.sinkName()))
        .toList();
    List<SinkLatencySnapshot> auxiliarySinks = fdb.sinkLatencies().stream()
        .filter(sink -> !isCurrentBusinessSink(plan.sink(), sink.sinkName()))
        .filter(sink -> isAuxiliarySink(sink.sinkName()))
        .toList();
    if (!currentSinks.isEmpty() || !auxiliarySinks.isEmpty()) {
      StringBuilder rows = new StringBuilder();
      for (SinkLatencySnapshot sink : currentSinks) {
        appendSinkRow(rows, "Current", sink);
      }
      if (currentSinks.isEmpty()) {
        rows.append("<tr><td>Current</td><td colspan=\"7\">No current business sink latency samples</td></tr>");
      }
      for (SinkLatencySnapshot sink : auxiliarySinks) {
        appendSinkRow(rows, "Auxiliary", sink);
      }
      rows.append("<tr><td>Storage Health</td><td colspan=\"3\">")
          .append(storage.healthy() ? "healthy" : "unhealthy").append("</td><td colspan=\"4\">")
          .append(escape(storage.summary())).append("</td></tr>");
      appendStorageProbeRow(rows, plan.sink(), storage);
      return """
          <table>
            <thead><tr><th>Type</th><th>Sink</th><th>Records</th><th>Bytes</th><th>P50</th><th>P95</th><th>P99</th><th>Failures</th></tr></thead>
            <tbody>%s</tbody>
          </table>
          """.formatted(rows);
    }
    return """
        <table>
          <thead><tr><th>Area</th><th>Value</th><th>Status</th></tr></thead>
          <tbody>
            <tr><td>Sink P95</td><td>%s</td><td>%s failures</td></tr>
            <tr><td>Storage Health</td><td>%s</td><td>%s</td></tr>
            <tr><td>Storage Records</td><td>%s</td><td>Small files %s, in-progress %s</td></tr>
          </tbody>
        </table>
        """.formatted(
            formatMs(fdb.sinkP95Ms()),
            fdb.sinkFailures(),
            storage.healthy() ? "healthy" : "unhealthy",
            escape(storage.summary()),
            storage.records(),
            storage.smallFiles(),
            storage.inProgressFiles());
  }

  private static void appendStorageProbeRow(StringBuilder rows, BenchmarkSink sink, StorageSnapshot storage) {
    if (sink == BenchmarkSink.STARROCKS) {
      rows.append("<tr><td>Storage Rows</td><td>").append(formatNumber(storage.records()))
          .append("</td><td>-</td><td colspan=\"5\">")
          .append(escape(storage.summary())).append("</td></tr>");
      return;
    }
    rows.append("<tr><td>Storage Files</td><td>").append(formatNumber(storage.records()))
        .append("</td><td>-</td><td colspan=\"5\">Small files ")
        .append(storage.smallFiles()).append(", in-progress ").append(storage.inProgressFiles()).append("</td></tr>");
  }

  private static void appendSinkRow(StringBuilder rows, String type, SinkLatencySnapshot sink) {
    rows.append("<tr><td>").append(escape(type)).append("</td><td>").append(escape(sink.sinkName())).append("</td><td>")
        .append(sink.records()).append("</td><td>")
        .append(sink.bytes()).append("</td><td>")
        .append(formatMs(sink.latencyP50Ms())).append("</td><td>")
        .append(formatMs(sink.latencyP95Ms())).append("</td><td>")
        .append(formatMs(sink.latencyP99Ms())).append("</td><td>")
        .append(sink.failures()).append("</td></tr>");
  }

  private static boolean isRelevantStage(BenchmarkSink sink, String stageId) {
    String value = lower(stageId);
    if (value.contains("starrocks") || value.contains("iceberg") || value.contains("hive")
        || value.contains("kafka")) {
      return sink != BenchmarkSink.NONE && value.contains(sink.value());
    }
    return true;
  }

  private static boolean isCurrentBusinessSink(BenchmarkSink sink, String sinkName) {
    return sink != BenchmarkSink.NONE && lower(sinkName).contains(sink.value());
  }

  private static boolean isAuxiliarySink(String sinkName) {
    String value = lower(sinkName);
    return value.contains("dlq") || value.contains("metric") || value.contains("late");
  }

  private static String lower(String value) {
    return value == null ? "" : value.toLowerCase(Locale.ROOT);
  }

  private static String htmlId(String value) {
    String normalized = value == null ? "" : value.replaceAll("[^A-Za-z0-9_-]", "_");
    return normalized.isBlank() ? "operator" : normalized;
  }

  private static String shortLabel(String value, int maxLength) {
    if (value == null || value.length() <= maxLength) {
      return value == null ? "" : value;
    }
    return value.substring(0, Math.max(0, maxLength - 1)) + "...";
  }

  private static String twoColumnTable(List<TableRow> rows) {
    StringBuilder body = new StringBuilder();
    for (TableRow row : rows) {
      body.append("<tr><th>").append(escape(row.label())).append("</th><td>")
          .append(escape(row.value())).append("</td></tr>");
    }
    return "<table><tbody>" + body + "</tbody></table>";
  }

  private static TableRow row(String label, String value) {
    return new TableRow(label, value);
  }

  private record TableRow(String label, String value) {
  }

  private record HealthGateRow(String metric, String observed, String threshold, GateHealth health) {
  }

  private enum GateHealth {
    HEALTHY("health-ok", "HEALTHY"),
    UNSTABLE("health-warn", "UNSTABLE"),
    FAILED("health-fail", "FAILED"),
    NA("health-na", "N/A");

    private final String cssClass;
    private final String label;

    GateHealth(String cssClass, String label) {
      this.cssClass = cssClass;
      this.label = label;
    }

    String cssClass() {
      return cssClass;
    }

    String label() {
      return label;
    }
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

  private static long countStatus(List<BenchmarkRunResult> results, BenchmarkStatus status) {
    return results.stream().filter(result -> result.status() == status).count();
  }

  private static long countNonStable(List<BenchmarkRunResult> results) {
    return results.stream().filter(result -> result.status() != BenchmarkStatus.STABLE).count();
  }

  private static String bestStableBound(List<BenchmarkRunResult> results) {
    return results.stream()
        .filter(result -> result.status() == BenchmarkStatus.STABLE)
        .max(Comparator.comparingInt(result -> result.plan().cellLevel()))
        .map(result -> result.plan().sink().value() + " " + result.plan().cellLevel() + " cells")
        .orElse("none");
  }

  private static String recommendations(List<BenchmarkRunResult> results) {
    return results.stream()
        .filter(result -> result.status() != BenchmarkStatus.STABLE)
        .findFirst()
        .map(result -> "<p>First bottleneck: " + escape(result.plan().sink().value()) + " at "
            + result.plan().cellLevel() + " cells. " + escape(result.bottleneckReason()) + ".</p>")
        .orElse("<p>All measured runs stayed within thresholds. Increase cell levels or EPS per cell for the next run.</p>");
  }

  private static long effectiveCheckpointIntervalMs(BenchmarkSink sink, long configuredMs) {
    if (sink == BenchmarkSink.HIVE || sink == BenchmarkSink.ICEBERG) {
      return Math.min(configuredMs, FILE_SINK_MAX_CHECKPOINT_INTERVAL_MS);
    }
    return configuredMs;
  }

  private static long maxAvailableLatency(long left, long right) {
    long max = -1L;
    if (left >= 0) {
      max = left;
    }
    if (right >= 0) {
      max = Math.max(max, right);
    }
    return max;
  }

  private static String statusClass(BenchmarkStatus status) {
    return status.name().toLowerCase(Locale.ROOT);
  }

  private static String formatMs(long value) {
    return value < 0 ? "N/A" : value + " ms";
  }

  private static String formatRatio(double value) {
    return String.format(Locale.ROOT, "%.2f%%", value * 100.0);
  }

  private static String formatNumber(double value) {
    if (!Double.isFinite(value)) {
      return String.valueOf(value);
    }
    if (Math.rint(value) == value) {
      return String.valueOf((long) value);
    }
    return String.format(Locale.ROOT, "%.2f", value);
  }

  private static String formatDouble(double value) {
    return String.format(Locale.ROOT, "%.2f", value);
  }

  private static String rateValue(double value) {
    return java.math.BigDecimal.valueOf(value).stripTrailingZeros().toPlainString();
  }

  private static String formatRange(double min, double max) {
    return String.format(Locale.ROOT, "%.6f..%.6f", min, max);
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

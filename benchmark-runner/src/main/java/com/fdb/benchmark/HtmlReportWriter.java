package com.fdb.benchmark;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
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
          .append("<td>").append(formatFlinkMetric(result, result.flink().recordsOutPerSec())).append("</td>")
          .append("<td>").append(formatMs(result.fdb().kpi1mP95Ms())).append("</td>")
          .append("<td>").append(formatMs(result.fdb().sinkP95Ms())).append("</td>")
          .append("<td>").append(formatMs(result.fdb().connectorCommitP95Ms())).append("</td>")
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
            .muted { color: #667085; }
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
                <tr><th>Sink</th><th>Cells</th><th>CHR EPS/cell/s</th><th>Global CHR EPS</th><th>Status</th><th>Sampled Avg Out EPS</th><th>KPI 1m P95</th><th>Sink P95</th><th>Connector Commit P95</th><th>Checkpoint</th><th>Backpressure</th><th>Watermark Lag</th><th>Storage</th><th>Reason</th><th>Report</th></tr>
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
            .flow-node .flow-latency { font-weight: 700; }
            .flow-node-ok rect { fill: #edf8f1; stroke: #2f855a; }
            .flow-node-ok .flow-latency { fill: #186238; }
            .flow-node-warn rect { fill: #fff3d6; stroke: #d97706; stroke-width: 2; }
            .flow-node-warn .flow-latency { fill: #875400; }
            .flow-node-fail rect { fill: #fde2e1; stroke: #b42318; stroke-width: 2; }
            .flow-node-fail .flow-latency { fill: #b42318; }
            .flow-node-na rect { fill: #f5f7fa; stroke: #b9c2d0; }
            .flow-node-na .flow-latency { fill: #667085; }
            .flow-node.active rect { fill: #e8f1ff; stroke: #1d4ed8; stroke-width: 2.2; }
            .flow-edge { stroke: #9aa8bb; stroke-width: 1.4; }
            .flow-edge-marker-ok { stroke: #64748b; stroke-width: 1.8; }
            .flow-edge-marker-warn { stroke: #d97706; stroke-width: 2.6; }
            .flow-edge-marker-label { fill: #475467; font-size: 11px; font-weight: 700; paint-order: stroke; stroke: #ffffff; stroke-width: 3px; stroke-linejoin: round; }
            .flow-edge-marker-label-warn { fill: #875400; }
            .flow-edge.active { stroke: #1d4ed8; stroke-width: 2.4; }
            tr.active-row td { background: #e8f1ff; }
            .operator-marker-row td { background: #fbfcfe; color: #475467; font-size: 12px; }
            .operator-marker-row td:first-child { padding-left: 24px; font-weight: 700; }
            .operator-section { display: flex; flex-direction: column; gap: 12px; }
            .column-guide { background: #fff; border: 1px solid #d9dee7; padding: 12px; }
            .column-guide h3 { font-size: 15px; margin: 0 0 8px; }
            .column-guide table { margin-top: 8px; }
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
              %s
              %s
            </div>
            <h2>Run Summary</h2>
            %s
            <h2>Stability Gates</h2>
            %s
            <h2>Source Density</h2>
            %s
            <h2>1m Window Diagnostics</h2>
            %s
            <h2>Run Notes</h2>
            %s
            <h2>Topology Generation</h2>
            %s
            <h2>Flink Resources</h2>
            %s
            <h2>Operator Flow &amp; Metrics</h2>
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
            metricCard("Sampled Avg Out EPS", formatFlinkMetric(result, flink.recordsOutPerSec()),
                "Flink sampled window avg records/s"),
            metricCard("Checkpoint", formatMs(flink.checkpointDurationMs()),
                "Failures " + flink.consecutiveCheckpointFailures()),
            metricCard("Backpressure", formatRatio(flink.backpressureRatio()), "Job-level ratio"),
            metricCard("KPI 1m P95", formatMs(fdb.kpi1mP95Ms()), "KPI availability"),
            metricCard("Sink P95", formatMs(fdb.sinkP95Ms()), "Failures " + fdb.sinkFailures()),
            metricCard("Connector Write P95", formatMs(fdb.connectorWriteP95Ms()), "Connector write/invoke duration"),
            metricCard("Connector Commit P95", formatMs(fdb.connectorCommitP95Ms()), "Flush, prepare, commit, checkpoint"),
            runSummaryTable(config, result),
            stabilityGateTable(config, result),
            sourceDensity(result.source(), plan.cellLevel()),
            windowDiagnosticsTable(plan, result.source(), fdb),
            runNotes(result),
            topologyTable(result.topology()),
            flinkResourcesTable(result),
            operatorFlowAndMetrics(config, result),
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
    WindowMaterializationSnapshot windows = WindowMaterializationSnapshot.from(result.plan(), source, fdb);
    boolean operatorMetricsTrusted = operatorMetricsTrusted(result);

    List<HealthGateRow> rows = List.of(
        gate("Flink Job Status", flink.jobStatus(), "RUNNING",
            "RUNNING".equalsIgnoreCase(flink.jobStatus()) ? GateHealth.HEALTHY : GateHealth.FAILED),
        gate("Source Metrics", source.present() ? "present" : "missing", "present",
            source.present() ? GateHealth.HEALTHY : GateHealth.UNSTABLE),
        gate("CHR Source Metrics", source.hasChrMetrics() ? "present" : "missing", "present",
            source.hasChrMetrics() ? GateHealth.HEALTHY : GateHealth.UNSTABLE),
        gate("Operator Metrics Validity", operatorMetricsTrusted ? "available" : operatorMetricsValidity(result),
            "available when source records are published",
            operatorMetricsTrusted ? GateHealth.HEALTHY : GateHealth.NA),
        gate("Source Throughput Attainment", source.hasChrMetrics() ? formatRatio(source.producerDeliveryRatio()) : "N/A",
            ">= " + formatRatio(thresholds.minProducerDeliveryRatio()),
            source.hasChrMetrics()
                ? health(source.producerDeliveryRatio() >= thresholds.minProducerDeliveryRatio())
                : GateHealth.NA),
        gate("1m Window Materialization", windows.observedClosedMinutes(), windows.thresholdText(),
            windows.applicable() ? health(windows.healthy()) : GateHealth.NA),
        gate("Source Operator Backlog", operatorMetricsTrusted ? flink.sourceBacklogRecords() + " records" : "N/A",
            "<= " + thresholds.maxSourceBacklogRecords() + " records",
            operatorMetricsTrusted
                ? health(flink.sourceBacklogRecords() <= thresholds.maxSourceBacklogRecords())
                : GateHealth.NA),
        gate("Backpressure Ratio", operatorMetricsTrusted ? formatRatio(flink.backpressureRatio()) : "N/A",
            "<= " + formatRatio(thresholds.maxBackpressureRatio()),
            operatorMetricsTrusted ? health(flink.backpressureRatio() <= thresholds.maxBackpressureRatio()) : GateHealth.NA),
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
        gate("Connector Write P95", formatMs(fdb.connectorWriteP95Ms()),
            "<= " + formatMs(thresholds.maxSinkP95Ms()),
            fdb.connectorWriteP95Ms() >= 0
                ? health(fdb.connectorWriteP95Ms() <= thresholds.maxSinkP95Ms())
                : GateHealth.NA),
        gate("Connector Commit P95", formatMs(fdb.connectorCommitP95Ms()),
            "<= " + formatMs(thresholds.maxSinkP95Ms()),
            fdb.connectorCommitP95Ms() >= 0
                ? health(fdb.connectorCommitP95Ms() <= thresholds.maxSinkP95Ms())
                : GateHealth.NA),
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
    WindowMaterializationSnapshot windows = WindowMaterializationSnapshot.from(plan, source, result.fdb());
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
        row("Expected 1m Closed Windows", String.valueOf(windows.expectedClosedMinuteWindows())),
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
        row("Sink Probe Scope",
            "sink-front is measured before connector write. connector-write is connector writer/invoke duration. connector-commit is flush/prepare/commit/checkpoint duration and does not include connector-write."),
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

  private static String windowDiagnosticsTable(BenchmarkRunPlan plan, SourceMetricsSnapshot source,
      FdbMetricsSnapshot fdb) {
    WindowMaterializationSnapshot windows = WindowMaterializationSnapshot.from(plan, source, fdb);
    WindowDiagnosticColumns columns = windowDiagnosticColumns(windows);
    StringBuilder rows = new StringBuilder();
    appendWindowStageRow(rows, windows.chr(), windows.expectedClosedMinuteWindows(), windows.applicable(), columns);
    appendWindowStageRow(rows, windows.pm(), windows.expectedClosedMinuteWindows(), windows.applicable(), columns);
    appendWindowStageRow(rows, windows.kpi(), windows.expectedClosedMinuteWindows(), windows.applicable(), columns);
    return """
        <table>
          %s
          <tbody>%s</tbody>
        </table>
        """.formatted(windowDiagnosticsHeader(columns), rows);
  }

  private static void appendWindowStageRow(StringBuilder rows, WindowMaterializationSnapshot.WindowStage stage,
      long expectedClosedMinuteWindows, boolean applicable, WindowDiagnosticColumns columns) {
    GateHealth health = !applicable ? GateHealth.NA : health(stage.healthy(expectedClosedMinuteWindows));
    rows.append("<tr><td>").append(escape(stage.label())).append("</td><td>")
        .append(formatNumber(stage.recordsInTotal())).append("</td><td>")
        .append(formatNumber(stage.recordsOutTotal())).append("</td><td>")
        .append(stage.closedMinuteWindows()).append("</td><td>")
        .append(expectedClosedMinuteWindows).append("</td>");
    if (columns.showInputWatermark()) {
      rows.append("<td>").append(escape(formatWatermark(stage.currentInputWatermarkMs()))).append("</td>");
    }
    if (columns.showOutputWatermark()) {
      rows.append("<td>").append(escape(formatWatermark(stage.currentOutputWatermarkMs()))).append("</td>");
    }
    rows.append("<td>")
        .append(healthBadge(health)).append("</td></tr>");
  }

  private static String windowDiagnosticsHeader(WindowDiagnosticColumns columns) {
    StringBuilder header = new StringBuilder("<thead><tr><th>Stage</th><th>Records In Total</th>"
        + "<th>Records Out Total</th><th>Closed Minutes</th><th>Expected Closed Minutes</th>");
    if (columns.showInputWatermark()) {
      header.append("<th>Input Watermark</th>");
    }
    if (columns.showOutputWatermark()) {
      header.append("<th>Output Watermark</th>");
    }
    return header.append("<th>Health</th></tr></thead>").toString();
  }

  private static WindowDiagnosticColumns windowDiagnosticColumns(WindowMaterializationSnapshot windows) {
    return new WindowDiagnosticColumns(
        hasWindowInputWatermark(windows.chr()) || hasWindowInputWatermark(windows.pm())
            || hasWindowInputWatermark(windows.kpi()),
        hasWindowOutputWatermark(windows.chr()) || hasWindowOutputWatermark(windows.pm())
            || hasWindowOutputWatermark(windows.kpi()));
  }

  private static boolean hasWindowInputWatermark(WindowMaterializationSnapshot.WindowStage stage) {
    return stage.currentInputWatermarkMs() >= 0;
  }

  private static boolean hasWindowOutputWatermark(WindowMaterializationSnapshot.WindowStage stage) {
    return stage.currentOutputWatermarkMs() >= 0;
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

  private static String flinkResourcesTable(BenchmarkRunResult result) {
    FlinkSnapshot flink = result.flink();
    boolean trusted = operatorMetricsTrusted(result);
    return twoColumnTable(List.of(
        row("Job Status", flink.jobStatus()),
        row("TaskManagers", String.valueOf(flink.taskManagers())),
        row("Slots", String.valueOf(flink.slots())),
        row("Operator Metrics Validity", operatorMetricsValidity(result)),
        row("Sampled Avg Records In/s", trusted ? formatNumber(flink.recordsInPerSec()) : "N/A"),
        row("Sampled Avg Records Out/s", trusted ? formatNumber(flink.recordsOutPerSec()) : "N/A"),
        row("Operator Aggregate Records In Total", trusted ? formatNumber(flink.recordsInTotal()) : "N/A"),
        row("Operator Aggregate Records Out Total", trusted ? formatNumber(flink.recordsOutTotal()) : "N/A"),
        row("Backpressure", trusted ? formatRatio(flink.backpressureRatio()) : "N/A"),
        row("Checkpoint Duration", formatMs(flink.checkpointDurationMs())),
        row("Checkpoint Failures", String.valueOf(flink.consecutiveCheckpointFailures()))));
  }

  private static String operatorFlowAndMetrics(BenchmarkConfig config, BenchmarkRunResult result) {
    return """
        <div class="operator-section">
          %s
          %s
          %s
          %s
        </div>
        """.formatted(
            operatorFlowDiagram(config, result),
            operatorMetricCoverageTable(result),
            operatorMetricsTable(config, result),
            operatorMetricGuide());
  }

  private static String operatorMetricCoverageTable(BenchmarkRunResult result) {
    BenchmarkRunPlan plan = result.plan();
    FlinkSnapshot flink = result.flink();
    FdbMetricsSnapshot fdb = result.fdb();
    OperatorTableColumns columns = operatorTableColumns(plan, fdb, flink);
    int total = flink.operators().size();
    int operatorMetrics = Math.toIntExact(flink.operators().stream()
        .filter(FlinkOperatorSnapshot::metricsAvailable)
        .count());
    int inputWatermarks = Math.toIntExact(flink.operators().stream()
        .filter(operator -> operator.currentInputWatermarkMs() >= 0)
        .count());
    int outputWatermarks = Math.toIntExact(flink.operators().stream()
        .filter(operator -> operator.currentOutputWatermarkMs() >= 0)
        .count());
    int businessLatencies = Math.toIntExact(flink.operators().stream()
        .filter(operator -> operatorLatency(plan, fdb, operator).available())
        .count());
    int markerLatencies = Math.toIntExact(flink.operators().stream()
        .filter(operator -> operator.flinkMarkerP95Ms() >= 0 || !operator.flinkMarkerLatencies().isEmpty())
        .count());
    return """
        <table>
          <thead><tr><th>Metric Coverage</th><th>Operators</th><th>Main Table</th></tr></thead>
          <tbody>
            <tr><td>Flink Operator Metrics</td><td>%s</td><td>shown</td></tr>
            <tr><td>Input Watermark</td><td>%s</td><td>%s</td></tr>
            <tr><td>Output Watermark</td><td>%s</td><td>%s</td></tr>
            <tr><td>Business Latency Mapping</td><td>%s</td><td>shown</td></tr>
            <tr><td>Flink Marker P95</td><td>%s</td><td>%s</td></tr>
          </tbody>
        </table>
        """.formatted(
            coverage(operatorMetrics, total),
            coverage(inputWatermarks, total), coverageVisibility(columns.showInputWatermark(),
                "hidden (no valid input watermark exposed)"),
            coverage(outputWatermarks, total), coverageVisibility(columns.showOutputWatermark(),
                "hidden (no valid output watermark exposed)"),
            coverage(businessLatencies, total),
            coverage(markerLatencies, total), coverageVisibility(columns.showFlinkMarker(),
                "hidden (Flink REST did not expose marker percentiles)"));
  }

  private static String coverage(int count, int total) {
    return count + "/" + total + " operators";
  }

  private static String coverageVisibility(boolean shown, String hiddenReason) {
    return shown ? "shown" : hiddenReason;
  }

  private static String operatorMetricsTable(BenchmarkConfig config, BenchmarkRunResult result) {
    BenchmarkRunPlan plan = result.plan();
    BenchmarkThresholds thresholds = config.thresholds();
    FlinkSnapshot flink = result.flink();
    FdbMetricsSnapshot fdb = result.fdb();
    boolean trusted = operatorMetricsTrusted(result);
    OperatorTableColumns columns = operatorTableColumns(plan, fdb, flink);
    StringBuilder rows = new StringBuilder();
    if (flink.operators().isEmpty()) {
      rows.append("<tr><td>aggregate job</td><td>aggregate</td><td>-</td><td>")
          .append(formatNumber(flink.recordsInPerSec())).append("</td><td>")
          .append(formatNumber(flink.recordsOutPerSec())).append("</td><td>")
          .append(formatNumber(flink.recordsInTotal())).append("</td><td>")
          .append(formatNumber(flink.recordsOutTotal()))
          .append("</td><td>-</td><td>-</td><td>-</td><td>-</td>");
      appendOptionalWatermarkCells(rows, columns, "-", "-");
      rows.append("<td>").append(formatRatio(flink.backpressureRatio()))
          .append("</td><td>N/A</td><td>N/A</td><td>N/A</td><td>N/A</td>");
      if (columns.showFlinkMarker()) {
        rows.append("<td>N/A</td>");
      }
      rows.append("<td>NA</td><td>NA</td><td>")
          .append(healthBadge(GateHealth.NA)).append("</td></tr>");
    } else {
      for (FlinkOperatorSnapshot operator : flink.operators()) {
        OperatorLatency latency = operatorLatency(plan, fdb, operator);
        GateHealth health = operatorHealth(thresholds, operator, latency, trusted);
        rows.append("<tr id=\"operator-row-").append(htmlId(operator.id())).append("\" data-operator-id=\"")
            .append(escape(operator.id())).append("\"><td>").append(escape(operator.name())).append("</td><td>")
            .append(escape(operatorRole(operator))).append("</td><td>")
            .append(operator.parallelism()).append("</td><td>")
            .append(formatOperatorNumber(operator, operator.recordsInPerSec(), trusted)).append("</td><td>")
            .append(formatOperatorNumber(operator, operator.recordsOutPerSec(), trusted)).append("</td><td>")
            .append(formatOperatorNumber(operator, operator.recordsInTotal(), trusted)).append("</td><td>")
            .append(formatOperatorNumber(operator, operator.recordsOutTotal(), trusted)).append("</td><td>")
            .append(formatOperatorNumber(operator, operator.bytesInPerSec(), trusted)).append("</td><td>")
            .append(formatOperatorNumber(operator, operator.bytesOutPerSec(), trusted)).append("</td><td>")
            .append(formatOperatorRatio(operator, operator.busyRatio(), trusted)).append("</td><td>")
            .append(formatOperatorRatio(operator, operator.idleRatio(), trusted)).append("</td>");
        appendOptionalWatermarkCells(rows, columns,
            escape(formatOperatorWatermark(operator, operator.currentInputWatermarkMs(), trusted)),
            escape(formatOperatorWatermark(operator, operator.currentOutputWatermarkMs(), trusted)));
        rows.append("<td>")
            .append(formatOperatorRatio(operator, operator.backpressureRatio(), trusted)).append("</td>");
        if (latency.available()) {
          rows.append("<td>")
              .append(formatMs(latency.p50Ms())).append("</td><td>")
              .append(formatMs(latency.p95Ms())).append("</td><td>")
              .append(formatMs(latency.p99Ms())).append("</td><td>")
              .append(formatMs(latency.watermarkLagMs())).append("</td>");
        } else {
          rows.append("<td class=\"muted\" colspan=\"4\">unmapped business probe</td>");
        }
        if (columns.showFlinkMarker()) {
          rows.append("<td>").append(formatMs(operator.flinkMarkerP95Ms())).append("</td>");
        }
        if (latency.available()) {
          rows.append("<td>").append(escape(latency.source())).append("</td><td>")
              .append(escape(latency.scope())).append("</td><td>");
        } else {
          rows.append("<td class=\"muted\">unmapped</td><td class=\"muted\">unmapped</td><td>");
        }
        rows.append(healthBadge(health)).append("</td></tr>");
        rows.append(operatorMarkerRows(thresholds, operator, columns));
      }
    }
    return """
        <table>
          %s
          <tbody>%s</tbody>
        </table>
        """.formatted(operatorMetricsHeader(columns), rows);
  }

  private static String operatorMetricsHeader(OperatorTableColumns columns) {
    StringBuilder header = new StringBuilder(
        "<thead><tr><th>Operator</th><th>Role</th><th>Parallelism</th>"
            + "<th>Sampled Avg Records In/s</th><th>Sampled Avg Records Out/s</th>"
            + "<th>Records In Total</th><th>Records Out Total</th><th>Bytes In/s</th><th>Bytes Out/s</th>"
            + "<th>Busy</th><th>Idle</th>");
    if (columns.showInputWatermark()) {
      header.append("<th>Input Watermark</th>");
    }
    if (columns.showOutputWatermark()) {
      header.append("<th>Output Watermark</th>");
    }
    header.append("<th>Backpressure</th><th>Cumulative Business Age P50</th>"
        + "<th>Cumulative Business Age P95</th><th>Cumulative Business Age P99</th><th>Watermark Lag</th>");
    if (columns.showFlinkMarker()) {
      header.append("<th>Flink Marker P95</th>");
    }
    return header.append("<th>Business Probe Source</th><th>Latency Scope</th><th>Health</th></tr></thead>").toString();
  }

  private static OperatorTableColumns operatorTableColumns(BenchmarkRunPlan plan, FdbMetricsSnapshot fdb,
      FlinkSnapshot flink) {
    return new OperatorTableColumns(
        flink.operators().stream().anyMatch(operator -> operator.currentInputWatermarkMs() >= 0),
        flink.operators().stream().anyMatch(operator -> operator.currentOutputWatermarkMs() >= 0),
        flink.operators().stream()
            .anyMatch(operator -> operator.flinkMarkerP95Ms() >= 0 || !operator.flinkMarkerLatencies().isEmpty()));
  }

  private static void appendOptionalWatermarkCells(StringBuilder rows, OperatorTableColumns columns,
      String inputValue, String outputValue) {
    if (columns.showInputWatermark()) {
      rows.append("<td>").append(inputValue).append("</td>");
    }
    if (columns.showOutputWatermark()) {
      rows.append("<td>").append(outputValue).append("</td>");
    }
  }

  private static String operatorMarkerRows(BenchmarkThresholds thresholds, FlinkOperatorSnapshot operator,
      OperatorTableColumns columns) {
    StringBuilder rows = new StringBuilder();
    for (FlinkMarkerLatencySnapshot marker : operator.flinkMarkerLatencies()) {
      GateHealth health = markerHealth(thresholds, marker);
      rows.append("<tr class=\"operator-marker-row\" data-operator-id=\"")
          .append(escape(operator.id())).append("\"><td>")
          .append("marker: ").append(escape(marker.sourceOperatorId())).append(" -> ")
          .append(escape(marker.targetOperatorId())).append("</td><td>marker</td><td>-</td>")
          .append("<td>-</td><td>-</td><td>-</td><td>-</td><td>-</td><td>-</td>")
          .append("<td>-</td><td>-</td>");
      appendOptionalWatermarkCells(rows, columns, "-", "-");
      rows.append("<td>-</td><td>N/A</td><td>N/A</td><td>N/A</td><td>N/A</td>");
      if (columns.showFlinkMarker()) {
        rows.append("<td>").append(formatMs(marker.p95Ms())).append("</td>");
      }
      rows.append("<td>Flink marker</td><td>transport</td><td>")
          .append(healthBadge(health)).append("</td></tr>");
    }
    return rows.toString();
  }

  private static OperatorLatency operatorLatency(BenchmarkRunPlan plan, FdbMetricsSnapshot fdb,
      FlinkOperatorSnapshot operator) {
    boolean sinkOperator = "sink".equals(operatorRole(operator));
    if (sinkOperator) {
      OperatorLatency sinkLatency = sinkOperatorLatency(plan, fdb, operator);
      if (sinkLatency.available()) {
        return sinkLatency;
      }
    }
    for (StageLatencySnapshot stage : fdb.stageLatencies()) {
      if (isRelevantStage(plan.sink(), stage.stageId()) && metricMatchesOperator(operator, stage.stageId())) {
        return new OperatorLatency(stage.latencyP50Ms(), stage.latencyP95Ms(), stage.latencyP99Ms(),
            stage.watermarkLagMs(), "stage:" + stage.stageId(), "output-age");
      }
    }
    if (!sinkOperator) {
      OperatorLatency sinkLatency = sinkOperatorLatency(plan, fdb, operator);
      if (sinkLatency.available()) {
        return sinkLatency;
      }
    }
    return OperatorLatency.empty();
  }

  private static OperatorLatency sinkOperatorLatency(BenchmarkRunPlan plan, FdbMetricsSnapshot fdb,
      FlinkOperatorSnapshot operator) {
    List<SinkLatencySnapshot> orderedSinks = fdb.sinkLatencies().stream()
        .sorted(Comparator.comparingInt((SinkLatencySnapshot sink) -> sinkOperatorLatencyRank(sink.scope()))
            .thenComparing(SinkLatencySnapshot::sinkName))
        .toList();
    for (SinkLatencySnapshot sink : orderedSinks) {
      if ((isCurrentBusinessSink(plan.sink(), sink.sinkName()) || isAuxiliarySink(sink.sinkName()))
          && metricMatchesOperator(operator, sinkBaseName(sink.sinkName()))) {
        return new OperatorLatency(sink.latencyP50Ms(), sink.latencyP95Ms(), sink.latencyP99Ms(),
            -1L, "sink:" + sink.sinkName(), sink.scope());
      }
    }
    return OperatorLatency.empty();
  }

  private static int sinkOperatorLatencyRank(String scope) {
    return switch (lower(scope)) {
      case "connector-commit" -> 0;
      case "connector-write" -> 1;
      case "sink-front" -> 2;
      default -> 3;
    };
  }

  private static String sinkBaseName(String sinkName) {
    String value = sinkName == null ? "" : sinkName;
    if (lower(value).endsWith(".connector-write")) {
      return value.substring(0, value.length() - ".connector-write".length());
    }
    if (lower(value).endsWith(".connector-commit")) {
      return value.substring(0, value.length() - ".connector-commit".length());
    }
    return value;
  }

  private static boolean metricMatchesOperator(FlinkOperatorSnapshot operator, String metricId) {
    String haystack = lower(operator.id() + " " + operator.name());
    for (String alias : metricAliases(metricId)) {
      if (!alias.isBlank() && haystack.contains(alias)) {
        return true;
      }
    }
    return false;
  }

  private static List<String> metricAliases(String metricId) {
    String value = lower(metricId);
    if (value.isBlank()) {
      return List.of();
    }
    return List.of(
        value,
        value.replace(".connector-write", ""),
        value.replace(".connector-commit", ""),
        value.replace("window-", ""),
        value.replace("starrocks-", ""),
        value.replace("iceberg-", ""),
        value.replace("hive-", ""),
        value.replace("kafka-", ""));
  }

  private static String operatorRole(FlinkOperatorSnapshot operator) {
    String name = lower(operator.name());
    if (name.contains("source")) {
      return "source";
    }
    if (name.contains("sink") || name.contains("iceberg") || name.contains("starrocks")
        || name.contains("hive") || name.contains("kafka")) {
      return "sink";
    }
    if (name.contains("enrich")) {
      return "enrich";
    }
    if (name.contains("anomaly") || name.contains("coverage")) {
      return "anomaly";
    }
    if (name.contains("kpi")) {
      return "kpi";
    }
    if (name.contains("chr") || name.contains("pm-") || name.contains("cfg")) {
      return "stage";
    }
    return "operator";
  }

  private static GateHealth operatorHealth(BenchmarkThresholds thresholds, FlinkOperatorSnapshot operator,
      OperatorLatency latency, boolean trusted) {
    long businessThreshold = sinkLatencyScope(latency.scope())
        ? thresholds.maxSinkP95Ms()
        : thresholds.maxKpiAvailabilityP95Ms();
    if (latency.p95Ms() >= 0 && latency.p95Ms() > businessThreshold) {
      return GateHealth.UNSTABLE;
    }
    if (operator.flinkMarkerP95Ms() >= 0 && operator.flinkMarkerP95Ms() > thresholds.maxWatermarkLagMs()) {
      return GateHealth.UNSTABLE;
    }
    if (trusted && operator.metricsAvailable() && operator.backpressureRatio() > thresholds.maxBackpressureRatio()) {
      return GateHealth.UNSTABLE;
    }
    if (!latency.available() && operator.flinkMarkerP95Ms() < 0 && (!trusted || !operator.metricsAvailable())) {
      return GateHealth.NA;
    }
    return GateHealth.HEALTHY;
  }

  private static GateHealth operatorGraphHealth(BenchmarkThresholds thresholds, FlinkOperatorSnapshot operator,
      OperatorLatency latency, boolean trusted) {
    if (!latency.available()
        && (!trusted || !operator.metricsAvailable()
            || operator.backpressureRatio() <= thresholds.maxBackpressureRatio())) {
      return GateHealth.NA;
    }
    long businessThreshold = sinkLatencyScope(latency.scope())
        ? thresholds.maxSinkP95Ms()
        : thresholds.maxKpiAvailabilityP95Ms();
    if (latency.p95Ms() >= 0 && latency.p95Ms() > businessThreshold) {
      return GateHealth.UNSTABLE;
    }
    if (trusted && operator.metricsAvailable() && operator.backpressureRatio() > thresholds.maxBackpressureRatio()) {
      return GateHealth.UNSTABLE;
    }
    return GateHealth.HEALTHY;
  }

  private static GateHealth markerHealth(BenchmarkThresholds thresholds, FlinkMarkerLatencySnapshot marker) {
    return marker.p95Ms() > thresholds.maxWatermarkLagMs() ? GateHealth.UNSTABLE : GateHealth.HEALTHY;
  }

  private static boolean sinkLatencyScope(String scope) {
    String value = lower(scope);
    return "sink-front".equals(value) || "connector-write".equals(value) || "connector-commit".equals(value);
  }

  private static String flowNodeClass(GateHealth health) {
    return switch (health) {
      case HEALTHY -> "flow-node-ok";
      case UNSTABLE -> "flow-node-warn";
      case FAILED -> "flow-node-fail";
      case NA -> "flow-node-na";
    };
  }

  private static String flowEdgeMarkerClass(GateHealth health) {
    return health == GateHealth.UNSTABLE ? "flow-edge-marker-warn" : "flow-edge-marker-ok";
  }

  private static String operatorNodeLatencyLabel(FlinkOperatorSnapshot operator, OperatorLatency latency) {
    if (latency.p95Ms() >= 0) {
      return "P95 " + formatMs(latency.p95Ms());
    }
    return "no probe";
  }

  private static String operatorNodeTitle(FlinkOperatorSnapshot operator, OperatorLatency latency,
      GateHealth health) {
    String title = operator.name()
        + " | " + health.label()
        + " | " + operatorNodeLatencyLabel(operator, latency);
    if (!latency.available()) {
      return title + " | no mapped business probe";
    }
    return title
        + " | source " + latency.source()
        + " | scope " + latency.scope();
  }

  private static FlinkMarkerLatencySnapshot edgeMarkerLatency(FlinkOperatorSnapshot targetOperator,
      FlinkOperatorEdge edge) {
    if (targetOperator == null) {
      return null;
    }
    FlinkMarkerLatencySnapshot best = null;
    for (FlinkMarkerLatencySnapshot marker : targetOperator.flinkMarkerLatencies()) {
      if (edge.sourceId().equals(marker.sourceOperatorId()) && edge.targetId().equals(marker.targetOperatorId())
          && marker.p95Ms() >= 0
          && (best == null || marker.p95Ms() > best.p95Ms())) {
        best = marker;
      }
    }
    return best;
  }

  private static String operatorMetricGuide() {
    return """
        <div class="column-guide">
          <h3>Column Guide</h3>
          <table>
            <thead><tr><th>Column</th><th>Meaning</th></tr></thead>
            <tbody>
              <tr><td>Cumulative Business Age P50/P95/P99</td><td>Custom stage or sink-front age mapped to this operator row. It is based on business event/window time and is cumulative to the probe point, not the operator's own processing time.</td></tr>
              <tr><td>Watermark Lag</td><td>Custom stage watermark lag from the same mapped stage metric. N/A means no stage metric is mapped to this operator.</td></tr>
              <tr><td>Flink Marker P95</td><td>Flink latency marker P95 observed on this downstream operator. Marker detail rows show each source-to-target input when Flink exposes it. N/A means latency marker unavailable because the Flink REST metrics for this job/operator did not expose marker percentiles.</td></tr>
              <tr><td>Flow Node Color</td><td>Green means mapped business age/backpressure is within threshold, orange means business age/backpressure is over threshold, grey means no mapped business age sample is available for that operator.</td></tr>
              <tr><td>Flow Edge Marker</td><td>Edge labels show Flink marker P95 for the source-to-target edge. NA marker values are not rendered.</td></tr>
              <tr><td>Business Probe Source</td><td>The custom metric matched to the operator row, such as stage:kpi-1m, sink:iceberg-kpi-1m, or NA.</td></tr>
              <tr><td>Latency Scope</td><td>output-age is cumulative business age to the operator probe. sink-front is before connector write. connector-write is connector writer/invoke duration. connector-commit is flush/prepare/commit/checkpoint duration and does not include connector-write. NA means no mapped latency sample.</td></tr>
            </tbody>
          </table>
          <h3>Abnormal Value Analysis</h3>
          <table>
            <thead><tr><th>Pattern</th><th>Likely Cause</th><th>Next Check</th></tr></thead>
            <tbody>
              <tr><td>Cumulative Business Age P95 high while Flink Marker P95 is normal</td><td>Window waiting, event-time skew, join wait, or business timestamp baseline is dominating.</td><td>Check Watermark Lag, 1m Window Diagnostics, and stage source mapping.</td></tr>
              <tr><td>Flink Marker P95 high</td><td>Source-to-operator queueing, network transfer, shuffle, or backpressure before this operator.</td><td>Check upstream Records Out/s, downstream Busy, and Backpressure.</td></tr>
              <tr><td>Backpressure high with low Records Out/s</td><td>The operator or downstream sink cannot drain at the produced rate.</td><td>Increase targeted parallelism or inspect sink/storage health.</td></tr>
              <tr><td>Connector Commit P95 high while Connector Write P95 is normal</td><td>Connector commit/checkpoint/storage visibility is dominating, not per-record write.</td><td>Check checkpoint interval, committer parallelism, storage metadata pressure, and connector commit logs.</td></tr>
              <tr><td>Business Probe Source is NA</td><td>No custom stage or sink metric can be mapped to this Flink operator yet.</td><td>Add a lightweight StageMetricsProbe only if this operator is important for pressure analysis.</td></tr>
              <tr><td>Flink operator metrics are N/A</td><td>Flink REST did not expose usable operator metric values, or source records were published while all operator metrics stayed zero.</td><td>Check /jobs/{jobId}/vertices/{vertexId}/metrics while the job is actively processing.</td></tr>
            </tbody>
          </table>
        </div>
        """;
  }

  private record OperatorLatency(long p50Ms, long p95Ms, long p99Ms, long watermarkLagMs, String source,
                                 String scope) {
    static OperatorLatency empty() {
      return new OperatorLatency(-1L, -1L, -1L, -1L, "NA", "NA");
    }

    boolean available() {
      return p50Ms >= 0 || p95Ms >= 0 || p99Ms >= 0 || watermarkLagMs >= 0;
    }
  }

  private record OperatorTableColumns(boolean showInputWatermark, boolean showOutputWatermark,
                                      boolean showFlinkMarker) {
  }

  private record WindowDiagnosticColumns(boolean showInputWatermark, boolean showOutputWatermark) {
  }

  private static String operatorFlowDiagram(BenchmarkConfig config, BenchmarkRunResult result) {
    BenchmarkRunPlan plan = result.plan();
    BenchmarkThresholds thresholds = config.thresholds();
    FdbMetricsSnapshot fdb = result.fdb();
    FlinkSnapshot flink = result.flink();
    boolean trusted = operatorMetricsTrusted(result);
    if (flink.operators().isEmpty()) {
      return "<p>No operator graph was captured for this run.</p>";
    }
    Map<String, Integer> levels = operatorLevels(flink.operators(), flink.operatorEdges());
    Map<String, FlowPosition> positions = operatorPositions(flink.operators(), levels);
    Map<String, FlinkOperatorSnapshot> operatorsById = operatorsById(flink.operators());
    int maxLevel = levels.values().stream().max(Integer::compareTo).orElse(0);
    Map<Integer, Long> perLevel = levels.values().stream()
        .collect(Collectors.groupingBy(level -> level, Collectors.counting()));
    long maxRows = perLevel.values().stream().max(Long::compareTo).orElse(1L);
    int width = Math.max(760, 260 + (maxLevel * 260));
    int height = Math.max(190, 110 + ((int) maxRows * 130));

    StringBuilder edges = new StringBuilder();
    for (FlinkOperatorEdge edge : flink.operatorEdges()) {
      FlowPosition source = positions.get(edge.sourceId());
      FlowPosition target = positions.get(edge.targetId());
      if (source == null || target == null) {
        continue;
      }
      FlinkMarkerLatencySnapshot marker = edgeMarkerLatency(operatorsById.get(edge.targetId()), edge);
      String edgeClass = marker == null ? "" : " " + flowEdgeMarkerClass(markerHealth(thresholds, marker));
      int x1 = source.x() + 190;
      int y1 = source.y() + 43;
      int x2 = target.x() - 10;
      int y2 = target.y() + 43;
      edges.append("<line class=\"flow-edge").append(edgeClass).append("\" data-source-id=\"")
          .append(escape(edge.sourceId()))
          .append("\" data-target-id=\"").append(escape(edge.targetId()))
          .append("\" x1=\"").append(x1).append("\" y1=\"").append(y1)
          .append("\" x2=\"").append(x2).append("\" y2=\"").append(y2)
          .append("\" marker-end=\"url(#arrow)\"></line>");
      if (marker != null) {
        GateHealth markerHealth = markerHealth(thresholds, marker);
        edges.append("<text class=\"flow-edge-marker-label")
            .append(markerHealth == GateHealth.UNSTABLE ? " flow-edge-marker-label-warn" : "")
            .append("\" x=\"").append((x1 + x2) / 2)
            .append("\" y=\"").append(((y1 + y2) / 2) - 8)
            .append("\">P95 ").append(escape(formatMs(marker.p95Ms()))).append("</text>");
      }
    }

    StringBuilder nodes = new StringBuilder();
    for (FlinkOperatorSnapshot operator : flink.operators()) {
      FlowPosition position = positions.get(operator.id());
      OperatorLatency latency = operatorLatency(plan, fdb, operator);
      GateHealth health = operatorGraphHealth(thresholds, operator, latency, trusted);
      nodes.append("<g class=\"flow-node ").append(flowNodeClass(health))
          .append("\" data-operator-id=\"").append(escape(operator.id()))
          .append("\" data-latency-health=\"").append(escape(health.label()))
          .append("\" transform=\"translate(").append(position.x()).append(",").append(position.y()).append(")\">")
          .append("<title>").append(escape(operatorNodeTitle(operator, latency, health))).append("</title>")
          .append("<rect width=\"180\" height=\"86\" rx=\"4\"></rect>")
          .append("<text x=\"10\" y=\"22\">").append(escape(shortLabel(operator.name(), 28))).append("</text>")
          .append("<text x=\"10\" y=\"43\">p=").append(operator.parallelism())
          .append(" out/s=").append(escape(formatOperatorNumber(operator, operator.recordsOutPerSec(), trusted)))
          .append("</text>")
          .append("<text class=\"flow-latency\" x=\"10\" y=\"62\">")
          .append(escape(operatorNodeLatencyLabel(operator, latency))).append("</text>")
          .append("<text x=\"10\" y=\"79\">bp=")
          .append(escape(formatOperatorRatio(operator, operator.backpressureRatio(), trusted)))
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
      positions.put(operator.id(), new FlowPosition(40 + level * 260, 30 + index * 130));
    }
    return positions;
  }

  private static Map<String, FlinkOperatorSnapshot> operatorsById(List<FlinkOperatorSnapshot> operators) {
    Map<String, FlinkOperatorSnapshot> values = new LinkedHashMap<>();
    for (FlinkOperatorSnapshot operator : operators) {
      values.put(operator.id(), operator);
    }
    return values;
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
            <tr><td>Sink-front Age</td><td>%s</td><td>Age before connector write; not connector-internal write duration</td></tr>
            <tr><td>Connector Write</td><td>%s</td><td>Connector writer/invoke duration only</td></tr>
            <tr><td>Connector Commit</td><td>%s</td><td>Connector flush/prepare/commit/checkpoint duration only</td></tr>
            <tr><td>Watermark Lag</td><td>%s</td><td>Maximum observed stage watermark lag</td></tr>
          </tbody>
        </table>
        """.formatted(
            formatMs(fdb.sourceDelayP95Ms()),
            formatMs(fdb.kpi1mP95Ms()),
            formatMs(fdb.kpi5mP95Ms()),
            formatMs(fdb.sinkP95Ms()),
            formatMs(fdb.connectorWriteP95Ms()),
            formatMs(fdb.connectorCommitP95Ms()),
            formatMs(fdb.watermarkLagMs()));
  }

  private static String storageTable(BenchmarkRunPlan plan, FdbMetricsSnapshot fdb, StorageSnapshot storage) {
    List<SinkLatencySnapshot> currentSinks = fdb.sinkLatencies().stream()
        .filter(sink -> isCurrentBusinessSink(plan.sink(), sink.sinkName()))
        .sorted(Comparator.comparingInt((SinkLatencySnapshot sink) -> sinkScopeRank(sink.scope()))
            .thenComparing(SinkLatencySnapshot::sinkName))
        .toList();
    List<SinkLatencySnapshot> auxiliarySinks = fdb.sinkLatencies().stream()
        .filter(sink -> !isCurrentBusinessSink(plan.sink(), sink.sinkName()))
        .filter(sink -> isAuxiliarySink(sink.sinkName()))
        .sorted(Comparator.comparingInt((SinkLatencySnapshot sink) -> sinkScopeRank(sink.scope()))
            .thenComparing(SinkLatencySnapshot::sinkName))
        .toList();
    if (!currentSinks.isEmpty() || !auxiliarySinks.isEmpty()) {
      StringBuilder rows = new StringBuilder();
      for (SinkLatencySnapshot sink : currentSinks) {
        appendSinkRow(rows, "Current", sink);
      }
      if (currentSinks.isEmpty()) {
        rows.append("<tr><td>Current</td><td colspan=\"9\">No current business sink latency samples</td></tr>");
      }
      for (SinkLatencySnapshot sink : auxiliarySinks) {
        appendSinkRow(rows, "Auxiliary", sink);
      }
      rows.append("<tr><td>Storage Health</td><td>storage</td><td colspan=\"3\">")
          .append(storage.healthy() ? "healthy" : "unhealthy").append("</td><td colspan=\"5\">")
          .append(escape(storage.summary())).append("</td></tr>");
      appendStorageProbeRow(rows, plan.sink(), storage);
      return """
          <table>
            <thead><tr><th>Type</th><th>Scope</th><th>Sink</th><th>Records</th><th>Bytes</th><th>P50</th><th>P95</th><th>P99</th><th>Failures</th><th>Latency Confidence</th></tr></thead>
            <tbody>%s</tbody>
          </table>
          """.formatted(rows);
    }
    return """
        <table>
          <thead><tr><th>Area</th><th>Value</th><th>Status</th></tr></thead>
          <tbody>
            <tr><td>Sink P95</td><td>%s</td><td>%s failures</td></tr>
            <tr><td>Connector Write P95</td><td>%s</td><td>connector writer/invoke duration</td></tr>
            <tr><td>Connector Commit P95</td><td>%s</td><td>connector flush/prepare/commit/checkpoint duration</td></tr>
            <tr><td>Storage Health</td><td>%s</td><td>%s</td></tr>
            <tr><td>Storage Records</td><td>%s</td><td>Small files %s, in-progress %s</td></tr>
          </tbody>
        </table>
        """.formatted(
            formatMs(fdb.sinkP95Ms()),
            fdb.sinkFailures(),
            formatMs(fdb.connectorWriteP95Ms()),
            formatMs(fdb.connectorCommitP95Ms()),
            storage.healthy() ? "healthy" : "unhealthy",
            escape(storage.summary()),
            storage.records(),
            storage.smallFiles(),
            storage.inProgressFiles());
  }

  private static void appendStorageProbeRow(StringBuilder rows, BenchmarkSink sink, StorageSnapshot storage) {
    if (sink == BenchmarkSink.STARROCKS) {
      rows.append("<tr><td>Storage Rows</td><td>storage</td><td>starrocks</td><td>")
          .append(formatNumber(storage.records()))
          .append("</td><td>-</td><td colspan=\"5\">")
          .append(escape(storage.summary())).append("</td></tr>");
      return;
    }
    rows.append("<tr><td>Storage Files</td><td>storage</td><td>").append(escape(sink.value())).append("</td><td>")
        .append(formatNumber(storage.records()))
        .append("</td><td>-</td><td colspan=\"5\">Small files ")
        .append(storage.smallFiles()).append(", in-progress ").append(storage.inProgressFiles()).append("</td></tr>");
  }

  private static void appendSinkRow(StringBuilder rows, String type, SinkLatencySnapshot sink) {
    rows.append("<tr><td>").append(escape(type)).append("</td><td>").append(escape(sink.scope())).append("</td><td>")
        .append(escape(sink.sinkName())).append("</td><td>")
        .append(sink.records()).append("</td><td>")
        .append(sink.bytes()).append("</td><td>")
        .append(formatMs(sink.latencyP50Ms())).append("</td><td>")
        .append(formatMs(sink.latencyP95Ms())).append("</td><td>")
        .append(formatMs(sink.latencyP99Ms())).append("</td><td>")
        .append(sink.failures()).append("</td><td>")
        .append(escape(latencyConfidence(sink.records()))).append("</td></tr>");
  }

  private static int sinkScopeRank(String scope) {
    return switch (lower(scope)) {
      case "sink-front" -> 0;
      case "connector-write" -> 1;
      case "connector-commit" -> 2;
      default -> 3;
    };
  }

  private static String latencyConfidence(long records) {
    if (records <= 0) {
      return "N/A";
    }
    if (records < 30) {
      return "low (<30 records)";
    }
    if (records < 100) {
      return "medium (<100 records)";
    }
    return "high";
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

  private static boolean operatorMetricsTrusted(BenchmarkRunResult result) {
    FlinkSnapshot flink = result.flink();
    SourceMetricsSnapshot source = result.source();
    if (flink.operators().isEmpty()) {
      return hasAggregateMetricSignal(flink) || !hasPublishedSourceRecords(source);
    }
    if (!flink.hasOperatorMetrics()) {
      return false;
    }
    return !hasPublishedSourceRecords(source) || hasOperatorMetricSignal(flink);
  }

  private static String operatorMetricsValidity(BenchmarkRunResult result) {
    if (operatorMetricsTrusted(result)) {
      return "available";
    }
    if (!result.flink().hasOperatorMetrics()) {
      return "unavailable (Flink REST returned no operator metric values)";
    }
    if (hasPublishedSourceRecords(result.source()) && !hasOperatorMetricSignal(result.flink())) {
      return "unavailable (source published records but Flink operator metrics are all zero)";
    }
    return "unavailable";
  }

  private static boolean hasPublishedSourceRecords(SourceMetricsSnapshot source) {
    return source != null && (source.chrPublished() > 0 || source.pmPublished() > 0 || source.cfgPublished() > 0);
  }

  private static boolean hasAggregateMetricSignal(FlinkSnapshot flink) {
    return positive(flink.recordsInPerSec())
        || positive(flink.recordsOutPerSec())
        || positive(flink.recordsInTotal())
        || positive(flink.recordsOutTotal())
        || positive(flink.backpressureRatio());
  }

  private static boolean hasOperatorMetricSignal(FlinkSnapshot flink) {
    if (hasAggregateMetricSignal(flink)) {
      return true;
    }
    return flink.operators().stream()
        .filter(FlinkOperatorSnapshot::metricsAvailable)
        .anyMatch(operator -> positive(operator.recordsInPerSec())
            || positive(operator.recordsOutPerSec())
            || positive(operator.recordsInTotal())
            || positive(operator.recordsOutTotal())
            || positive(operator.bytesInPerSec())
            || positive(operator.bytesOutPerSec())
            || positive(operator.busyRatio())
            || positive(operator.backpressureRatio())
            || operator.currentInputWatermarkMs() >= 0
            || operator.currentOutputWatermarkMs() >= 0
            || operator.pendingRecords() > 0);
  }

  private static boolean positive(double value) {
    return Double.isFinite(value) && value > 0.0d;
  }

  private static String formatFlinkMetric(BenchmarkRunResult result, double value) {
    return operatorMetricsTrusted(result) ? formatNumber(value) : "N/A";
  }

  private static String formatOperatorNumber(FlinkOperatorSnapshot operator, double value, boolean trusted) {
    return trusted && operator.metricsAvailable() ? formatNumber(value) : "N/A";
  }

  private static String formatOperatorRatio(FlinkOperatorSnapshot operator, double value, boolean trusted) {
    return trusted && operator.metricsAvailable() ? formatRatio(value) : "N/A";
  }

  private static String formatOperatorWatermark(FlinkOperatorSnapshot operator, long value, boolean trusted) {
    return trusted && operator.metricsAvailable() ? formatWatermark(value) : "N/A";
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

  private static String formatWatermark(long value) {
    return value < 0 ? "N/A" : Instant.ofEpochMilli(value).toString();
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

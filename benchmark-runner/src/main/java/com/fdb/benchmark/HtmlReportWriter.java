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
          .append("/report.html\">单轮报告</a></td>")
          .append("</tr>\n");
    }

    return """
        <!doctype html>
        <html lang="zh-CN">
        <head>
          <meta charset="utf-8">
          <meta name="viewport" content="width=device-width, initial-scale=1">
          <title>Sink 上限压测</title>
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
            <h1>Sink 上限压测</h1>
            <div class="meta">压测: %s | 目标: %s | 预热: %ss | 观测: %ss</div>
            <div class="cards">
              %s
              %s
              %s
              %s
            </div>
            <h2>稳定上限</h2>
            %s
            <h2>运行明细</h2>
            <table>
              <thead>
                <tr><th>Sink</th><th>小区数</th><th>CHR EPS/小区/s</th><th>全局 CHR EPS</th><th>状态</th><th>采样输出 EPS</th><th>KPI 1m P95</th><th>Sink P95</th><th>Connector 提交 P95</th><th>Checkpoint</th><th>反压</th><th>Watermark 滞后</th><th>存储</th><th>原因</th><th>报告</th></tr>
              </thead>
              <tbody>
        """.formatted(escape(config.benchmarkId()), escape(config.target()), config.warmupSec(), config.durationSec(),
            metricCard("运行轮次", String.valueOf(results.size()), "已尝试的全部档位"),
            metricCard("稳定轮次", String.valueOf(countStatus(results, BenchmarkStatus.STABLE)),
                "观测期内满足门限"),
            metricCard("不稳定/失败", String.valueOf(countNonStable(results)), "压测停止或瓶颈档位"),
            metricCard("最佳稳定上限", bestStableBound(results), "最高稳定小区档位"),
            stableBounds(results))
        + rows
        + """
              </tbody>
            </table>
            <h2>建议</h2>
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
        <html lang="zh-CN">
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
            .operator-section { display: flex; flex-direction: column; gap: 12px; }
            .column-guide { background: #fff; border: 1px solid #d9dee7; padding: 12px; }
            .column-guide h3 { font-size: 15px; margin: 0 0 8px; }
            .column-guide table { margin-top: 8px; }
            a { color: #1d4ed8; }
          </style>
        </head>
        <body>
          <main>
            <a href="../../index.html">返回压测汇总</a>
            <h1>%s</h1>
            <p class="meta">压测 %s | 目标 %s | Sink %s | 小区数 %s | CHR EPS %s 条/小区/s</p>
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
            <h2>运行摘要</h2>
            %s
            <h2>稳定性门禁</h2>
            %s
            <h2>Source 密度</h2>
            %s
            <h2>1m 窗口诊断</h2>
            %s
            <h2>运行说明</h2>
            %s
            <h2>拓扑生成</h2>
            %s
            <h2>Flink 资源</h2>
            %s
            <h2>算子流程与指标</h2>
            %s
            <h2>Sink 与存储</h2>
            %s
            <h2>原始文件</h2>
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
            metricCard("状态", result.status().name(), result.bottleneckReason()),
            metricCard("采样输出 EPS", formatFlinkMetric(result, flink.recordsOutPerSec()),
                "Flink 采样窗口平均 records/s"),
            metricCard("Checkpoint", formatMs(flink.checkpointDurationMs()),
                "失败 " + flink.consecutiveCheckpointFailures() + " 次"),
            metricCard("反压", formatRatio(flink.backpressureRatio()), "Job 级比例"),
            metricCard("KPI 1m P95", formatMs(fdb.kpi1mP95Ms()), "KPI 可用延迟"),
            metricCard("Sink P95", formatMs(fdb.sinkP95Ms()), "失败 " + fdb.sinkFailures() + " 次"),
            metricCard("Connector 写入 P95", formatMs(fdb.connectorWriteP95Ms()), "connector writer/invoke 耗时"),
            metricCard("Connector 提交 P95", formatMs(fdb.connectorCommitP95Ms()), "flush、prepare、commit、checkpoint 耗时"),
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
        gate("Flink 作业状态", flink.jobStatus(), "RUNNING",
            "RUNNING".equalsIgnoreCase(flink.jobStatus()) ? GateHealth.HEALTHY : GateHealth.FAILED),
        gate("Source 指标", source.present() ? "存在" : "缺失", "存在",
            source.present() ? GateHealth.HEALTHY : GateHealth.UNSTABLE),
        gate("CHR Source 指标", source.hasChrMetrics() ? "存在" : "缺失", "存在",
            source.hasChrMetrics() ? GateHealth.HEALTHY : GateHealth.UNSTABLE),
        gate("算子指标有效性", operatorMetricsTrusted ? "可用" : operatorMetricsValidity(result),
            "Source 已发布记录时可用",
            operatorMetricsTrusted ? GateHealth.HEALTHY : GateHealth.NA),
        gate("Source 吞吐达成率", source.hasChrMetrics() ? formatRatio(source.producerDeliveryRatio()) : "N/A",
            ">= " + formatRatio(thresholds.minProducerDeliveryRatio()),
            source.hasChrMetrics()
                ? health(source.producerDeliveryRatio() >= thresholds.minProducerDeliveryRatio())
                : GateHealth.NA),
        gate("1m 窗口物化", windows.observedClosedMinutes(), windows.thresholdText(),
            windows.applicable() ? health(windows.healthy()) : GateHealth.NA),
        gate("Source 算子积压", operatorMetricsTrusted ? flink.sourceBacklogRecords() + " 条" : "N/A",
            "<= " + thresholds.maxSourceBacklogRecords() + " 条",
            operatorMetricsTrusted
                ? health(flink.sourceBacklogRecords() <= thresholds.maxSourceBacklogRecords())
                : GateHealth.NA),
        gate("反压比例", operatorMetricsTrusted ? formatRatio(flink.backpressureRatio()) : "N/A",
            "<= " + formatRatio(thresholds.maxBackpressureRatio()),
            operatorMetricsTrusted ? health(flink.backpressureRatio() <= thresholds.maxBackpressureRatio()) : GateHealth.NA),
        gate("连续 Checkpoint 失败", flink.consecutiveCheckpointFailures() + " 次",
            "< " + thresholds.maxConsecutiveCheckpointFailures() + " 次",
            health(flink.consecutiveCheckpointFailures() < thresholds.maxConsecutiveCheckpointFailures())),
        gate("Checkpoint 耗时", formatMs(flink.checkpointDurationMs()),
            "<= " + formatMs(thresholds.maxCheckpointDurationMs()),
            health(flink.checkpointDurationMs() <= thresholds.maxCheckpointDurationMs())),
        gate("KPI 可用延迟 P95", formatMs(kpiAvailabilityP95Ms),
            "<= " + formatMs(thresholds.maxKpiAvailabilityP95Ms()),
            kpiAvailabilityP95Ms >= 0
                ? health(kpiAvailabilityP95Ms <= thresholds.maxKpiAvailabilityP95Ms())
                : GateHealth.NA),
        gate("Sink P95", formatMs(fdb.sinkP95Ms()),
            "<= " + formatMs(thresholds.maxSinkP95Ms()),
            fdb.sinkP95Ms() >= 0 ? health(fdb.sinkP95Ms() <= thresholds.maxSinkP95Ms()) : GateHealth.NA),
        gate("Connector 写入 P95", formatMs(fdb.connectorWriteP95Ms()),
            "<= " + formatMs(thresholds.maxSinkP95Ms()),
            fdb.connectorWriteP95Ms() >= 0
                ? health(fdb.connectorWriteP95Ms() <= thresholds.maxSinkP95Ms())
                : GateHealth.NA),
        gate("Connector 提交 P95", formatMs(fdb.connectorCommitP95Ms()),
            "<= " + formatMs(thresholds.maxSinkP95Ms()),
            fdb.connectorCommitP95Ms() >= 0
                ? health(fdb.connectorCommitP95Ms() <= thresholds.maxSinkP95Ms())
                : GateHealth.NA),
        gate("Sink 失败数", fdb.sinkFailures() + " 次", "<= 0 次",
            health(fdb.sinkFailures() <= 0)),
        gate("Watermark 滞后", formatMs(fdb.watermarkLagMs()),
            "<= " + formatMs(thresholds.maxWatermarkLagMs()),
            health(fdb.watermarkLagMs() <= thresholds.maxWatermarkLagMs())),
        gate("存储健康", storage.healthy() ? "健康" : "异常", "健康",
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
          <thead><tr><th>指标</th><th>观测值</th><th>门限</th><th>健康状态</th></tr></thead>
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
        row("压测 ID", config.benchmarkId()),
        row("目标环境", config.target()),
        row("运行 ID", plan.runId()),
        row("运行标签", plan.runLabel()),
        row("Sink", plan.sink().value()),
        row("小区档位", String.valueOf(plan.cellLevel())),
        row("目标 CHR EPS", rateValue(plan.targetChrEpsPerCell()) + " 条/小区/s"),
        row("全局 CHR EPS", formatNumber(plan.targetChrTotalEps()) + " 条/s"),
        row("目标 PM EPS", rateValue(plan.targetPmEpsPerCell()) + " 条/小区/s"),
        row("全局 PM EPS", formatNumber(plan.targetPmTotalEps()) + " 条/s"),
        row("状态", result.status().name()),
        row("原因", result.bottleneckReason()),
        row("Source 吞吐达成率", formatRatio(source.producerDeliveryRatio())),
        row("Source 积压", source.chrBacklogRecords() + " 条 CHR"),
        row("预期 1m 关闭窗口", String.valueOf(windows.expectedClosedMinuteWindows())),
        row("Checkpoint 间隔", formatMs(effectiveCheckpointIntervalMs(plan.sink(), config.checkpointIntervalMs()))),
        row("Checkpoint 耗时", formatMs(flink.checkpointDurationMs())),
        row("Checkpoint 失败数", String.valueOf(flink.consecutiveCheckpointFailures()))));
  }

  private static String runNotes(BenchmarkRunResult result) {
    boolean hasKpi5mSinkRows = result.fdb().sinkLatencies().stream()
        .anyMatch(sink -> isBusinessSinkFront(sink)
            && lower(sink.sinkName()).contains("kpi-5m")
            && sink.records() > 0);
    boolean hasKpi5mOperatorInput = result.flink().operators().stream()
        .anyMatch(operator -> lower(operator.name()).contains("kpi-5m") && operator.recordsInTotal() > 0);
    String fiveMinuteNote = hasKpi5mSinkRows
        ? "本轮 KPI 5m 已采到 sink 样本。"
        : hasKpi5mOperatorInput
            ? "KPI 5m rollup 已收到 1m 输入，但采样时 5 分钟事件时间窗口尚未输出 sink 行。"
            : "KPI 5m 需要 5 分钟事件时间窗口和 watermark 推进；短跑或提前停止的运行可能显示 0 行。";
    return twoColumnTable(List.of(
        row("拓扑生成耗时", "仅表示本地拓扑对象生成耗时；Kafka 发布耗时和总耗时在其它字段展示。"),
        row("终端 Sink 输出记录", "Flink 终端 sink 算子的 Records Out Total 常见为 0；写入量请看 Records In Total 和 Sink 与存储。"),
        row("Sink 探针范围", "sink-front 是进入 connector 前的累计延迟；connector-write 是 writer/invoke 耗时；connector-commit 是 flush/prepare/commit/checkpoint 耗时，不包含 connector-write。"),
        row("KPI 5m 输出", fiveMinuteNote),
        row("异常输出", "异常规则连续满足后才输出；0 行表示未激活，或本轮还没有提交到 sink。")));
  }

  private static boolean isBusinessSinkFront(SinkLatencySnapshot sink) {
    return "sink-front".equals(lower(sink.scope()));
  }

  private static String sourceDensity(SourceMetricsSnapshot source, int cellLevel) {
    return """
        <table>
          <thead>
            <tr><th>Source</th><th>总记录</th><th>Records/s</th><th>每小区总记录</th><th>每小区 Records/s</th></tr>
          </thead>
          <tbody>
            %s
            %s
            %s
          </tbody>
        </table>
        """.formatted(
            densityRow("CHR 总量", source.chrPublished(), source.chrObservedEps(),
                source.chrTotalPerCell(cellLevel), source.chrPerSecondPerCell(cellLevel)),
            densityRow("PM 总量", source.pmPublished(), source.pmObservedEps(),
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
    return "<tr><th>CFG 总量</th><td>" + source.cfgPublished() + "</td><td>"
        + formatDouble(source.cfgObservedEps()) + "</td><td>" + formatDouble(source.cfgTotalPerCell(cellLevel))
        + "</td><td>N/A (一次性初始化)</td></tr>";
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
    StringBuilder header = new StringBuilder("<thead><tr><th>阶段</th><th>输入总记录</th>"
        + "<th>输出总记录</th><th>已关闭分钟窗口</th><th>预期关闭分钟窗口</th>");
    if (columns.showInputWatermark()) {
      header.append("<th>输入 Watermark</th>");
    }
    if (columns.showOutputWatermark()) {
      header.append("<th>输出 Watermark</th>");
    }
    return header.append("<th>健康状态</th></tr></thead>").toString();
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
      return twoColumnTable(List.of(row("状态", "未采集到 topology metrics 文件")));
    }
    return twoColumnTable(List.of(
        row("生成记录数", formatNumber(topology.generatedRecords())),
        row("站点数", formatNumber(topology.siteCount())),
        row("频段数", formatNumber(topology.frequencyBandCount())),
        row("拓扑生成耗时", formatMs(topology.generationDurationMs())),
        row("Kafka 发布耗时", formatMs(topology.publishDurationMs())),
        row("总耗时", formatMs(topology.totalDurationMs())),
        row("已发布拓扑记录", formatNumber(topology.publishedRecords())),
        row("发布失败数", formatNumber(topology.publishFailures())),
        row("纬度范围", formatRange(topology.minLatitude(), topology.maxLatitude())),
        row("经度范围", formatRange(topology.minLongitude(), topology.maxLongitude()))));
  }

  private static String flinkResourcesTable(BenchmarkRunResult result) {
    FlinkSnapshot flink = result.flink();
    boolean trusted = operatorMetricsTrusted(result);
    return twoColumnTable(List.of(
        row("作业状态", flink.jobStatus()),
        row("TaskManagers", String.valueOf(flink.taskManagers())),
        row("Slots", String.valueOf(flink.slots())),
        row("TaskManager CPU 负载", formatOptionalRatio(flink.taskManagerCpuLoad())),
        row("算子指标有效性", operatorMetricsValidity(result)),
        row("采样平均输入 Records/s", trusted ? formatNumber(flink.recordsInPerSec()) : "N/A"),
        row("采样平均输出 Records/s", trusted ? formatNumber(flink.recordsOutPerSec()) : "N/A"),
        row("算子聚合输入总记录", trusted ? formatNumber(flink.recordsInTotal()) : "N/A"),
        row("算子聚合输出总记录", trusted ? formatNumber(flink.recordsOutTotal()) : "N/A"),
        row("反压", trusted ? formatRatio(flink.backpressureRatio()) : "N/A"),
        row("Checkpoint 耗时", formatMs(flink.checkpointDurationMs())),
        row("Checkpoint 失败数", String.valueOf(flink.consecutiveCheckpointFailures()))));
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
    Map<String, OperatorLatency> latencies = operatorLatencies(plan, fdb, flink);
    int businessLatencies = Math.toIntExact(flink.operators().stream()
        .filter(operator -> latencies.getOrDefault(operator.id(), OperatorLatency.empty()).available())
        .count());
    return """
        <table>
          <thead><tr><th>指标覆盖</th><th>算子数</th><th>主表展示</th></tr></thead>
          <tbody>
            <tr><td>Flink 算子指标</td><td>%s</td><td>展示</td></tr>
            <tr><td>输入 Watermark</td><td>%s</td><td>%s</td></tr>
            <tr><td>输出 Watermark</td><td>%s</td><td>%s</td></tr>
            <tr><td>延迟探针映射</td><td>%s</td><td>展示</td></tr>
          </tbody>
        </table>
        """.formatted(
            coverage(operatorMetrics, total),
            coverage(inputWatermarks, total), coverageVisibility(columns.showInputWatermark(),
                "隐藏（未暴露有效输入 watermark）"),
            coverage(outputWatermarks, total), coverageVisibility(columns.showOutputWatermark(),
                "隐藏（未暴露有效输出 watermark）"),
            coverage(businessLatencies, total));
  }

  private static String coverage(int count, int total) {
    return count + "/" + total + " 算子";
  }

  private static String coverageVisibility(boolean shown, String hiddenReason) {
    return shown ? "展示" : hiddenReason;
  }

  private static String operatorMetricsTable(BenchmarkConfig config, BenchmarkRunResult result) {
    BenchmarkRunPlan plan = result.plan();
    BenchmarkThresholds thresholds = config.thresholds();
    FlinkSnapshot flink = result.flink();
    FdbMetricsSnapshot fdb = result.fdb();
    boolean trusted = operatorMetricsTrusted(result);
    OperatorTableColumns columns = operatorTableColumns(plan, fdb, flink);
    Map<String, OperatorLatency> latencies = operatorLatencies(plan, fdb, flink);
    StringBuilder rows = new StringBuilder();
    if (flink.operators().isEmpty()) {
      rows.append("<tr><td>aggregate job</td><td>aggregate</td><td>-</td><td>")
          .append(formatNumber(flink.recordsInPerSec())).append("</td><td>")
          .append(formatNumber(flink.recordsOutPerSec())).append("</td><td>")
          .append(formatNumber(flink.recordsInTotal())).append("</td><td>")
          .append(formatNumber(flink.recordsOutTotal()))
          .append("</td><td>-</td><td>-</td><td>-</td><td>-</td><td>-</td>");
      appendOptionalWatermarkCells(rows, columns, "-", "-");
      rows.append("<td>").append(formatRatio(flink.backpressureRatio()))
          .append("</td><td>").append(formatMsPerSecond(flink.backpressureRatio()))
          .append("</td><td>N/A</td><td>N/A</td><td>N/A</td><td>N/A</td><td>N/A</td>");
      rows.append("<td>N/A</td><td>N/A</td><td>")
          .append(healthBadge(GateHealth.NA)).append("</td></tr>");
    } else {
      for (FlinkOperatorSnapshot operator : flink.operators()) {
        OperatorLatency latency = latencies.getOrDefault(operator.id(), OperatorLatency.empty());
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
            .append(formatOperatorMsPerSecond(operator, operator.busyRatio(), trusted)).append("</td><td>")
            .append(formatOperatorRatio(operator, operator.idleRatio(), trusted)).append("</td>");
        appendOptionalWatermarkCells(rows, columns,
            escape(formatOperatorWatermark(operator, operator.currentInputWatermarkMs(), trusted)),
            escape(formatOperatorWatermark(operator, operator.currentOutputWatermarkMs(), trusted)));
        rows.append("<td>")
            .append(formatOperatorRatio(operator, operator.backpressureRatio(), trusted)).append("</td><td>")
            .append(formatOperatorMsPerSecond(operator, operator.backpressureRatio(), trusted)).append("</td>");
        rows.append("<td>").append(formatMs(latency.cumulativeP95Ms())).append("</td><td>")
            .append(formatMs(latency.operatorP95Ms())).append("</td><td>")
            .append(escape(latency.summary())).append("</td><td>")
            .append(escape(latency.probeSource())).append("</td><td>")
            .append(escape(latency.probeType())).append("</td><td>");
        rows.append(healthBadge(health)).append("</td></tr>");
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
        "<thead><tr><th>算子</th><th>角色</th><th>并行度</th>"
            + "<th>采样输入 Records/s</th><th>采样输出 Records/s</th>"
            + "<th>输入总记录</th><th>输出总记录</th><th>输入 Bytes/s</th><th>输出 Bytes/s</th>"
            + "<th>忙碌</th><th>忙碌 ms/s</th><th>空闲</th>");
    if (columns.showInputWatermark()) {
      header.append("<th>输入 Watermark</th>");
    }
    if (columns.showOutputWatermark()) {
      header.append("<th>输出 Watermark</th>");
    }
    header.append("<th>反压</th><th>反压 ms/s</th><th>累计延迟</th><th>算子延迟</th><th>延迟摘要</th>"
        + "<th>延迟探针</th><th>Probe类型</th><th>健康状态</th></tr></thead>");
    return header.toString();
  }

  private static OperatorTableColumns operatorTableColumns(BenchmarkRunPlan plan, FdbMetricsSnapshot fdb,
      FlinkSnapshot flink) {
    return new OperatorTableColumns(
        flink.operators().stream().anyMatch(operator -> operator.currentInputWatermarkMs() >= 0),
        flink.operators().stream().anyMatch(operator -> operator.currentOutputWatermarkMs() >= 0));
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

  private static Map<String, OperatorLatency> operatorLatencies(BenchmarkRunPlan plan, FdbMetricsSnapshot fdb,
      FlinkSnapshot flink) {
    Map<String, OperatorLatency> values = new LinkedHashMap<>();
    for (FlinkOperatorSnapshot operator : flink.operators()) {
      values.put(operator.id(), rawOperatorLatency(plan, fdb, operator));
    }
    Map<String, Long> upstreamCumulativeP95 = maxUpstreamCumulativeP95(flink.operatorEdges(), values);
    for (FlinkOperatorSnapshot operator : flink.operators()) {
      OperatorLatency latency = values.getOrDefault(operator.id(), OperatorLatency.empty());
      values.put(operator.id(), latency.withOperatorDelay(upstreamCumulativeP95.get(operator.id())));
    }
    return values;
  }

  private static Map<String, Long> maxUpstreamCumulativeP95(List<FlinkOperatorEdge> edges,
      Map<String, OperatorLatency> latencies) {
    Map<String, Long> values = new HashMap<>();
    for (FlinkOperatorEdge edge : edges) {
      OperatorLatency upstream = latencies.get(edge.sourceId());
      if (upstream == null || upstream.cumulativeP95Ms() < 0L) {
        continue;
      }
      values.merge(edge.targetId(), upstream.cumulativeP95Ms(), Math::max);
    }
    return values;
  }

  private static OperatorLatency rawOperatorLatency(BenchmarkRunPlan plan, FdbMetricsSnapshot fdb,
      FlinkOperatorSnapshot operator) {
    OperatorLatency sinkLatency = rawSinkOperatorLatency(plan, fdb, operator);
    if (sinkLatency.available()) {
      return sinkLatency;
    }
    StageLatencySnapshot stage = explicitStageLatency(fdb, operator);
    if (stage != null) {
      return OperatorLatency.stage(stage);
    }
    SinkLatencySnapshot windowMaterialization = explicitWindowMaterializationLatency(fdb, operator);
    if (windowMaterialization != null) {
      return OperatorLatency.windowMaterialization(windowMaterialization);
    }
    return OperatorLatency.empty();
  }

  private static StageLatencySnapshot explicitStageLatency(FdbMetricsSnapshot fdb, FlinkOperatorSnapshot operator) {
    for (StageLatencySnapshot stage : fdb.stageLatencies()) {
      if (matchesExplicitStageToken(operator, stage.stageId())) {
        return stage;
      }
    }
    return null;
  }

  private static SinkLatencySnapshot explicitWindowMaterializationLatency(FdbMetricsSnapshot fdb,
      FlinkOperatorSnapshot operator) {
    for (SinkLatencySnapshot sink : fdb.sinkLatencies()) {
      if (!"window-materialization".equals(lower(sink.scope())) && !"window-materialization".equals(lower(sink.sinkType()))) {
        continue;
      }
      if (matchesExplicitStageToken(operator, sink.sinkName())
          || matchesExactOperatorToken(operator, sink.dataset() + "-fact")
          || matchesExactOperatorToken(operator, sink.sinkName() + "-materialization")) {
        return sink;
      }
    }
    return null;
  }

  private static OperatorLatency rawSinkOperatorLatency(BenchmarkRunPlan plan, FdbMetricsSnapshot fdb,
      FlinkOperatorSnapshot operator) {
    for (SinkProbeBundle bundle : sinkProbeBundles(plan, fdb).values()) {
      if (matchesExactOperatorToken(operator, bundle.baseName())) {
        return OperatorLatency.sink(bundle);
      }
    }
    return OperatorLatency.empty();
  }

  private static Map<String, SinkProbeBundle> sinkProbeBundles(BenchmarkRunPlan plan, FdbMetricsSnapshot fdb) {
    Map<String, SinkProbeBundle> bundles = new LinkedHashMap<>();
    for (SinkLatencySnapshot sink : fdb.sinkLatencies()) {
      if (!isCurrentBusinessSink(plan.sink(), sink.sinkName()) && !isAuxiliarySink(sink.sinkName())) {
        continue;
      }
      String baseName = sinkBaseName(sink.sinkName());
      SinkProbeBundle bundle = bundles.computeIfAbsent(baseName, SinkProbeBundle::new);
      bundle.add(sink);
    }
    return bundles;
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

  private static boolean matchesExplicitStageToken(FlinkOperatorSnapshot operator, String stageId) {
    String value = lower(stageId);
    return switch (value) {
      case "chr-source" -> matchesAnyExactOperatorToken(operator, "chr-source-metrics", "Source: chr-source");
      case "pm-source" -> matchesAnyExactOperatorToken(operator, "pm-source-metrics", "Source: pm-source");
      case "cfg-source" -> matchesAnyExactOperatorToken(operator, "cfg-source-metrics", "cfg-source");
      case "kafka" -> matchesAnyExactOperatorToken(operator, "kafka-topics-metrics");
      case "enrichment" -> matchesAnyExactOperatorToken(operator, "enrichment-metrics");
      case "kpi-1m" -> matchesAnyExactOperatorToken(operator, "kpi-1m-metrics", "kpi-1m-full-join");
      case "kpi-5m" -> matchesAnyExactOperatorToken(operator, "kpi-5m-metrics", "kpi-5m-rollup");
      case "user-anomaly" -> matchesAnyExactOperatorToken(operator, "user-anomaly-metrics",
          "user-event-anomaly-detect-dedup");
      case "grid-anomaly" -> matchesAnyExactOperatorToken(operator, "grid-anomaly-metrics",
          "coverage-hole-detector");
      case "cell-anomaly" -> matchesAnyExactOperatorToken(operator, "cell-anomaly-metrics",
          "cell-kpi-anomaly-detect-dedup");
      case "assigner" -> matchesAnyExactOperatorToken(operator, "vbucket-assigner-metrics");
      case "load-coordinator" -> matchesAnyExactOperatorToken(operator, "load-coordinator-metrics");
      case "window-chr-1m" -> matchesAnyExactOperatorToken(operator, "window-chr-1m-materialization", "chr-1m-fact");
      case "window-pm-1m" -> matchesAnyExactOperatorToken(operator, "window-pm-1m-materialization", "pm-1m-fact");
      case "window-kpi-1m" -> matchesAnyExactOperatorToken(operator, "window-kpi-1m-materialization",
          "kpi-1m-full-join");
      default -> false;
    };
  }

  private static boolean matchesAnyExactOperatorToken(FlinkOperatorSnapshot operator, String... tokens) {
    for (String token : tokens) {
      if (matchesExactOperatorToken(operator, token)) {
        return true;
      }
    }
    return false;
  }

  private static boolean matchesExactOperatorToken(FlinkOperatorSnapshot operator, String token) {
    String expected = normalizedOperatorToken(token);
    if (expected.isBlank()) {
      return false;
    }
    if (expected.equals(normalizedOperatorToken(operator.id()))
        || expected.equals(normalizedOperatorToken(operator.name()))) {
      return true;
    }
    for (String part : lower(operator.name()).split("\\s*->\\s*")) {
      if (expected.equals(normalizedOperatorToken(part))) {
        return true;
      }
    }
    return false;
  }

  private static String normalizedOperatorToken(String value) {
    String token = lower(value).trim();
    while (!token.isEmpty() && isTokenWrapper(token.charAt(0))) {
      token = token.substring(1).trim();
    }
    while (!token.isEmpty() && isTokenWrapper(token.charAt(token.length() - 1))) {
      token = token.substring(0, token.length() - 1).trim();
    }
    return token;
  }

  private static boolean isTokenWrapper(char value) {
    return value == '(' || value == ')' || value == '[' || value == ']' || value == '{' || value == '}';
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
    if (latency.healthP95Ms() >= 0 && latency.healthP95Ms() > latencyThreshold(thresholds, latency)) {
      return GateHealth.UNSTABLE;
    }
    if (trusted && operator.metricsAvailable() && operator.backpressureRatio() > thresholds.maxBackpressureRatio()) {
      return GateHealth.UNSTABLE;
    }
    if (!latency.available() && (!trusted || !operator.metricsAvailable())) {
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
    if (latency.healthP95Ms() >= 0 && latency.healthP95Ms() > latencyThreshold(thresholds, latency)) {
      return GateHealth.UNSTABLE;
    }
    if (trusted && operator.metricsAvailable() && operator.backpressureRatio() > thresholds.maxBackpressureRatio()) {
      return GateHealth.UNSTABLE;
    }
    return GateHealth.HEALTHY;
  }

  private static long latencyThreshold(BenchmarkThresholds thresholds, OperatorLatency latency) {
    return latency.sinkLatency() ? thresholds.maxSinkP95Ms() : thresholds.maxKpiAvailabilityP95Ms();
  }

  private static GateHealth markerHealth(BenchmarkThresholds thresholds, FlinkMarkerLatencySnapshot marker) {
    return marker.p95Ms() > thresholds.maxWatermarkLagMs() ? GateHealth.UNSTABLE : GateHealth.HEALTHY;
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
    if (latency.cumulativeP95Ms() >= 0) {
      return "累计 " + formatMs(latency.cumulativeP95Ms());
    }
    return "累计 N/A";
  }

  private static String operatorNodeTitle(FlinkOperatorSnapshot operator, OperatorLatency latency,
      GateHealth health) {
    String title = operator.name()
        + " | " + health.label()
        + " | " + operatorNodeLatencyLabel(operator, latency);
    if (!latency.available()) {
      return title + " | 延迟探针 N/A";
    }
    return title
        + " | " + latency.summary()
        + " | 延迟探针 " + latency.probeSource()
        + " | Probe类型 " + latency.probeType();
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
          <h3>列说明</h3>
          <table>
            <thead><tr><th>列</th><th>含义</th></tr></thead>
            <tbody>
              <tr><td>TaskManager CPU 负载</td><td>来自 Flink REST 的 TaskManager JVM CPU Load 最大值。当前默认 REST 指标不提供精确算子级 CPU。</td></tr>
              <tr><td>忙碌 / 忙碌 ms/s</td><td>来自 Flink REST 的 busyTimeMsPerSecond，同时展示为一秒内占比和原始毫秒值。</td></tr>
              <tr><td>反压 / 反压 ms/s</td><td>来自 Flink REST 的 backPressuredTimeMsPerSecond，同时展示为一秒内占比和原始毫秒值。</td></tr>
              <tr><td>累计延迟</td><td>当前算子输出被探针观测到时，相对原始 eventTs/sourceEventTsAvg 的 P95 延迟。</td></tr>
              <tr><td>算子延迟</td><td>当前算子的累计延迟减去直接上游最大累计延迟；Source 算子没有上游时等于累计延迟。</td></tr>
              <tr><td>延迟摘要</td><td>同一行的可用延迟汇总。Sink 行会合并 Sink入口累计、写入耗时和提交耗时。</td></tr>
              <tr><td>延迟探针</td><td>显式映射到该算子的自定义探针，例如 stage:kpi-1m 或 sink:starrocks-kpi-1m。没有显式映射时为 N/A。</td></tr>
              <tr><td>Probe类型</td><td>输出累计、Sink入口累计、写入耗时、提交耗时或传输延迟。传输延迟只显示在流程图边上。</td></tr>
              <tr><td>流程图边</td><td>边上的“传输”来自 Flink latency marker P95；传输延迟不可用表示当前 Flink REST 指标没有暴露 marker 分位值。</td></tr>
            </tbody>
          </table>
          <h3>异常值分析</h3>
          <table>
            <thead><tr><th>现象</th><th>可能原因</th><th>下一步检查</th></tr></thead>
            <tbody>
              <tr><td>累计延迟高但传输延迟正常</td><td>窗口等待、事件乱序、Join 等待或源事件时间基线导致累计耗时增加。</td><td>检查 1m 窗口诊断、Join 等待配置和上游累计延迟。</td></tr>
              <tr><td>传输延迟高</td><td>Source 到目标算子之间存在排队、网络传输、shuffle 或上游反压。</td><td>检查上游输出速率、下游忙碌度和反压。</td></tr>
              <tr><td>反压高但输出速率低</td><td>该算子或下游 sink 无法按当前速率排空数据。</td><td>调整目标算子并行度，或检查 sink/storage 健康。</td></tr>
              <tr><td>提交耗时高但写入耗时正常</td><td>瓶颈更可能在 connector commit、checkpoint 或存储元数据提交，而不是单条写入。</td><td>检查 checkpoint 间隔、committer 并行度、存储元数据压力和 connector 日志。</td></tr>
              <tr><td>延迟探针为 N/A</td><td>该算子没有显式探针映射，或本轮没有采到合法 sourceEventTs 延迟样本。</td><td>只在该算子对压测判断关键时增加轻量探针。</td></tr>
              <tr><td>Flink 指标为 N/A</td><td>Flink REST 没有返回可用算子指标，或源端已发布记录但算子指标全为零。</td><td>任务处理期间检查 /jobs/{jobId}/vertices/{vertexId}/metrics。</td></tr>
            </tbody>
          </table>
        </div>
        """;
  }

  private record OperatorLatency(
      long cumulativeP95Ms,
      long operatorP95Ms,
      long writeP95Ms,
      long commitP95Ms,
      String probeSource,
      String probeType,
      boolean sinkLatency,
      String note) {
    static OperatorLatency empty() {
      return new OperatorLatency(-1L, -1L, -1L, -1L, "N/A", "N/A", false, "");
    }

    static OperatorLatency stage(StageLatencySnapshot stage) {
      return new OperatorLatency(stage.latencyP95Ms(), -1L, -1L, -1L,
          "stage:" + stage.stageId(), "输出累计", false, "");
    }

    static OperatorLatency windowMaterialization(SinkLatencySnapshot sink) {
      return new OperatorLatency(sink.latencyP95Ms(), -1L, -1L, -1L,
          "stage:" + sink.sinkName(), "输出累计", false, "");
    }

    static OperatorLatency sink(SinkProbeBundle bundle) {
      return new OperatorLatency(
          bundle.sinkFrontP95Ms(),
          -1L,
          bundle.connectorWriteP95Ms(),
          bundle.connectorCommitP95Ms(),
          "sink:" + bundle.baseName(),
          "Sink入口累计",
          true,
          "");
    }

    boolean available() {
      return cumulativeP95Ms >= 0 || writeP95Ms >= 0 || commitP95Ms >= 0;
    }

    long healthP95Ms() {
      return cumulativeP95Ms;
    }

    OperatorLatency withOperatorDelay(Long upstreamCumulativeP95Ms) {
      if (cumulativeP95Ms < 0L) {
        return this;
      }
      if (upstreamCumulativeP95Ms == null || upstreamCumulativeP95Ms < 0L) {
        return new OperatorLatency(cumulativeP95Ms, cumulativeP95Ms, writeP95Ms, commitP95Ms,
            probeSource, probeType, sinkLatency, note);
      }
      long delta = cumulativeP95Ms - upstreamCumulativeP95Ms;
      if (delta < -1_000L) {
        return new OperatorLatency(cumulativeP95Ms, -1L, writeP95Ms, commitP95Ms,
            probeSource, probeType, sinkLatency, "上游累计延迟高于当前超过 1s，可能是采样窗口错位");
      }
      String nextNote = delta < 0L ? "采样窗口轻微错位，算子延迟按 0 ms 展示" : note;
      return new OperatorLatency(cumulativeP95Ms, Math.max(0L, delta), writeP95Ms, commitP95Ms,
          probeSource, probeType, sinkLatency, nextNote);
    }

    String summary() {
      if (!available()) {
        return "N/A";
      }
      StringBuilder value = new StringBuilder()
          .append("累计=").append(formatMs(cumulativeP95Ms))
          .append(" | 算子=").append(formatMs(operatorP95Ms));
      if (writeP95Ms >= 0L) {
        value.append(" | 写入=").append(formatMs(writeP95Ms));
      }
      if (commitP95Ms >= 0L) {
        value.append(" | 提交=").append(formatMs(commitP95Ms));
      }
      if (!note.isBlank()) {
        value.append(" | ").append(note);
      }
      return value.toString();
    }
  }

  private static final class SinkProbeBundle {
    private final String baseName;
    private SinkLatencySnapshot sinkFront;
    private SinkLatencySnapshot connectorWrite;
    private SinkLatencySnapshot connectorCommit;

    private SinkProbeBundle(String baseName) {
      this.baseName = baseName;
    }

    private String baseName() {
      return baseName;
    }

    private void add(SinkLatencySnapshot snapshot) {
      switch (lower(snapshot.scope())) {
        case "connector-write" -> connectorWrite = maxP95(connectorWrite, snapshot);
        case "connector-commit" -> connectorCommit = maxP95(connectorCommit, snapshot);
        default -> sinkFront = maxP95(sinkFront, snapshot);
      }
    }

    private static SinkLatencySnapshot maxP95(SinkLatencySnapshot current, SinkLatencySnapshot next) {
      if (current == null || next.latencyP95Ms() > current.latencyP95Ms()) {
        return next;
      }
      return current;
    }

    private long sinkFrontP95Ms() {
      return sinkFront == null ? -1L : sinkFront.latencyP95Ms();
    }

    private long connectorWriteP95Ms() {
      return connectorWrite == null ? -1L : connectorWrite.latencyP95Ms();
    }

    private long connectorCommitP95Ms() {
      return connectorCommit == null ? -1L : connectorCommit.latencyP95Ms();
    }
  }

  private record OperatorTableColumns(boolean showInputWatermark, boolean showOutputWatermark) {
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
      return "<p>本轮未采集到算子流程图。</p>";
    }
    Map<String, Integer> levels = operatorLevels(flink.operators(), flink.operatorEdges());
    Map<String, FlowPosition> positions = operatorPositions(flink.operators(), levels);
    Map<String, FlinkOperatorSnapshot> operatorsById = operatorsById(flink.operators());
    Map<String, OperatorLatency> latencies = operatorLatencies(plan, fdb, flink);
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
            .append("\">传输 ").append(escape(formatMs(marker.p95Ms()))).append("</text>");
      }
    }

    StringBuilder nodes = new StringBuilder();
    for (FlinkOperatorSnapshot operator : flink.operators()) {
      FlowPosition position = positions.get(operator.id());
      OperatorLatency latency = latencies.getOrDefault(operator.id(), OperatorLatency.empty());
      GateHealth health = operatorGraphHealth(thresholds, operator, latency, trusted);
      nodes.append("<g class=\"flow-node ").append(flowNodeClass(health))
          .append("\" data-operator-id=\"").append(escape(operator.id()))
          .append("\" data-latency-health=\"").append(escape(health.label()))
          .append("\" transform=\"translate(").append(position.x()).append(",").append(position.y()).append(")\">")
          .append("<title>").append(escape(operatorNodeTitle(operator, latency, health))).append("</title>")
          .append("<rect width=\"180\" height=\"86\" rx=\"4\"></rect>")
          .append("<text x=\"10\" y=\"22\">").append(escape(shortLabel(operator.name(), 28))).append("</text>")
          .append("<text x=\"10\" y=\"43\">p=").append(operator.parallelism())
          .append(" 输出/s=").append(escape(formatOperatorNumber(operator, operator.recordsOutPerSec(), trusted)))
          .append("</text>")
          .append("<text class=\"flow-latency\" x=\"10\" y=\"62\">")
          .append(escape(operatorNodeLatencyLabel(operator, latency))).append("</text>")
          .append("<text x=\"10\" y=\"79\">反压=")
          .append(escape(formatOperatorRatio(operator, operator.backpressureRatio(), trusted)))
          .append("</text></g>");
    }

    return """
        <div class="flow-wrap">
          <svg class="operator-flow" viewBox="0 0 %s %s" role="img" aria-label="Flink 算子流程">
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
            <thead><tr><th>阶段</th><th>P50</th><th>P95</th><th>P99</th><th>Watermark 滞后</th></tr></thead>
            <tbody>%s</tbody>
          </table>
          """.formatted(rows);
    }
    return """
        <table>
          <thead><tr><th>指标</th><th>P95</th><th>说明</th></tr></thead>
          <tbody>
            <tr><td>Source 延迟</td><td>%s</td><td>Source 到达端到端延迟。</td></tr>
            <tr><td>KPI 1m 可用延迟</td><td>%s</td><td>1 分钟 KPI 可用延迟。</td></tr>
            <tr><td>KPI 5m 可用延迟</td><td>%s</td><td>5 分钟 KPI 可用延迟。</td></tr>
            <tr><td>Sink 入口累计延迟</td><td>%s</td><td>进入 connector 前的累计延迟，不是 connector 内部写入耗时。</td></tr>
            <tr><td>Connector 写入</td><td>%s</td><td>仅 connector writer/invoke 耗时。</td></tr>
            <tr><td>Connector 提交</td><td>%s</td><td>仅 connector flush/prepare/commit/checkpoint 耗时。</td></tr>
            <tr><td>Watermark 滞后</td><td>%s</td><td>观测到的最大 stage watermark lag。</td></tr>
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
        appendSinkRow(rows, "本次", sink);
      }
      if (currentSinks.isEmpty()) {
        rows.append("<tr><td>本次</td><td colspan=\"9\">未采集到本次业务 sink 延迟样本</td></tr>");
      }
      for (SinkLatencySnapshot sink : auxiliarySinks) {
        appendSinkRow(rows, "辅助", sink);
      }
      rows.append("<tr><td>存储健康</td><td>storage</td><td colspan=\"3\">")
          .append(storage.healthy() ? "健康" : "异常").append("</td><td colspan=\"5\">")
          .append(escape(storage.summary())).append("</td></tr>");
      appendStorageProbeRow(rows, plan.sink(), storage);
      return """
          <table>
            <thead><tr><th>类型</th><th>Scope</th><th>Sink</th><th>记录数</th><th>字节</th><th>P50</th><th>P95</th><th>P99</th><th>失败数</th><th>延迟样本可信度</th></tr></thead>
            <tbody>%s</tbody>
          </table>
          """.formatted(rows);
    }
    return """
        <table>
          <thead><tr><th>区域</th><th>值</th><th>状态</th></tr></thead>
          <tbody>
            <tr><td>Sink P95</td><td>%s</td><td>%s 次失败</td></tr>
            <tr><td>Connector 写入 P95</td><td>%s</td><td>connector writer/invoke 耗时</td></tr>
            <tr><td>Connector 提交 P95</td><td>%s</td><td>connector flush/prepare/commit/checkpoint 耗时</td></tr>
            <tr><td>存储健康</td><td>%s</td><td>%s</td></tr>
            <tr><td>存储记录</td><td>%s</td><td>小文件 %s, 写入中 %s</td></tr>
          </tbody>
        </table>
        """.formatted(
            formatMs(fdb.sinkP95Ms()),
            fdb.sinkFailures(),
            formatMs(fdb.connectorWriteP95Ms()),
            formatMs(fdb.connectorCommitP95Ms()),
            storage.healthy() ? "健康" : "异常",
            escape(storage.summary()),
            storage.records(),
            storage.smallFiles(),
            storage.inProgressFiles());
  }

  private static void appendStorageProbeRow(StringBuilder rows, BenchmarkSink sink, StorageSnapshot storage) {
    if (sink == BenchmarkSink.STARROCKS) {
      rows.append("<tr><td>存储行数</td><td>storage</td><td>starrocks</td><td>")
          .append(formatNumber(storage.records()))
          .append("</td><td>-</td><td colspan=\"5\">")
          .append(escape(storage.summary())).append("</td></tr>");
      return;
    }
    rows.append("<tr><td>存储文件</td><td>storage</td><td>").append(escape(sink.value())).append("</td><td>")
        .append(formatNumber(storage.records()))
        .append("</td><td>-</td><td colspan=\"5\">小文件 ")
        .append(storage.smallFiles()).append(", 写入中 ").append(storage.inProgressFiles()).append("</td></tr>");
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
      return "低（少于 30 条）";
    }
    if (records < 100) {
      return "中（少于 100 条）";
    }
    return "高";
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
    if (sink == BenchmarkSink.NONE) {
      return false;
    }
    String baseName = lower(sinkBaseName(sinkName));
    String expectedPrefix = sink.value() + "-";
    return baseName.equals(sink.value()) || baseName.startsWith(expectedPrefix);
  }

  private static boolean isAuxiliarySink(String sinkName) {
    String value = lower(sinkBaseName(sinkName));
    return value.equals("dlq")
        || value.endsWith("-dlq")
        || value.equals("metrics")
        || value.endsWith("-metrics")
        || value.equals("enrichment-late")
        || value.endsWith("-late");
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
      return "可用";
    }
    if (!result.flink().hasOperatorMetrics()) {
      return "不可用（Flink REST 未返回算子指标值）";
    }
    if (hasPublishedSourceRecords(result.source()) && !hasOperatorMetricSignal(result.flink())) {
      return "不可用（Source 已发布记录但 Flink 算子指标全为零）";
    }
    return "不可用";
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

  private static String formatOperatorMsPerSecond(FlinkOperatorSnapshot operator, double ratio, boolean trusted) {
    return trusted && operator.metricsAvailable() ? formatMsPerSecond(ratio) : "N/A";
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
      return "<p>本次未观察到稳定上限。</p>";
    }
    return "<ul>" + maxCells.entrySet().stream()
        .sorted(Map.Entry.comparingByKey())
        .map(entry -> "<li>" + escape(entry.getKey().value()) + ": " + entry.getValue() + " 小区</li>")
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
        .map(result -> result.plan().sink().value() + " " + result.plan().cellLevel() + " 小区")
        .orElse("无");
  }

  private static String recommendations(List<BenchmarkRunResult> results) {
    return results.stream()
        .filter(result -> result.status() != BenchmarkStatus.STABLE)
        .findFirst()
        .map(result -> "<p>首个瓶颈: " + escape(result.plan().sink().value()) + " 在 "
            + result.plan().cellLevel() + " 小区档位。 " + escape(result.bottleneckReason()) + "。</p>")
        .orElse("<p>所有已测轮次均满足门限。下一轮可继续提高小区档位或每小区 EPS。</p>");
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

  private static String formatOptionalRatio(Double value) {
    if (value == null || !Double.isFinite(value) || value < 0.0d) {
      return "N/A";
    }
    return formatRatio(value);
  }

  private static String formatMsPerSecond(double ratio) {
    if (!Double.isFinite(ratio) || ratio < 0.0d) {
      return "N/A";
    }
    return formatNumber(ratio * 1000.0d) + " ms/s";
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

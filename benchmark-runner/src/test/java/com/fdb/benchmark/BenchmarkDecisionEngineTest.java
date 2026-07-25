package com.fdb.benchmark;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;

class BenchmarkDecisionEngineTest {
  private final BenchmarkThresholds thresholds = new BenchmarkThresholds(
      0.2, 120_000, 2, 180_000, 180_000, 180_000);
  private final BenchmarkDecisionEngine engine = new BenchmarkDecisionEngine(thresholds);

  @Test
  void failed_job_marks_run_failed() {
    RunObservation observation = healthy().withFlink(healthy().flink().withJobStatus("FAILED"));

    BenchmarkRunResult result = engine.decide(plan(), observation);

    assertThat(result.status()).isEqualTo(BenchmarkStatus.FAILED);
    assertThat(result.bottleneckReason()).contains("Flink job status FAILED");
  }

  @Test
  void unknown_job_status_marks_run_unstable() {
    RunObservation observation = healthy().withFlink(healthy().flink().withJobStatus("UNKNOWN"));

    BenchmarkRunResult result = engine.decide(plan(), observation);

    assertThat(result.status()).isEqualTo(BenchmarkStatus.UNSTABLE);
    assertThat(result.bottleneckReason()).contains("Flink job status UNKNOWN");
  }

  @Test
  void sustained_backpressure_marks_unstable() {
    RunObservation observation = healthy().withFlink(healthy().flink().withBackpressureRatio(0.45));

    BenchmarkRunResult result = engine.decide(plan(), observation);

    assertThat(result.status()).isEqualTo(BenchmarkStatus.UNSTABLE);
    assertThat(result.bottleneckReason()).contains("backpressure");
  }

  @Test
  void producer_under_delivery_marks_unstable() {
    SourceMetricsSnapshot source = new SourceMetricsSnapshot(true, 300, 600, 270.0, 2_000, 0, 0,
        100, 1000, 100.0, 10_000, 0, 0,
        250, 1000, 1000.0, 0, 0, 0);

    BenchmarkRunResult result = engine.decide(plan(), healthy().withSource(source));

    assertThat(result.status()).isEqualTo(BenchmarkStatus.UNSTABLE);
    assertThat(result.bottleneckReason()).contains("source throughput attainment").contains("0.9");
  }

  @Test
  void missing_source_metrics_marks_unstable() {
    BenchmarkRunResult result = engine.decide(plan(), healthy().withSource(SourceMetricsSnapshot.empty()));

    assertThat(result.status()).isEqualTo(BenchmarkStatus.UNSTABLE);
    assertThat(result.bottleneckReason()).contains("source metrics missing");
  }

  @Test
  void pm_and_cfg_only_source_metrics_mark_unstable() {
    SourceMetricsSnapshot source = new SourceMetricsSnapshot(true, 0, 0, 0.0, 0, 0, 0,
        100, 1000, 100.0, 10_000, 0, 0,
        250, 1000, 250.0, 4_000, 0, 0);

    BenchmarkRunResult result = engine.decide(plan(), healthy().withSource(source));

    assertThat(result.status()).isEqualTo(BenchmarkStatus.UNSTABLE);
    assertThat(result.bottleneckReason()).contains("CHR source metrics missing");
  }

  @Test
  void source_backlog_over_threshold_marks_unstable() {
    BenchmarkDecisionEngine backlogEngine = new BenchmarkDecisionEngine(new BenchmarkThresholds(
        0.2, 120_000, 2, 180_000, 180_000, 180_000, 0.98, 10));
    RunObservation observation = healthy().withFlink(healthy().flink().withSourceBacklogRecords(42));

    BenchmarkRunResult result = backlogEngine.decide(plan(), observation);

    assertThat(result.status()).isEqualTo(BenchmarkStatus.UNSTABLE);
    assertThat(result.bottleneckReason()).contains("max source operator backlog").contains("42");
  }

  @Test
  void long_run_without_chr_minute_window_output_marks_unstable() {
    SourceMetricsSnapshot source = new SourceMetricsSnapshot(true, 300_000, 90_000_000, 300_000.0, 343_000, 0, 0,
        10_000, 3_430_000, 10_000.0, 343_000, 0, 0,
        10_000, 10_000, 0.0, 0, 0, 0);
    FlinkSnapshot flink = new FlinkSnapshot("RUNNING", 0, 30_000, 0, 310_000, 310_000,
        126_000_000, 126_000_000, 1, 6, List.of(
            new FlinkOperatorSnapshot("chr-1m", "chr-1m-fact -> to-chr-minute-fact-env", 6,
                50_000, 0, 17_422_480, 0, 8_192, 0, 0.2, 0.8, 0.0),
            new FlinkOperatorSnapshot("pm-1m", "pm-1m-fact -> to-pm-minute-fact-env", 6,
                10_000, 10_000, 3_260_000, 40_000, 8_192, 2_048, 0.2, 0.8, 0.0),
            new FlinkOperatorSnapshot("kpi-1m", "kpi-1m-full-join -> kpi-1m-metrics", 6,
                10_000, 0, 50_000, 0, 8_192, 0, 0.2, 0.8, 0.0)));

    BenchmarkRunResult result = engine.decide(plan10000(), healthy()
        .withSource(source)
        .withFlink(flink)
        .withFdb(new FdbMetricsSnapshot(2_000, 40_000, 45_000, 5_000, 0, 20_000,
            List.of(), List.of(
                windowMaterialization("window-pm-1m", "pm-1m", "MIN_1@60000", 10_000),
                windowMaterialization("window-pm-1m", "pm-1m", "MIN_1@120000", 10_000),
                windowMaterialization("window-pm-1m", "pm-1m", "MIN_1@180000", 10_000),
                windowMaterialization("window-pm-1m", "pm-1m", "MIN_1@240000", 10_000)))));

    assertThat(result.status()).isEqualTo(BenchmarkStatus.UNSTABLE);
    assertThat(result.bottleneckReason())
        .contains("1m window materialization")
        .contains("expected >= 4")
        .contains("CHR=0")
        .contains("PM=4")
        .contains("KPI=0");
  }

  @Test
  void six_minute_run_without_kpi_five_minute_output_marks_unstable() {
    SourceMetricsSnapshot source = new SourceMetricsSnapshot(true, 300_000, 108_000_000, 300_000.0, 360_000, 0, 0,
        10_000, 3_600_000, 10_000.0, 360_000, 0, 0,
        10_000, 10_000, 0.0, 0, 0, 0);
    FlinkSnapshot flink = new FlinkSnapshot("RUNNING", 0, 30_000, 0, 310_000, 310_000,
        126_000_000, 126_000_000, 1, 6, List.of(
            new FlinkOperatorSnapshot("kpi-5m", "kpi-5m-rollup -> kpi-5m-metrics", 6,
                10_000, 0, 0, 0, 8_192, 0, 0.2, 0.8, 0.0)));

    BenchmarkRunResult result = engine.decide(plan10000(), healthy()
        .withSource(source)
        .withFlink(flink)
        .withFdb(new FdbMetricsSnapshot(2_000, 40_000, 45_000, 5_000, 0, 20_000,
            List.of(), List.of(
                windowMaterialization("window-chr-1m", "chr-1m", "MIN_1@60000", 10_000),
                windowMaterialization("window-chr-1m", "chr-1m", "MIN_1@120000", 10_000),
                windowMaterialization("window-chr-1m", "chr-1m", "MIN_1@180000", 10_000),
                windowMaterialization("window-chr-1m", "chr-1m", "MIN_1@240000", 10_000),
                windowMaterialization("window-chr-1m", "chr-1m", "MIN_1@300000", 10_000),
                windowMaterialization("window-pm-1m", "pm-1m", "MIN_1@60000", 10_000),
                windowMaterialization("window-pm-1m", "pm-1m", "MIN_1@120000", 10_000),
                windowMaterialization("window-pm-1m", "pm-1m", "MIN_1@180000", 10_000),
                windowMaterialization("window-pm-1m", "pm-1m", "MIN_1@240000", 10_000),
                windowMaterialization("window-pm-1m", "pm-1m", "MIN_1@300000", 10_000),
                windowMaterialization("window-kpi-1m", "kpi-1m", "MIN_1@60000", 10_000),
                windowMaterialization("window-kpi-1m", "kpi-1m", "MIN_1@120000", 10_000),
                windowMaterialization("window-kpi-1m", "kpi-1m", "MIN_1@180000", 10_000),
                windowMaterialization("window-kpi-1m", "kpi-1m", "MIN_1@240000", 10_000),
                windowMaterialization("window-kpi-1m", "kpi-1m", "MIN_1@300000", 10_000)))));

    assertThat(result.status()).isEqualTo(BenchmarkStatus.UNSTABLE);
    assertThat(result.bottleneckReason()).contains("5m KPI window materialization");
  }

  @Test
  void six_minute_run_with_kpi_five_minute_probe_input_is_stable() {
    SourceMetricsSnapshot source = new SourceMetricsSnapshot(true, 300_000, 108_000_000, 300_000.0, 360_000, 0, 0,
        10_000, 3_600_000, 10_000.0, 360_000, 0, 0,
        10_000, 10_000, 0.0, 0, 0, 0);
    FlinkSnapshot flink = new FlinkSnapshot("RUNNING", 0, 30_000, 0, 310_000, 310_000,
        126_000_000, 126_000_000, 1, 6, List.of(
            new FlinkOperatorSnapshot("kpi-5m", "kpi-5m-rollup -> kpi-5m-metrics", 6,
                10_000, 0, 10_000, 0, 8_192, 0, 0.2, 0.8, 0.0)));

    BenchmarkRunResult result = engine.decide(plan10000(), healthy()
        .withSource(source)
        .withFlink(flink)
        .withFdb(new FdbMetricsSnapshot(2_000, 40_000, 45_000, 5_000, 0, 20_000,
            List.of(), healthyOneMinuteWindowMetrics())));

    assertThat(result.status()).isEqualTo(BenchmarkStatus.STABLE);
  }

  @Test
  void high_kpi_or_sink_latency_marks_unstable() {
    assertThat(engine.decide(plan(), healthy().withFdb(healthy().fdb().withKpi1mP95Ms(181_000))).status())
        .isEqualTo(BenchmarkStatus.UNSTABLE);
    assertThat(engine.decide(plan(), healthy().withFdb(healthy().fdb().withSinkP95Ms(181_000))).status())
        .isEqualTo(BenchmarkStatus.UNSTABLE);
  }

  @Test
  void high_connector_write_or_commit_latency_marks_unstable() {
    BenchmarkRunResult writeResult = engine.decide(plan(),
        healthy().withFdb(healthy().fdb().withConnectorWriteP95Ms(181_000)));
    BenchmarkRunResult commitResult = engine.decide(plan(),
        healthy().withFdb(healthy().fdb().withConnectorCommitP95Ms(181_000)));

    assertThat(writeResult.status()).isEqualTo(BenchmarkStatus.UNSTABLE);
    assertThat(writeResult.bottleneckReason()).contains("connector write p95");
    assertThat(commitResult.status()).isEqualTo(BenchmarkStatus.UNSTABLE);
    assertThat(commitResult.bottleneckReason()).contains("connector commit p95");
  }

  @Test
  void unavailable_kpi_latency_samples_do_not_mark_run_unstable() {
    RunObservation observation = healthy().withFdb(new FdbMetricsSnapshot(2_000, -1, -1, 5_000, 0, 20_000));

    BenchmarkRunResult result = engine.decide(plan(), observation);

    assertThat(result.status()).isEqualTo(BenchmarkStatus.STABLE);
    assertThat(result.bottleneckReason()).isEqualTo("all thresholds healthy");
  }

  @Test
  void unavailable_sink_latency_sample_does_not_mark_run_unstable() {
    RunObservation observation = healthy().withFdb(healthy().fdb().withSinkP95Ms(-1));

    BenchmarkRunResult result = engine.decide(plan(), observation);

    assertThat(result.status()).isEqualTo(BenchmarkStatus.STABLE);
    assertThat(result.bottleneckReason()).isEqualTo("all thresholds healthy");
  }

  @Test
  void healthy_observation_is_stable() {
    BenchmarkRunResult result = engine.decide(plan(), healthy());

    assertThat(result.status()).isEqualTo(BenchmarkStatus.STABLE);
    assertThat(result.bottleneckReason()).isEqualTo("all thresholds healthy");
  }

  @Test
  void preserves_source_metrics_in_decision_result() {
    SourceMetricsSnapshot source = new SourceMetricsSnapshot(true, 300, 600, 294.0, 2_000, 12, 12,
        100, 1000, 100.0, 10_000, 0, 0,
        250, 1000, 1000.0, 0, 0, 0);
    BenchmarkRunResult result = engine.decide(plan(), healthy().withSource(source));

    assertThat(result.source().present()).isTrue();
    assertThat(result.source().chrTargetEps()).isEqualTo(300);
    assertThat(result.source().pmTargetEps()).isEqualTo(100);
    assertThat(result.source().cfgTargetEps()).isEqualTo(250);
    assertThat(result.source().chrPublished()).isEqualTo(600);
    assertThat(result.source().producerDeliveryRatio()).isEqualTo(0.98);
  }

  private static BenchmarkRunPlan plan() {
    return new BenchmarkRunPlan("bench-a", BenchmarkSink.NONE, 1000,
        0.3, 300, 0.1, 100, "bench-a-none-cells1000-chr-eps0.3", "benchmark-none");
  }

  private static BenchmarkRunPlan plan10000() {
    return new BenchmarkRunPlan("bench-a", BenchmarkSink.HIVE, 10_000,
        30, 300_000, 1, 10_000, "bench-a-hive-cells10000-chr-eps30", "benchmark-hive");
  }

  private static RunObservation healthy() {
    return new RunObservation(
        new FlinkSnapshot("RUNNING", 0.05, 30_000, 0, 10_000, 9_900, 4, 4),
        new FdbMetricsSnapshot(2_000, 40_000, 45_000, 5_000, 0, 20_000),
        new StorageSnapshot(true, "healthy", 100, 0, 0)).withSource(healthySource());
  }

  private static SourceMetricsSnapshot healthySource() {
    return new SourceMetricsSnapshot(true, 300, 600, 294.0, 2_000, 0, 0,
        100, 1000, 100.0, 10_000, 0, 0,
        250, 1000, 250.0, 4_000, 0, 0);
  }

  private static SinkLatencySnapshot windowMaterialization(
      String sinkName, String dataset, String windowKind, long records) {
    return new SinkLatencySnapshot(
        sinkName, "window-materialization", dataset, windowKind, records, 0, 0, 0, 0, 0);
  }

  private static List<SinkLatencySnapshot> healthyOneMinuteWindowMetrics() {
    return List.of(
        windowMaterialization("window-chr-1m", "chr-1m", "MIN_1@60000", 10_000),
        windowMaterialization("window-chr-1m", "chr-1m", "MIN_1@120000", 10_000),
        windowMaterialization("window-chr-1m", "chr-1m", "MIN_1@180000", 10_000),
        windowMaterialization("window-chr-1m", "chr-1m", "MIN_1@240000", 10_000),
        windowMaterialization("window-chr-1m", "chr-1m", "MIN_1@300000", 10_000),
        windowMaterialization("window-pm-1m", "pm-1m", "MIN_1@60000", 10_000),
        windowMaterialization("window-pm-1m", "pm-1m", "MIN_1@120000", 10_000),
        windowMaterialization("window-pm-1m", "pm-1m", "MIN_1@180000", 10_000),
        windowMaterialization("window-pm-1m", "pm-1m", "MIN_1@240000", 10_000),
        windowMaterialization("window-pm-1m", "pm-1m", "MIN_1@300000", 10_000),
        windowMaterialization("window-kpi-1m", "kpi-1m", "MIN_1@60000", 10_000),
        windowMaterialization("window-kpi-1m", "kpi-1m", "MIN_1@120000", 10_000),
        windowMaterialization("window-kpi-1m", "kpi-1m", "MIN_1@180000", 10_000),
        windowMaterialization("window-kpi-1m", "kpi-1m", "MIN_1@240000", 10_000),
        windowMaterialization("window-kpi-1m", "kpi-1m", "MIN_1@300000", 10_000));
  }
}

package com.fdb.benchmark;

import static org.assertj.core.api.Assertions.assertThat;

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
  void sustained_backpressure_marks_unstable() {
    RunObservation observation = healthy().withFlink(healthy().flink().withBackpressureRatio(0.45));

    BenchmarkRunResult result = engine.decide(plan(), observation);

    assertThat(result.status()).isEqualTo(BenchmarkStatus.UNSTABLE);
    assertThat(result.bottleneckReason()).contains("backpressure");
  }

  @Test
  void high_kpi_or_sink_latency_marks_unstable() {
    assertThat(engine.decide(plan(), healthy().withFdb(healthy().fdb().withKpi1mP95Ms(181_000))).status())
        .isEqualTo(BenchmarkStatus.UNSTABLE);
    assertThat(engine.decide(plan(), healthy().withFdb(healthy().fdb().withSinkP95Ms(181_000))).status())
        .isEqualTo(BenchmarkStatus.UNSTABLE);
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
    return new BenchmarkRunPlan("bench-a", BenchmarkSink.NONE, 1000, 300, "bench-a-none-cells1000-eps300",
        "benchmark-none");
  }

  private static RunObservation healthy() {
    return new RunObservation(
        new FlinkSnapshot("RUNNING", 0.05, 30_000, 0, 10_000, 9_900, 4, 4),
        new FdbMetricsSnapshot(2_000, 40_000, 45_000, 5_000, 0, 20_000),
        new StorageSnapshot(true, "healthy", 100, 0, 0));
  }
}

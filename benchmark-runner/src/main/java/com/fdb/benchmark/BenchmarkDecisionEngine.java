package com.fdb.benchmark;

public final class BenchmarkDecisionEngine {
  private final BenchmarkThresholds thresholds;

  public BenchmarkDecisionEngine(BenchmarkThresholds thresholds) {
    this.thresholds = thresholds;
  }

  public BenchmarkRunResult decide(BenchmarkRunPlan plan, RunObservation observation) {
    FlinkSnapshot flink = observation.flink();
    FdbMetricsSnapshot fdb = observation.fdb();
    StorageSnapshot storage = observation.storage();

    if (!"RUNNING".equalsIgnoreCase(flink.jobStatus())) {
      return result(plan, BenchmarkStatus.FAILED, "Flink job status " + flink.jobStatus(), observation);
    }
    if (flink.backpressureRatio() > thresholds.maxBackpressureRatio()) {
      return result(plan, BenchmarkStatus.UNSTABLE, "sustained backpressure ratio " + flink.backpressureRatio(),
          observation);
    }
    if (flink.consecutiveCheckpointFailures() >= thresholds.maxConsecutiveCheckpointFailures()) {
      return result(plan, BenchmarkStatus.UNSTABLE, "checkpoint failures " + flink.consecutiveCheckpointFailures(),
          observation);
    }
    if (flink.checkpointDurationMs() > thresholds.maxCheckpointDurationMs()) {
      return result(plan, BenchmarkStatus.UNSTABLE, "checkpoint duration " + flink.checkpointDurationMs() + " ms",
          observation);
    }
    if (Math.max(fdb.kpi1mP95Ms(), fdb.kpi5mP95Ms()) > thresholds.maxKpiAvailabilityP95Ms()) {
      return result(plan, BenchmarkStatus.UNSTABLE, "KPI availability p95 over threshold", observation);
    }
    if (fdb.sinkP95Ms() > thresholds.maxSinkP95Ms()) {
      return result(plan, BenchmarkStatus.UNSTABLE, "sink p95 over threshold", observation);
    }
    if (fdb.sinkFailures() > 0) {
      return result(plan, BenchmarkStatus.UNSTABLE, "sink failures " + fdb.sinkFailures(), observation);
    }
    if (fdb.watermarkLagMs() > thresholds.maxWatermarkLagMs()) {
      return result(plan, BenchmarkStatus.UNSTABLE, "watermark lag " + fdb.watermarkLagMs() + " ms", observation);
    }
    if (!storage.healthy()) {
      return result(plan, BenchmarkStatus.UNSTABLE, "storage probe unhealthy: " + storage.summary(), observation);
    }
    return result(plan, BenchmarkStatus.STABLE, "all thresholds healthy", observation);
  }

  private static BenchmarkRunResult result(BenchmarkRunPlan plan, BenchmarkStatus status, String reason,
      RunObservation observation) {
    return new BenchmarkRunResult(plan, status, reason, observation.flink(), observation.fdb(), observation.storage());
  }
}

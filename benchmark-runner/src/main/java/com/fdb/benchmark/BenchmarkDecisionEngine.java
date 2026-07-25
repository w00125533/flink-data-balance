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
    SourceMetricsSnapshot source = observation.source();

    if ("UNKNOWN".equalsIgnoreCase(flink.jobStatus())) {
      return result(plan, BenchmarkStatus.UNSTABLE, "Flink job status UNKNOWN", observation);
    }
    if (!"RUNNING".equalsIgnoreCase(flink.jobStatus())) {
      return result(plan, BenchmarkStatus.FAILED, "Flink job status " + flink.jobStatus(), observation);
    }
    if (!source.present()) {
      return result(plan, BenchmarkStatus.UNSTABLE, "source metrics missing", observation);
    }
    if (!source.hasChrMetrics()) {
      return result(plan, BenchmarkStatus.UNSTABLE, "CHR source metrics missing", observation);
    }
    WindowMaterializationSnapshot windows = WindowMaterializationSnapshot.from(plan, source, fdb, flink);
    if (windows.applicable() && !windows.healthy()) {
      return result(plan, BenchmarkStatus.UNSTABLE, windows.bottleneckReason(), observation);
    }
    if (source.producerDeliveryRatio() < thresholds.minProducerDeliveryRatio()) {
      return result(plan, BenchmarkStatus.UNSTABLE,
          "source throughput attainment " + source.producerDeliveryRatio(), observation);
    }
    if (flink.sourceBacklogRecords() > thresholds.maxSourceBacklogRecords()) {
      return result(plan, BenchmarkStatus.UNSTABLE,
          "max source operator backlog " + flink.sourceBacklogRecords() + " records", observation);
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
    if (maxAvailableLatency(fdb.kpi1mP95Ms(), fdb.kpi5mP95Ms()) > thresholds.maxKpiAvailabilityP95Ms()) {
      return result(plan, BenchmarkStatus.UNSTABLE, "KPI availability p95 over threshold", observation);
    }
    if (fdb.sinkP95Ms() >= 0 && fdb.sinkP95Ms() > thresholds.maxSinkP95Ms()) {
      return result(plan, BenchmarkStatus.UNSTABLE, "sink p95 over threshold", observation);
    }
    if (fdb.connectorWriteP95Ms() >= 0 && fdb.connectorWriteP95Ms() > thresholds.maxSinkP95Ms()) {
      return result(plan, BenchmarkStatus.UNSTABLE, "connector write p95 over threshold", observation);
    }
    if (fdb.connectorCommitP95Ms() >= 0 && fdb.connectorCommitP95Ms() > thresholds.maxSinkP95Ms()) {
      return result(plan, BenchmarkStatus.UNSTABLE, "connector commit p95 over threshold", observation);
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
    return new BenchmarkRunResult(plan, status, reason, observation.flink(), observation.fdb(), observation.storage(),
        observation.topology(), observation.source());
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
}

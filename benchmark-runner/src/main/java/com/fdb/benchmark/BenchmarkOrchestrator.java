package com.fdb.benchmark;

import java.util.ArrayList;
import java.util.List;

public final class BenchmarkOrchestrator {
  private static final long POST_CLEANUP_STORAGE_REFRESH_MIN_TIMEOUT_SEC = 30;
  private static final long POST_CLEANUP_STORAGE_REFRESH_MAX_SLEEP_SEC = 5;
  private static final int POST_CLEANUP_FLINK_METRICS_REFRESH_ATTEMPTS = 3;
  private static final long POST_CLEANUP_FLINK_METRICS_REFRESH_SLEEP_MS = 2_000L;

  private final BenchmarkConfig config;
  private final DeployCommandClient deploy;
  private final SimulatorProcessManager simulators;
  private final BenchmarkClients clients;
  private final BenchmarkDecisionEngine decisionEngine;

  public BenchmarkOrchestrator(
      BenchmarkConfig config,
      DeployCommandClient deploy,
      SimulatorProcessManager simulators,
      BenchmarkClients clients,
      BenchmarkDecisionEngine decisionEngine) {
    this.config = config;
    this.deploy = deploy;
    this.simulators = simulators;
    this.clients = clients;
    this.decisionEngine = decisionEngine;
  }

  public List<BenchmarkRunResult> run() throws Exception {
    List<BenchmarkRunResult> results = new ArrayList<>();
    BenchmarkSink currentSink = null;
    boolean skipCurrentSink = false;

    for (BenchmarkRunPlan plan : BenchmarkMatrix.expand(config)) {
      if (currentSink != plan.sink()) {
        currentSink = plan.sink();
        skipCurrentSink = false;
      }
      if (skipCurrentSink) {
        continue;
      }
      BenchmarkRunResult result = null;
      try {
        deploy.prepare(plan);
        deploy.submit(plan);
        sleepSeconds(config.warmupSec());
        simulators.start(plan);
        if (config.simulationDurationSec() > 0) {
          simulators.awaitCompletion(simulatorCompletionTimeoutSec());
        } else {
          simulators.stop();
        }
        result = observeDrain(plan);
      } catch (Exception e) {
        result = failedResult(plan, e);
      }
      Exception cleanupFailure = cleanup(plan);
      if (cleanupFailure == null && result != null && shouldRefreshFlinkAfterCleanup(result)) {
        result = refreshFlinkAfterCleanup(plan, result);
      }
      if (cleanupFailure == null && result != null && shouldRefreshStorageAfterCleanup(plan, result)) {
        result = refreshStorageAfterCleanup(plan, result);
      }
      if (cleanupFailure != null && (result == null || result.status() == BenchmarkStatus.STABLE)) {
        // Cleanup failures make a nominally healthy run unusable, but keep the
        // original submit/observe failure if the run had already failed.
        result = failedResult(plan, cleanupFailure);
      }
      results.add(result);
      if (config.earlyStopOnUnstable() && result.status() != BenchmarkStatus.STABLE) {
        skipCurrentSink = true;
      }
    }
    return List.copyOf(results);
  }

  private Exception cleanup(BenchmarkRunPlan plan) {
    Exception failure = null;
    try {
      simulators.stop();
    } catch (Exception e) {
      failure = e;
    }
    try {
      deploy.stop(plan);
    } catch (Exception e) {
      if (failure == null) {
        failure = e;
      } else {
        failure.addSuppressed(e);
      }
    }
    return failure;
  }

  private BenchmarkRunResult observeDrain(BenchmarkRunPlan plan) throws Exception {
    long deadline = System.currentTimeMillis() + (config.drainTimeoutSec() * 1000);
    List<RunObservation> observations = new ArrayList<>();
    RunObservation observation = clients.observe(plan);
    observations.add(observation);
    BenchmarkRunResult last = decisionEngine.decide(plan, observation);
    if (config.drainTimeoutSec() <= 0 || shouldStopCurrentRunDuringDrain(last) || windowsReady(plan, observation)) {
      return decisionEngine.decide(plan, measurementWindow(observations));
    }
    while (System.currentTimeMillis() < deadline) {
      long remainingMs = deadline - System.currentTimeMillis();
      long sleepMs = Math.min(config.pollIntervalSec() * 1000, Math.max(remainingMs, 0));
      if (sleepMs > 0) {
        Thread.sleep(sleepMs);
      }
      observation = clients.observe(plan);
      observations.add(observation);
      last = decisionEngine.decide(plan, observation);
      if (shouldStopCurrentRunDuringDrain(last) || windowsReady(plan, observation)) {
        return decisionEngine.decide(plan, measurementWindow(observations));
      }
    }
    return decisionEngine.decide(plan, measurementWindow(observations));
  }

  private boolean shouldStopCurrentRunDuringDrain(BenchmarkRunResult result) {
    return result.status() == BenchmarkStatus.FAILED;
  }

  private boolean shouldRefreshStorageAfterCleanup(BenchmarkRunPlan plan, BenchmarkRunResult result) {
    return result.status() != BenchmarkStatus.FAILED
        && (plan.sink() == BenchmarkSink.STARROCKS
            || plan.sink() == BenchmarkSink.HIVE
            || plan.sink() == BenchmarkSink.ICEBERG);
  }

  private BenchmarkRunResult refreshStorageAfterCleanup(
      BenchmarkRunPlan plan,
      BenchmarkRunResult result) {
    StorageSnapshot baseline = result.storage();
    StorageSnapshot best = baseline;
    int attempts = postCleanupStorageRefreshAttempts();
    for (int attempt = 0; attempt < attempts; attempt++) {
      try {
        StorageSnapshot current = clients.observeStorage(plan);
        if (isBetterStorageSnapshot(best, current)) {
          best = current;
        }
        if (isPostCleanupStorageVisible(plan, baseline, current)) {
          return result.withStorage(current);
        }
      } catch (Exception e) {
        // Keep the pre-cleanup result when the post-cleanup visibility probe is transiently unavailable.
      }
      if (attempt + 1 < attempts && !sleepPostCleanupStoragePoll()) {
        break;
      }
    }
    return result.withStorage(best);
  }

  private BenchmarkRunResult refreshFlinkAfterCleanup(BenchmarkRunPlan plan, BenchmarkRunResult result) {
    FlinkSnapshot best = result.flink();
    for (int attempt = 0; attempt < POST_CLEANUP_FLINK_METRICS_REFRESH_ATTEMPTS; attempt++) {
      try {
        FlinkSnapshot current = clients.observe(plan).flink();
        if (isBetterFlinkSnapshot(best, current)) {
          best = current.withJobStatus(result.flink().jobStatus());
        }
        if (operatorMetricsAvailableCount(best) == best.operators().size() && !best.operators().isEmpty()) {
          return result.withFlink(best);
        }
      } catch (Exception e) {
        // Keep the drain result when post-cleanup Flink REST metrics are transiently unavailable.
      }
      if (attempt + 1 < POST_CLEANUP_FLINK_METRICS_REFRESH_ATTEMPTS && !sleepFlinkMetricsRefresh()) {
        break;
      }
    }
    return result.withFlink(best);
  }

  private static boolean shouldRefreshFlinkAfterCleanup(BenchmarkRunResult result) {
    return result.status() != BenchmarkStatus.FAILED
        && hasPublishedSourceRecords(result.source())
        && hasMissingOperatorMetrics(result.flink());
  }

  private static boolean hasPublishedSourceRecords(SourceMetricsSnapshot source) {
    return source.present()
        && (source.chrPublished() > 0 || source.pmPublished() > 0 || source.cfgPublished() > 0);
  }

  private static boolean hasMissingOperatorMetrics(FlinkSnapshot flink) {
    return !flink.operators().isEmpty()
        && flink.operators().stream().anyMatch(operator -> !operator.metricsAvailable());
  }

  private static boolean isBetterFlinkSnapshot(FlinkSnapshot best, FlinkSnapshot current) {
    int bestAvailable = operatorMetricsAvailableCount(best);
    int currentAvailable = operatorMetricsAvailableCount(current);
    if (currentAvailable > bestAvailable) {
      return true;
    }
    if (currentAvailable == 0 || currentAvailable < bestAvailable) {
      return false;
    }
    return current.recordsInTotal() > best.recordsInTotal()
        || current.recordsOutTotal() > best.recordsOutTotal();
  }

  private static int operatorMetricsAvailableCount(FlinkSnapshot flink) {
    return Math.toIntExact(flink.operators().stream().filter(FlinkOperatorSnapshot::metricsAvailable).count());
  }

  private boolean sleepFlinkMetricsRefresh() {
    try {
      Thread.sleep(POST_CLEANUP_FLINK_METRICS_REFRESH_SLEEP_MS);
      return true;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    }
  }

  private boolean isPostCleanupStorageVisible(
      BenchmarkRunPlan plan,
      StorageSnapshot baseline,
      StorageSnapshot current) {
    if (plan.sink() != BenchmarkSink.STARROCKS) {
      return true;
    }
    return current.healthy() && current.records() > 0 && current.records() >= baseline.records();
  }

  private static boolean isBetterStorageSnapshot(StorageSnapshot best, StorageSnapshot current) {
    if (!best.healthy() && current.healthy()) {
      return true;
    }
    return current.records() > best.records()
        || current.smallFiles() > best.smallFiles()
        || current.inProgressFiles() > best.inProgressFiles();
  }

  private boolean sleepPostCleanupStoragePoll() {
    long intervalSec = postCleanupStoragePollIntervalSec();
    if (intervalSec <= 0) {
      return true;
    }
    try {
      Thread.sleep(intervalSec * 1000);
      return true;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    }
  }

  private int postCleanupStorageRefreshAttempts() {
    long timeoutSec = Math.max(POST_CLEANUP_STORAGE_REFRESH_MIN_TIMEOUT_SEC, config.drainTimeoutSec());
    long intervalSec = postCleanupStoragePollIntervalSec();
    if (intervalSec <= 0) {
      return 25;
    }
    return Math.toIntExact(Math.min(120L, Math.max(2L, 1L + ((timeoutSec + intervalSec - 1L) / intervalSec))));
  }

  private long postCleanupStoragePollIntervalSec() {
    return Math.min(config.pollIntervalSec(), POST_CLEANUP_STORAGE_REFRESH_MAX_SLEEP_SEC);
  }

  private boolean windowsReady(BenchmarkRunPlan plan, RunObservation observation) {
    WindowMaterializationSnapshot windows = WindowMaterializationSnapshot.from(
        plan, observation.source(), observation.fdb());
    return !windows.applicable() || windows.healthy();
  }

  private static RunObservation measurementWindow(List<RunObservation> observations) {
    RunObservation last = observations.get(observations.size() - 1);
    return last.withFlink(FlinkSnapshot.measurementWindow(
        observations.stream().map(RunObservation::flink).toList()));
  }

  private static BenchmarkRunResult failedResult(BenchmarkRunPlan plan, Exception e) {
    String reason = e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage();
    return new BenchmarkRunResult(plan, BenchmarkStatus.FAILED, reason,
        new FlinkSnapshot("FAILED", 0, 0, 0, 0, 0, 0, 0),
        new FdbMetricsSnapshot(-1, -1, -1, -1, 0, 0),
        new StorageSnapshot(false, "benchmark failed: " + reason, 0, 0, 0));
  }

  private static void sleepSeconds(long seconds) throws InterruptedException {
    if (seconds > 0) {
      Thread.sleep(seconds * 1000);
    }
  }

  private long simulatorCompletionTimeoutSec() {
    long graceSec = Math.max(60L, config.drainTimeoutSec());
    return config.simulationDurationSec() + graceSec;
  }
}

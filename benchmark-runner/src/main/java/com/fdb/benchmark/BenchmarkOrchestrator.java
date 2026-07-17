package com.fdb.benchmark;

import java.util.ArrayList;
import java.util.List;

public final class BenchmarkOrchestrator {
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
        simulators.start(plan);
        deploy.submit(plan);
        sleepSeconds(config.warmupSec());
        result = observeMeasurement(plan);
      } catch (Exception e) {
        result = failedResult(plan, e);
      }
      Exception cleanupFailure = cleanup(plan);
      if (cleanupFailure != null && (result == null || result.status() == BenchmarkStatus.STABLE)) {
        // Cleanup failures make a nominally healthy run unusable, but keep the
        // original submit/observe failure if the run had already failed.
        result = failedResult(plan, cleanupFailure);
      }
      results.add(result);
      if (result.status() != BenchmarkStatus.STABLE) {
        skipCurrentSink = true;
      }
    }
    return List.copyOf(results);
  }

  private Exception cleanup(BenchmarkRunPlan plan) {
    Exception failure = null;
    try {
      deploy.stop(plan);
    } catch (Exception e) {
      failure = e;
    }
    try {
      simulators.stop();
    } catch (Exception e) {
      if (failure == null) {
        failure = e;
      } else {
        failure.addSuppressed(e);
      }
    }
    return failure;
  }

  private BenchmarkRunResult observeMeasurement(BenchmarkRunPlan plan) throws Exception {
    long deadline = System.currentTimeMillis() + (config.durationSec() * 1000);
    BenchmarkRunResult last = decideOnce(plan);
    if (config.durationSec() <= 0 || last.status() != BenchmarkStatus.STABLE) {
      return last;
    }
    while (System.currentTimeMillis() < deadline) {
      long remainingMs = deadline - System.currentTimeMillis();
      long sleepMs = Math.min(config.pollIntervalSec() * 1000, Math.max(remainingMs, 0));
      if (sleepMs > 0) {
        Thread.sleep(sleepMs);
      }
      last = decideOnce(plan);
      if (last.status() != BenchmarkStatus.STABLE) {
        return last;
      }
    }
    return last;
  }

  private BenchmarkRunResult decideOnce(BenchmarkRunPlan plan) throws Exception {
    RunObservation observation = clients.observe(plan);
    return decisionEngine.decide(plan, observation);
  }

  private static BenchmarkRunResult failedResult(BenchmarkRunPlan plan, Exception e) {
    String reason = e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage();
    return new BenchmarkRunResult(plan, BenchmarkStatus.FAILED, reason,
        new FlinkSnapshot("FAILED", 0, 0, 0, 0, 0, 0, 0),
        new FdbMetricsSnapshot(0, 0, 0, 0, 0, 0),
        new StorageSnapshot(false, "benchmark failed: " + reason, 0, 0, 0));
  }

  private static void sleepSeconds(long seconds) throws InterruptedException {
    if (seconds > 0) {
      Thread.sleep(seconds * 1000);
    }
  }
}

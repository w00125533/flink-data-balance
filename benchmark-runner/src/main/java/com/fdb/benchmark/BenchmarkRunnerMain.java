package com.fdb.benchmark;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

public final class BenchmarkRunnerMain {
  private BenchmarkRunnerMain() {
  }

  public static void main(String[] args) {
    System.exit(run(args));
  }

  static int run(String[] args) {
    try {
      ParsedArgs parsed = ParsedArgs.parse(args);
      if (parsed.help()) {
        usage();
        return 0;
      }
      Map<String, String> env = EnvFile.load(parsed.envFile(), System.getenv());
      env.putAll(parsed.overrides());
      env.put("FDB_ENV_FILE", parsed.envFile().toString());
      BenchmarkConfig config = BenchmarkConfig.from(parsed.target(), env);
      List<BenchmarkRunResult> results;
      if (parsed.dryRun()) {
        results = BenchmarkMatrix.expand(config).stream()
            .map(plan -> new BenchmarkDecisionEngine(config.thresholds()).decide(plan, dryRunObservation(config, plan)))
            .toList();
      } else {
        results = defaultOrchestrator(config, env).run();
      }
      new BenchmarkResultWriter(config.outputRoot()).write(config, results);
      Path index = new HtmlReportWriter(config.outputRoot()).write(config, results);
      System.out.println("benchmark report: " + index);
      return 0;
    } catch (Exception e) {
      System.err.println("[ERROR] " + e.getMessage());
      return 1;
    }
  }

  private static BenchmarkOrchestrator defaultOrchestrator(BenchmarkConfig config, Map<String, String> env) {
    ProcessCommandRunner commandRunner = new ProcessCommandRunner();
    return new BenchmarkOrchestrator(
        config,
        new ShellDeployCommandClient(config.target(), commandRunner, env),
        new JavaSimulatorProcessManager(env, config.outputRoot()),
        new DefaultBenchmarkClients(config, new JdkHttpGateway(), commandRunner, env),
        new BenchmarkDecisionEngine(config.thresholds()));
  }

  private static RunObservation dryRunObservation(BenchmarkConfig config, BenchmarkRunPlan plan) {
    return new RunObservation(
        new FlinkSnapshot("RUNNING", 0.03, 10_000, 0, 1000, 1000, 8192, 8192, 1, 4, List.of(
            new FlinkOperatorSnapshot("dry-source", "dry-source-chr", 4, 1000, 1000, 8192, 8192, 0.30, 0.67, 0.03),
            new FlinkOperatorSnapshot("dry-kpi", "dry-kpi-1m", 4, 1000, 250, 8192, 2048, 0.45, 0.52, 0.03),
            new FlinkOperatorSnapshot("dry-sink-op", "dry-sink", 4, 250, 250, 2048, 2048, 0.35, 0.62, 0.03)),
            List.of(
                new FlinkOperatorEdge("dry-source", "dry-kpi"),
                new FlinkOperatorEdge("dry-kpi", "dry-sink-op"))),
        new FdbMetricsSnapshot(500, 1000, 3000, 1000, 0, 1000, List.of(
            new StageLatencySnapshot("dry-source-chr", 250, 500, 800, 1000),
            new StageLatencySnapshot("dry-kpi-1m", 600, 1000, 1500, 900),
            new StageLatencySnapshot("dry-kpi-5m", 1800, 3000, 4500, 800)),
            List.of(new SinkLatencySnapshot("dry-sink", 250, 2048, 600, 1000, 1800, 0))),
        new StorageSnapshot(true, "dry-run", 0, 0, 0)).withSource(dryRunSource(config, plan));
  }

  private static SourceMetricsSnapshot dryRunSource(BenchmarkConfig config, BenchmarkRunPlan plan) {
    long chrTargetEps = plan.targetChrTotalEps();
    long chrDurationMs = 2_000;
    long chrPublished = Math.round(chrTargetEps * (chrDurationMs / 1000.0d));
    double chrObservedEps = chrTargetEps * 0.98d;
    long pmTargetEps = plan.targetPmTotalEps();
    long cfgTargetEps = Math.max(1L, Math.round(chrTargetEps / 6.0d));
    return new SourceMetricsSnapshot(true, chrTargetEps, chrPublished, chrObservedEps, chrDurationMs, 0, 0,
        pmTargetEps, pmTargetEps * 10, pmTargetEps, 10_000, 0, 0,
        cfgTargetEps, cfgTargetEps * 8, cfgTargetEps, 4_000, 0, 0);
  }

  private static void usage() {
    System.out.println("Usage: benchmark-runner <local|external-yarn> [--env <file>] [--set KEY=VALUE]... [--dry-run]");
  }
}

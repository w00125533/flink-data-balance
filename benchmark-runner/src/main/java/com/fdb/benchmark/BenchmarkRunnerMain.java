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
      env.put("FDB_ENV_FILE", parsed.envFile().toString());
      BenchmarkConfig config = BenchmarkConfig.from(parsed.target(), env);
      List<BenchmarkRunResult> results;
      if (parsed.dryRun()) {
        results = BenchmarkMatrix.expand(config).stream()
            .map(plan -> new BenchmarkDecisionEngine(config.thresholds()).decide(plan, dryRunObservation(plan)))
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
        new JavaSimulatorProcessManager(env),
        new DefaultBenchmarkClients(config, new JdkHttpGateway(), commandRunner, env),
        new BenchmarkDecisionEngine(config.thresholds()));
  }

  private static RunObservation dryRunObservation(BenchmarkRunPlan plan) {
    return new RunObservation(
        new FlinkSnapshot("RUNNING", 0.0, 10_000, 0, 1000, 1000, 1, 4),
        new FdbMetricsSnapshot(0, 1000, 1000, 1000, 0, 1000),
        new StorageSnapshot(true, "dry-run", 0, 0, 0)).withSource(dryRunSource(plan));
  }

  private static SourceMetricsSnapshot dryRunSource(BenchmarkRunPlan plan) {
    long chrTargetEps = plan.targetChrEps();
    long chrDurationMs = 2_000;
    long chrPublished = Math.round(chrTargetEps * (chrDurationMs / 1000.0d));
    double chrObservedEps = chrTargetEps * 0.98d;
    long pmTargetEps = Math.max(1L, Math.round(chrTargetEps / 3.0d));
    long cfgTargetEps = Math.max(1L, Math.round(chrTargetEps / 6.0d));
    return new SourceMetricsSnapshot(true, chrTargetEps, chrPublished, chrObservedEps, chrDurationMs, 0, 0,
        pmTargetEps, pmTargetEps * 10, pmTargetEps, 10_000, 0, 0,
        cfgTargetEps, cfgTargetEps * 8, cfgTargetEps, 4_000, 0, 0);
  }

  private static void usage() {
    System.out.println("Usage: benchmark-runner <local|external-yarn> [--env <file>] [--dry-run]");
  }
}

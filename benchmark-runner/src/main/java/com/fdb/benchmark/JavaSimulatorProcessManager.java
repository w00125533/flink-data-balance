package com.fdb.benchmark;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public final class JavaSimulatorProcessManager implements SimulatorProcessManager {
  private static final String ANOMALY_RATIO_ENV = "FDB_BENCHMARK_ANOMALY_INJECTION_RATIO";
  private static final String DEFAULT_ANOMALY_RATIO = "0.05";
  private static final String DEFAULT_PM_EPS_PER_CELL = "1.0";
  private static final String DEFAULT_MAX_OUT_OF_ORDER_LAG_MS = "2000";
  private static final String DEFAULT_SIMULATION_DURATION_SEC = "300";
  private static final long STOP_GRACE_MS = 5_000;
  private static final long FORCE_STOP_GRACE_MS = 5_000;

  private final Map<String, String> baseEnv;
  private final Path outputRoot;
  private final Path logsDir;
  private final List<ManagedSimulatorProcess> processes = new ArrayList<>();

  public JavaSimulatorProcessManager(Map<String, String> baseEnv) {
    this(baseEnv, BenchmarkPaths.outputRoot());
  }

  public JavaSimulatorProcessManager(Map<String, String> baseEnv, Path outputRoot) {
    this(baseEnv, outputRoot, BenchmarkPaths.logsDir());
  }

  JavaSimulatorProcessManager(Map<String, String> baseEnv, Path outputRoot, Path logsDir) {
    this.baseEnv = Map.copyOf(baseEnv);
    this.outputRoot = outputRoot;
    this.logsDir = logsDir;
  }

  @Override
  public void start(BenchmarkRunPlan plan) throws Exception {
    stop();
    Map<String, String> env = envFor(plan);
    runTopology(env);
    processes.add(startManagedProcess("simulator-cfg", env,
        List.of("java", "-jar", "simulator/target/simulator-0.1.0-SNAPSHOT.jar", "cfg")));
    processes.add(startManagedProcess("simulator-pm", env,
        List.of("java", "-jar", "simulator/target/simulator-0.1.0-SNAPSHOT.jar", "pm")));
    processes.add(startManagedProcess("simulator-chr", env,
        List.of("java", "-jar", "simulator/target/simulator-0.1.0-SNAPSHOT.jar", "chr")));
  }

  @Override
  public void awaitCompletion(long timeoutSec) throws Exception {
    long deadlineMs = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(Math.max(0L, timeoutSec));
    List<ManagedSimulatorProcess> tracked = List.copyOf(processes);
    for (ManagedSimulatorProcess managed : tracked) {
      if (!waitForExitBeforeDeadline(managed.process(), deadlineMs)) {
        throw new IOException("simulator processes did not finish within " + timeoutSec
            + "s; timed out waiting for " + managed.name());
      }
      int exitCode = managed.process().exitValue();
      if (exitCode != 0) {
        throw new IOException(managed.name() + " exited with " + exitCode);
      }
    }
    processes.removeAll(tracked);
  }

  @Override
  public void stop() {
    for (ManagedSimulatorProcess managed : processes) {
      Process process = managed.process();
      if (process.isAlive()) {
        process.destroy();
      }
    }
    for (ManagedSimulatorProcess managed : processes) {
      Process process = managed.process();
      if (process.isAlive()) {
        waitForExit(process, STOP_GRACE_MS);
      }
      if (process.isAlive()) {
        process.destroyForcibly();
      }
    }
    for (ManagedSimulatorProcess managed : processes) {
      Process process = managed.process();
      if (process.isAlive()) {
        waitForExit(process, FORCE_STOP_GRACE_MS);
      }
    }
    processes.clear();
  }

  private static void waitForExit(Process process, long timeoutMs) {
    try {
      process.waitFor(timeoutMs, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private static boolean waitForExitBeforeDeadline(Process process, long deadlineMs) throws InterruptedException {
    long remainingMs = deadlineMs - System.currentTimeMillis();
    if (remainingMs <= 0L) {
      return !process.isAlive();
    }
    return process.waitFor(remainingMs, TimeUnit.MILLISECONDS);
  }

  private void runTopology(Map<String, String> env) throws IOException, InterruptedException {
    Process process = startProcess("topology-service", env,
        List.of("java", "-jar", "topology-service/target/topology-service-0.1.0-SNAPSHOT.jar"));
    int exitCode = process.waitFor();
    if (exitCode != 0) {
      throw new IOException("topology-service exited with " + exitCode);
    }
  }

  private Process startProcess(String name, Map<String, String> env, List<String> command) throws IOException {
    Files.createDirectories(logsDir);
    ProcessBuilder builder = new ProcessBuilder(command);
    builder.environment().putAll(env);
    builder.redirectOutput(ProcessBuilder.Redirect.appendTo(logFile(name, "out").toFile()));
    builder.redirectError(ProcessBuilder.Redirect.appendTo(logFile(name, "err").toFile()));
    return builder.start();
  }

  private ManagedSimulatorProcess startManagedProcess(
      String name, Map<String, String> env, List<String> command) throws IOException {
    return new ManagedSimulatorProcess(name, startProcess(name, env, command));
  }

  Path logFile(String name, String extension) {
    return logsDir.resolve("benchmark-" + name + "." + extension);
  }

  void trackForTesting(Process process) {
    processes.add(new ManagedSimulatorProcess("test-process", process));
  }

  Map<String, String> envFor(BenchmarkRunPlan plan) {
    Map<String, String> env = new HashMap<>(baseEnv);
    String hostBootstrap = baseEnv.get("FDB_KAFKA_HOST_BOOTSTRAP");
    if (hostBootstrap != null && !hostBootstrap.isBlank()) {
      env.put("FDB_KAFKA_BOOTSTRAP", hostBootstrap);
    }
    env.put("FDB_SITES_COUNT", String.valueOf(plan.cellLevel()));
    env.put("FDB_TOPOLOGY_TARGET_CELLS", String.valueOf(plan.cellLevel()));
    env.put("FDB_RATE_EPS", String.valueOf(plan.targetChrTotalEps()));
    env.put("FDB_RATE_MAX_OUT_OF_ORDER_LAG_MS", valueOrDefault(
        baseEnv.get("FDB_RATE_MAX_OUT_OF_ORDER_LAG_MS"), DEFAULT_MAX_OUT_OF_ORDER_LAG_MS));
    env.put("FDB_PM_MAX_OUT_OF_ORDER_LAG_MS", valueOrDefault(
        baseEnv.get("FDB_PM_MAX_OUT_OF_ORDER_LAG_MS"), DEFAULT_MAX_OUT_OF_ORDER_LAG_MS));
    env.put("FDB_PM_EPS_PER_CELL", valueOrDefault(baseEnv.get("FDB_PM_EPS_PER_CELL"),
        valueOrDefault(baseEnv.get("FDB_BENCHMARK_PM_EPS_PER_CELL"), DEFAULT_PM_EPS_PER_CELL)));
    env.put("FDB_SIMULATION_DURATION_SEC", valueOrDefault(baseEnv.get("FDB_SIMULATION_DURATION_SEC"),
        valueOrDefault(baseEnv.get("FDB_BENCHMARK_SIMULATION_DURATION_SEC"),
            valueOrDefault(baseEnv.get("FDB_BENCHMARK_DURATION_SEC"), DEFAULT_SIMULATION_DURATION_SEC))));
    env.put("FDB_E2E_SUMMARY", "1");
    env.put("FDB_SIM_SUMMARY", "1");
    env.put("FDB_TOPOLOGY_SUMMARY", "1");
    env.put(ANOMALY_RATIO_ENV, valueOrDefault(baseEnv.get(ANOMALY_RATIO_ENV), DEFAULT_ANOMALY_RATIO));
    env.put("FDB_TOPOLOGY_METRICS_FILE", portablePath(TopologyMetricsSnapshot.metricsFile(outputRoot, plan)));
    Path runDir = SourceMetricsSnapshot.runDir(outputRoot, plan);
    env.put("FDB_CHR_METRICS_FILE", portablePath(runDir.resolve("chr-source-metrics.json")));
    env.put("FDB_PM_METRICS_FILE", portablePath(runDir.resolve("pm-source-metrics.json")));
    env.put("FDB_CFG_METRICS_FILE", portablePath(runDir.resolve("cfg-source-metrics.json")));
    return env;
  }

  private static String portablePath(Path path) {
    return path.toString().replace('\\', '/');
  }

  private static String valueOrDefault(String value, String defaultValue) {
    return value == null || value.isBlank() ? defaultValue : value.trim();
  }

  private record ManagedSimulatorProcess(String name, Process process) {
  }
}

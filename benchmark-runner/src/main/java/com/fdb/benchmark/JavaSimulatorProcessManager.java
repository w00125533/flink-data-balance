package com.fdb.benchmark;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class JavaSimulatorProcessManager implements SimulatorProcessManager {
  private final Map<String, String> baseEnv;
  private final List<Process> processes = new ArrayList<>();

  public JavaSimulatorProcessManager(Map<String, String> baseEnv) {
    this.baseEnv = Map.copyOf(baseEnv);
  }

  @Override
  public void start(BenchmarkRunPlan plan) throws Exception {
    stop();
    Map<String, String> env = envFor(plan);
    runTopology(env);
    processes.add(startProcess("simulator-cfg", env,
        List.of("java", "-jar", "simulator/target/simulator-0.1.0-SNAPSHOT.jar", "cfg")));
    processes.add(startProcess("simulator-pm", env,
        List.of("java", "-jar", "simulator/target/simulator-0.1.0-SNAPSHOT.jar", "pm")));
    processes.add(startProcess("simulator-chr", env,
        List.of("java", "-jar", "simulator/target/simulator-0.1.0-SNAPSHOT.jar", "chr")));
  }

  @Override
  public void stop() {
    for (Process process : processes) {
      if (process.isAlive()) {
        process.destroy();
      }
    }
    for (Process process : processes) {
      if (process.isAlive()) {
        process.destroyForcibly();
      }
    }
    processes.clear();
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
    Files.createDirectories(Path.of("logs"));
    ProcessBuilder builder = new ProcessBuilder(command);
    builder.environment().putAll(env);
    builder.redirectOutput(ProcessBuilder.Redirect.appendTo(Path.of("logs", "benchmark-" + name + ".out").toFile()));
    builder.redirectError(ProcessBuilder.Redirect.appendTo(Path.of("logs", "benchmark-" + name + ".err").toFile()));
    return builder.start();
  }

  private Map<String, String> envFor(BenchmarkRunPlan plan) {
    Map<String, String> env = new HashMap<>(baseEnv);
    env.put("FDB_SITES_COUNT", String.valueOf(plan.cellLevel()));
    env.put("FDB_RATE_EPS", String.valueOf(plan.targetChrEps()));
    env.put("FDB_E2E_SUMMARY", "1");
    env.put("FDB_SIM_SUMMARY", "1");
    env.put("FDB_TOPOLOGY_SUMMARY", "1");
    return env;
  }
}

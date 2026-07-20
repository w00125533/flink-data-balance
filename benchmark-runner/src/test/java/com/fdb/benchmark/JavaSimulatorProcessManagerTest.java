package com.fdb.benchmark;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Test;

class JavaSimulatorProcessManagerTest {
  @Test
  void uses_host_kafka_bootstrap_for_local_simulator_processes() {
    JavaSimulatorProcessManager manager = new JavaSimulatorProcessManager(Map.of(
        "FDB_KAFKA_BOOTSTRAP", "kafka:9092",
        "FDB_KAFKA_HOST_BOOTSTRAP", "localhost:9092"));

    Map<String, String> env = manager.envFor(plan());

    assertThat(env.get("FDB_KAFKA_BOOTSTRAP")).isEqualTo("localhost:9092");
    assertThat(env).containsEntry("FDB_TOPOLOGY_TARGET_CELLS", "1000");
    assertThat(env).containsEntry("FDB_SITES_COUNT", "1000");
    assertThat(env.get("FDB_RATE_EPS")).isEqualTo("100");
    assertThat(env.get("FDB_PM_EPS_PER_CELL")).isEqualTo("1.0");
    assertThat(env).containsEntry("FDB_BENCHMARK_ANOMALY_INJECTION_RATIO", "0.05");
    assertThat(env.get("FDB_TOPOLOGY_METRICS_FILE"))
        .endsWith("benchmark-runner/output/benchmark-runs/bench-a/runs/bench-a-none-cells1000-eps1/topology-metrics.json");
    assertThat(env.get("FDB_TOPOLOGY_METRICS_FILE"))
        .doesNotContain("benchmark-runner/benchmark-runner");
  }

  @Test
  void preserves_explicit_anomaly_injection_ratio_env() {
    JavaSimulatorProcessManager manager = new JavaSimulatorProcessManager(Map.of(
        "FDB_BENCHMARK_ANOMALY_INJECTION_RATIO", "0.12"));

    Map<String, String> env = manager.envFor(plan());

    assertThat(env).containsEntry("FDB_BENCHMARK_ANOMALY_INJECTION_RATIO", "0.12");
  }

  @Test
  void passes_explicit_pm_eps_per_cell_to_pm_simulator() {
    JavaSimulatorProcessManager manager = new JavaSimulatorProcessManager(Map.of(
        "FDB_BENCHMARK_PM_EPS_PER_CELL", "2.5"));

    Map<String, String> env = manager.envFor(plan());

    assertThat(env).containsEntry("FDB_PM_EPS_PER_CELL", "2.5");
  }

  @Test
  void writes_process_logs_under_configured_logs_directory() {
    Path logsDir = Path.of("logs-root");
    JavaSimulatorProcessManager manager = new JavaSimulatorProcessManager(
        Map.of(), Path.of("output-root"), logsDir);

    assertThat(manager.logFile("simulator-chr", "out"))
        .isEqualTo(logsDir.resolve("benchmark-simulator-chr.out"));
    assertThat(manager.logFile("simulator-chr", "err"))
        .isEqualTo(logsDir.resolve("benchmark-simulator-chr.err"));
  }

  private static BenchmarkRunPlan plan() {
    return new BenchmarkRunPlan("bench-a", BenchmarkSink.NONE, 1000,
        0.1, 100, 1.0, 1000, "bench-a-none-cells1000-eps1", "small");
  }
}

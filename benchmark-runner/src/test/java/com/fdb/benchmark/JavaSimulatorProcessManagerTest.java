package com.fdb.benchmark;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.TimeUnit;
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
    assertThat(env).containsEntry("FDB_RATE_MAX_OUT_OF_ORDER_LAG_MS", "2000");
    assertThat(env).containsEntry("FDB_PM_MAX_OUT_OF_ORDER_LAG_MS", "2000");
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
  void passes_benchmark_simulation_duration_to_simulators() {
    JavaSimulatorProcessManager manager = new JavaSimulatorProcessManager(Map.of(
        "FDB_BENCHMARK_SIMULATION_DURATION_SEC", "300"));

    Map<String, String> env = manager.envFor(plan());

    assertThat(env).containsEntry("FDB_SIMULATION_DURATION_SEC", "300");
  }

  @Test
  void explicit_simulation_duration_env_wins_over_benchmark_duration() {
    JavaSimulatorProcessManager manager = new JavaSimulatorProcessManager(Map.of(
        "FDB_SIMULATION_DURATION_SEC", "120",
        "FDB_BENCHMARK_SIMULATION_DURATION_SEC", "300"));

    Map<String, String> env = manager.envFor(plan());

    assertThat(env).containsEntry("FDB_SIMULATION_DURATION_SEC", "120");
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

  @Test
  void stop_waits_for_tracked_processes_to_exit_before_forgetting_them() {
    JavaSimulatorProcessManager manager = new JavaSimulatorProcessManager(Map.of());
    RecordingProcess process = new RecordingProcess();
    manager.trackForTesting(process);

    manager.stop();

    assertThat(process.destroyed).isTrue();
    assertThat(process.waited).isTrue();
    assertThat(process.isAlive()).isFalse();
  }

  @Test
  void await_completion_waits_for_tracked_processes_and_removes_completed_ones() throws Exception {
    JavaSimulatorProcessManager manager = new JavaSimulatorProcessManager(Map.of());
    RecordingProcess process = new RecordingProcess();
    manager.trackForTesting(process);

    manager.awaitCompletion(1);
    manager.stop();

    assertThat(process.waited).isTrue();
    assertThat(process.destroyed).isFalse();
    assertThat(process.isAlive()).isFalse();
  }

  @Test
  void await_completion_fails_when_process_does_not_exit_before_timeout() {
    JavaSimulatorProcessManager manager = new JavaSimulatorProcessManager(Map.of());
    RecordingProcess process = new RecordingProcess(false);
    manager.trackForTesting(process);

    assertThatThrownBy(() -> manager.awaitCompletion(0))
        .isInstanceOf(Exception.class)
        .hasMessageContaining("test-process")
        .hasMessageContaining("did not finish");
  }

  private static BenchmarkRunPlan plan() {
    return new BenchmarkRunPlan("bench-a", BenchmarkSink.NONE, 1000,
        0.1, 100, 1.0, 1000, "bench-a-none-cells1000-eps1", "small");
  }

  private static final class RecordingProcess extends Process {
    private boolean alive = true;
    private boolean destroyed;
    private boolean waited;
    private final boolean completeOnWait;

    RecordingProcess() {
      this(true);
    }

    RecordingProcess(boolean completeOnWait) {
      this.completeOnWait = completeOnWait;
    }

    @Override
    public OutputStream getOutputStream() {
      return OutputStream.nullOutputStream();
    }

    @Override
    public InputStream getInputStream() {
      return InputStream.nullInputStream();
    }

    @Override
    public InputStream getErrorStream() {
      return InputStream.nullInputStream();
    }

    @Override
    public int waitFor() {
      waited = true;
      alive = false;
      return 0;
    }

    @Override
    public boolean waitFor(long timeout, TimeUnit unit) {
      waited = true;
      if (completeOnWait) {
        alive = false;
        return true;
      }
      return false;
    }

    @Override
    public int exitValue() {
      if (alive) {
        throw new IllegalThreadStateException();
      }
      return 0;
    }

    @Override
    public void destroy() {
      destroyed = true;
    }

    @Override
    public Process destroyForcibly() {
      alive = false;
      return this;
    }

    @Override
    public boolean isAlive() {
      return alive;
    }
  }
}

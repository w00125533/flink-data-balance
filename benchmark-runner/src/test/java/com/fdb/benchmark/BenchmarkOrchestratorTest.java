package com.fdb.benchmark;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class BenchmarkOrchestratorTest {
  @Test
  void stops_higher_levels_after_first_unstable_run_for_a_sink() throws Exception {
    BenchmarkConfig config = BenchmarkConfig.from("local", Map.of(
        "FDB_BENCHMARK_ID", "bench-a",
        "FDB_BENCHMARK_SINKS", "none",
        "FDB_BENCHMARK_CELL_LEVELS", "1000 3000 9000",
        "FDB_BENCHMARK_CHR_EPS_PER_CELL", "0.1",
        "FDB_BENCHMARK_WARMUP_SEC", "0",
        "FDB_BENCHMARK_DURATION_SEC", "0"));
    RecordingDeploy deploy = new RecordingDeploy();
    RecordingSimulators simulators = new RecordingSimulators();
    FakeObservationSource observations = new FakeObservationSource(List.of(
        healthy(),
        healthy().withFlink(healthy().flink().withBackpressureRatio(0.5))));

    List<BenchmarkRunResult> results = new BenchmarkOrchestrator(
        config,
        deploy,
        simulators,
        observations,
        new BenchmarkDecisionEngine(config.thresholds())).run();

    assertThat(results).hasSize(2);
    assertThat(results).extracting(result -> result.plan().cellLevel()).containsExactly(1000, 3000);
    assertThat(results).extracting(BenchmarkRunResult::status)
        .containsExactly(BenchmarkStatus.STABLE, BenchmarkStatus.UNSTABLE);
    assertThat(deploy.actions).containsExactly(
        "submit:bench-a-none-cells1000-eps100", "stop:bench-a-none-cells1000-eps100",
        "submit:bench-a-none-cells3000-eps300", "stop:bench-a-none-cells3000-eps300");
    assertThat(simulators.actions).contains("start:1000:100", "start:3000:300", "stop", "stop");
  }

  @Test
  void records_submit_failure_as_failed_run_and_stops_next_levels_for_sink() throws Exception {
    BenchmarkConfig config = BenchmarkConfig.from("local", Map.of(
        "FDB_BENCHMARK_ID", "bench-a",
        "FDB_BENCHMARK_SINKS", "none",
        "FDB_BENCHMARK_CELL_LEVELS", "1000 3000",
        "FDB_BENCHMARK_WARMUP_SEC", "0",
        "FDB_BENCHMARK_DURATION_SEC", "0"));
    RecordingDeploy deploy = new RecordingDeploy();
    deploy.failSubmit = true;

    List<BenchmarkRunResult> results = new BenchmarkOrchestrator(
        config,
        deploy,
        new RecordingSimulators(),
        plan -> healthy(),
        new BenchmarkDecisionEngine(config.thresholds())).run();

    assertThat(results).hasSize(1);
    assertThat(results.get(0).status()).isEqualTo(BenchmarkStatus.FAILED);
    assertThat(results.get(0).bottleneckReason()).contains("submit failed");
  }

  private static RunObservation healthy() {
    return new RunObservation(
        new FlinkSnapshot("RUNNING", 0.0, 20_000, 0, 1000, 1000, 1, 4),
        new FdbMetricsSnapshot(1000, 2000, 3000, 4000, 0, 1000),
        new StorageSnapshot(true, "healthy", 100, 0, 0));
  }

  static final class RecordingDeploy implements DeployCommandClient {
    final List<String> actions = new ArrayList<>();
    boolean failSubmit;

    @Override
    public void submit(BenchmarkRunPlan plan) throws Exception {
      actions.add("submit:" + plan.runId());
      if (failSubmit) {
        throw new Exception("submit failed");
      }
    }

    @Override
    public void stop(BenchmarkRunPlan plan) {
      actions.add("stop:" + plan.runId());
    }
  }

  static final class RecordingSimulators implements SimulatorProcessManager {
    final List<String> actions = new ArrayList<>();

    @Override
    public void start(BenchmarkRunPlan plan) {
      actions.add("start:" + plan.cellLevel() + ":" + plan.targetChrEps());
    }

    @Override
    public void stop() {
      actions.add("stop");
    }
  }

  static final class FakeObservationSource implements BenchmarkClients {
    private final List<RunObservation> observations;
    private int index;

    FakeObservationSource(List<RunObservation> observations) {
      this.observations = observations;
    }

    @Override
    public RunObservation observe(BenchmarkRunPlan plan) {
      return observations.get(index++);
    }
  }
}

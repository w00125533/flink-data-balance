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
        "prepare:bench-a-none-cells1000-chr-eps0.1", "submit:bench-a-none-cells1000-chr-eps0.1",
        "stop:bench-a-none-cells1000-chr-eps0.1",
        "prepare:bench-a-none-cells3000-chr-eps0.1", "submit:bench-a-none-cells3000-chr-eps0.1",
        "stop:bench-a-none-cells3000-chr-eps0.1");
    assertThat(simulators.actions).contains("start:1000:100", "start:3000:300", "stop", "stop");
  }

  @Test
  void continues_higher_levels_when_early_stop_is_disabled() throws Exception {
    BenchmarkConfig config = BenchmarkConfig.from("local", Map.of(
        "FDB_BENCHMARK_ID", "bench-a",
        "FDB_BENCHMARK_SINKS", "none",
        "FDB_BENCHMARK_CELL_LEVELS", "1000 3000",
        "FDB_BENCHMARK_CHR_EPS_PER_CELL", "0.1",
        "FDB_BENCHMARK_WARMUP_SEC", "0",
        "FDB_BENCHMARK_DURATION_SEC", "0",
        "FDB_BENCHMARK_EARLY_STOP_ON_UNSTABLE", "false"));
    RecordingDeploy deploy = new RecordingDeploy();
    RecordingSimulators simulators = new RecordingSimulators();
    FakeObservationSource observations = new FakeObservationSource(List.of(
        healthy().withFlink(healthy().flink().withBackpressureRatio(0.5)),
        healthy()));

    List<BenchmarkRunResult> results = new BenchmarkOrchestrator(
        config,
        deploy,
        simulators,
        observations,
        new BenchmarkDecisionEngine(config.thresholds())).run();

    assertThat(results).hasSize(2);
    assertThat(results).extracting(result -> result.plan().cellLevel()).containsExactly(1000, 3000);
    assertThat(results).extracting(BenchmarkRunResult::status)
        .containsExactly(BenchmarkStatus.UNSTABLE, BenchmarkStatus.STABLE);
  }

  @Test
  void stops_current_run_immediately_when_flink_job_is_failed_even_when_early_stop_is_disabled() throws Exception {
    BenchmarkConfig config = BenchmarkConfig.from("local", Map.of(
        "FDB_BENCHMARK_ID", "bench-a",
        "FDB_BENCHMARK_SINKS", "none",
        "FDB_BENCHMARK_CELL_LEVELS", "1000",
        "FDB_BENCHMARK_WARMUP_SEC", "0",
        "FDB_BENCHMARK_DURATION_SEC", "1",
        "FDB_BENCHMARK_POLL_INTERVAL_SEC", "1",
        "FDB_BENCHMARK_EARLY_STOP_ON_UNSTABLE", "false"));
    FakeObservationSource observations = new FakeObservationSource(List.of(
        healthy().withFlink(healthy().flink().withJobStatus("RESTARTING")),
        healthy().withFlink(healthy().flink().withJobStatus("RESTARTING"))));

    List<BenchmarkRunResult> results = new BenchmarkOrchestrator(
        config,
        new RecordingDeploy(),
        new RecordingSimulators(),
        observations,
        new BenchmarkDecisionEngine(config.thresholds())).run();

    assertThat(results).hasSize(1);
    assertThat(results.get(0).status()).isEqualTo(BenchmarkStatus.FAILED);
    assertThat(results.get(0).bottleneckReason()).isEqualTo("Flink job status RESTARTING");
    assertThat(observations.observationCount()).isEqualTo(1);
  }

  @Test
  void submits_job_before_starting_simulators() throws Exception {
    BenchmarkConfig config = BenchmarkConfig.from("local", Map.of(
        "FDB_BENCHMARK_ID", "bench-a",
        "FDB_BENCHMARK_SINKS", "none",
        "FDB_BENCHMARK_CELL_LEVELS", "1000",
        "FDB_BENCHMARK_WARMUP_SEC", "0",
        "FDB_BENCHMARK_DURATION_SEC", "0"));
    List<String> events = new ArrayList<>();
    RecordingDeploy deploy = new RecordingDeploy(events);
    RecordingSimulators simulators = new RecordingSimulators(events);

    new BenchmarkOrchestrator(
        config,
        deploy,
        simulators,
        plan -> healthy(),
        new BenchmarkDecisionEngine(config.thresholds())).run();

    assertThat(events).containsSubsequence(
        "prepare:bench-a-none-cells1000-chr-eps30",
        "submit:bench-a-none-cells1000-chr-eps30",
        "start:1000:30000");
  }

  @Test
  void stops_simulators_before_canceling_job() throws Exception {
    BenchmarkConfig config = BenchmarkConfig.from("local", Map.of(
        "FDB_BENCHMARK_ID", "bench-a",
        "FDB_BENCHMARK_SINKS", "none",
        "FDB_BENCHMARK_CELL_LEVELS", "1000",
        "FDB_BENCHMARK_WARMUP_SEC", "0",
        "FDB_BENCHMARK_DURATION_SEC", "0"));
    List<String> events = new ArrayList<>();
    RecordingDeploy deploy = new RecordingDeploy(events);
    RecordingSimulators simulators = new RecordingSimulators(events);

    new BenchmarkOrchestrator(
        config,
        deploy,
        simulators,
        plan -> healthy(),
        new BenchmarkDecisionEngine(config.thresholds())).run();

    assertThat(events).containsSubsequence(
        "start:1000:30000",
        "stop",
        "stop:bench-a-none-cells1000-chr-eps30");
  }

  @Test
  void stops_simulators_after_fixed_generation_before_drain_observation() throws Exception {
    BenchmarkConfig config = BenchmarkConfig.from("local", Map.of(
        "FDB_BENCHMARK_ID", "bench-a",
        "FDB_BENCHMARK_SINKS", "none",
        "FDB_BENCHMARK_CELL_LEVELS", "1000",
        "FDB_BENCHMARK_WARMUP_SEC", "0",
        "FDB_BENCHMARK_SIMULATION_DURATION_SEC", "0",
        "FDB_BENCHMARK_DRAIN_TIMEOUT_SEC", "0"));
    List<String> events = new ArrayList<>();
    RecordingDeploy deploy = new RecordingDeploy(events);
    RecordingSimulators simulators = new RecordingSimulators(events);
    FakeObservationSource observations = new FakeObservationSource(List.of(healthy()), events);

    new BenchmarkOrchestrator(
        config,
        deploy,
        simulators,
        observations,
        new BenchmarkDecisionEngine(config.thresholds())).run();

    assertThat(events).containsSubsequence(
        "start:1000:30000",
        "stop",
        "observe:bench-a-none-cells1000-chr-eps30",
        "stop:bench-a-none-cells1000-chr-eps30");
  }

  @Test
  void refreshes_business_sink_storage_after_cleanup() throws Exception {
    BenchmarkConfig config = BenchmarkConfig.from("local", Map.of(
        "FDB_BENCHMARK_ID", "bench-a",
        "FDB_BENCHMARK_SINKS", "starrocks",
        "FDB_BENCHMARK_CELL_LEVELS", "1000",
        "FDB_BENCHMARK_WARMUP_SEC", "0",
        "FDB_BENCHMARK_SIMULATION_DURATION_SEC", "0",
        "FDB_BENCHMARK_DRAIN_TIMEOUT_SEC", "0",
        "FDB_BENCHMARK_POLL_INTERVAL_SEC", "0"));
    List<String> events = new ArrayList<>();
    RunObservation beforeCleanup = healthy().withStorage(new StorageSnapshot(true, "starrocks rows=0", 0, 0, 0));
    RunObservation afterCleanup = healthy().withStorage(new StorageSnapshot(true, "starrocks rows=10000", 10_000, 0, 0));
    FakeObservationSource observations = new FakeObservationSource(List.of(beforeCleanup, afterCleanup), events);

    List<BenchmarkRunResult> results = new BenchmarkOrchestrator(
        config,
        new RecordingDeploy(events),
        new RecordingSimulators(events),
        observations,
        new BenchmarkDecisionEngine(config.thresholds())).run();

    assertThat(results).hasSize(1);
    assertThat(results.get(0).storage().records()).isEqualTo(10_000);
    assertThat(results.get(0).storage().summary()).isEqualTo("starrocks rows=10000");
    assertThat(events).containsSubsequence(
        "observe:bench-a-starrocks-cells1000-chr-eps30",
        "stop:bench-a-starrocks-cells1000-chr-eps30",
        "observe:bench-a-starrocks-cells1000-chr-eps30");
  }

  @Test
  void refreshes_business_sink_storage_without_full_observation_after_cleanup() throws Exception {
    BenchmarkConfig config = BenchmarkConfig.from("local", Map.of(
        "FDB_BENCHMARK_ID", "bench-a",
        "FDB_BENCHMARK_SINKS", "starrocks",
        "FDB_BENCHMARK_CELL_LEVELS", "1000",
        "FDB_BENCHMARK_WARMUP_SEC", "0",
        "FDB_BENCHMARK_SIMULATION_DURATION_SEC", "0",
        "FDB_BENCHMARK_DRAIN_TIMEOUT_SEC", "0"));
    List<String> events = new ArrayList<>();
    RunObservation beforeCleanup = healthy().withStorage(new StorageSnapshot(true, "starrocks rows=0", 0, 0, 0));
    FakeObservationSource observations = new FakeObservationSource(
        List.of(beforeCleanup),
        events,
        List.of(
            new StorageSnapshot(true, "starrocks rows=0", 0, 0, 0),
            new StorageSnapshot(true, "starrocks rows=10000", 10_000, 0, 0)),
        1);

    List<BenchmarkRunResult> results = new BenchmarkOrchestrator(
        config,
        new RecordingDeploy(events),
        new RecordingSimulators(events),
        observations,
        new BenchmarkDecisionEngine(config.thresholds())).run();

    assertThat(results).hasSize(1);
    assertThat(results.get(0).storage().records()).isEqualTo(10_000);
    assertThat(events).containsSubsequence(
        "observe:bench-a-starrocks-cells1000-chr-eps30",
        "stop:bench-a-starrocks-cells1000-chr-eps30",
        "storage:bench-a-starrocks-cells1000-chr-eps30",
        "storage:bench-a-starrocks-cells1000-chr-eps30");
    assertThat(observations.observationCount()).isEqualTo(1);
  }

  @Test
  void waits_for_simulators_to_self_complete_before_drain_observation() throws Exception {
    BenchmarkConfig config = BenchmarkConfig.from("local", Map.of(
        "FDB_BENCHMARK_ID", "bench-a",
        "FDB_BENCHMARK_SINKS", "none",
        "FDB_BENCHMARK_CELL_LEVELS", "1000",
        "FDB_BENCHMARK_WARMUP_SEC", "0",
        "FDB_BENCHMARK_SIMULATION_DURATION_SEC", "300",
        "FDB_BENCHMARK_DRAIN_TIMEOUT_SEC", "60"));
    List<String> events = new ArrayList<>();
    RecordingDeploy deploy = new RecordingDeploy(events);
    RecordingSimulators simulators = new RecordingSimulators(events);
    FakeObservationSource observations = new FakeObservationSource(List.of(healthy()), events);

    new BenchmarkOrchestrator(
        config,
        deploy,
        simulators,
        observations,
        new BenchmarkDecisionEngine(config.thresholds())).run();

    assertThat(events).containsSubsequence(
        "start:1000:30000",
        "await:360",
        "observe:bench-a-none-cells1000-chr-eps30",
        "stop:bench-a-none-cells1000-chr-eps30");
    assertThat(events).doesNotContainSubsequence("start:1000:30000", "stop", "observe:bench-a-none-cells1000-chr-eps30");
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
        new StorageSnapshot(true, "healthy", 100, 0, 0)).withSource(healthySource());
  }

  private static SourceMetricsSnapshot healthySource() {
    return new SourceMetricsSnapshot(true, 600, 1200, 588.0, 2_000, 0, 0,
        200, 2000, 200.0, 10_000, 0, 0,
        500, 2000, 500.0, 4_000, 0, 0);
  }

  static final class RecordingDeploy implements DeployCommandClient {
    final List<String> actions = new ArrayList<>();
    final List<String> events;
    boolean failSubmit;

    RecordingDeploy() {
      this(new ArrayList<>());
    }

    RecordingDeploy(List<String> events) {
      this.events = events;
    }

    @Override
    public void prepare(BenchmarkRunPlan plan) {
      actions.add("prepare:" + plan.runId());
      events.add("prepare:" + plan.runId());
    }

    @Override
    public void submit(BenchmarkRunPlan plan) throws Exception {
      actions.add("submit:" + plan.runId());
      events.add("submit:" + plan.runId());
      if (failSubmit) {
        throw new Exception("submit failed");
      }
    }

    @Override
    public void stop(BenchmarkRunPlan plan) {
      actions.add("stop:" + plan.runId());
      events.add("stop:" + plan.runId());
    }
  }

  static final class RecordingSimulators implements SimulatorProcessManager {
    final List<String> actions = new ArrayList<>();
    final List<String> events;

    RecordingSimulators() {
      this(new ArrayList<>());
    }

    RecordingSimulators(List<String> events) {
      this.events = events;
    }

    @Override
    public void start(BenchmarkRunPlan plan) {
      actions.add("start:" + plan.cellLevel() + ":" + plan.targetChrTotalEps());
      events.add("start:" + plan.cellLevel() + ":" + plan.targetChrTotalEps());
    }

    @Override
    public void awaitCompletion(long timeoutSec) {
      actions.add("await:" + timeoutSec);
      events.add("await:" + timeoutSec);
    }

    @Override
    public void stop() {
      actions.add("stop");
      events.add("stop");
    }
  }

  static final class FakeObservationSource implements BenchmarkClients {
    private final List<RunObservation> observations;
    private final List<String> events;
    private final List<StorageSnapshot> storageSnapshots;
    private final int failObserveAfter;
    private int index;
    private int storageIndex;

    FakeObservationSource(List<RunObservation> observations) {
      this(observations, new ArrayList<>());
    }

    FakeObservationSource(List<RunObservation> observations, List<String> events) {
      this(observations, events, List.of(), Integer.MAX_VALUE);
    }

    FakeObservationSource(
        List<RunObservation> observations,
        List<String> events,
        List<StorageSnapshot> storageSnapshots,
        int failObserveAfter) {
      this.observations = observations;
      this.events = events;
      this.storageSnapshots = storageSnapshots;
      this.failObserveAfter = failObserveAfter;
    }

    @Override
    public RunObservation observe(BenchmarkRunPlan plan) {
      events.add("observe:" + plan.runId());
      if (index >= failObserveAfter) {
        throw new IllegalStateException("full observation unavailable");
      }
      return observations.get(index++);
    }

    @Override
    public StorageSnapshot observeStorage(BenchmarkRunPlan plan) {
      events.add("storage:" + plan.runId());
      if (storageIndex < storageSnapshots.size()) {
        return storageSnapshots.get(storageIndex++);
      }
      return observe(plan).storage();
    }

    int observationCount() {
      return index;
    }
  }
}

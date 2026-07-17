package com.fdb.benchmark;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.junit.jupiter.api.Test;

class JavaSimulatorProcessManagerTest {
  @Test
  void uses_host_kafka_bootstrap_for_local_simulator_processes() {
    JavaSimulatorProcessManager manager = new JavaSimulatorProcessManager(Map.of(
        "FDB_KAFKA_BOOTSTRAP", "kafka:9092",
        "FDB_KAFKA_HOST_BOOTSTRAP", "localhost:9092"));

    Map<String, String> env = manager.envFor(new BenchmarkRunPlan(
        "bench-a", BenchmarkSink.NONE, 1000, 1, "bench-a-none-cells1000-eps1", "small"));

    assertThat(env.get("FDB_KAFKA_BOOTSTRAP")).isEqualTo("localhost:9092");
    assertThat(env).containsEntry("FDB_TOPOLOGY_TARGET_CELLS", "1000");
    assertThat(env).containsEntry("FDB_SITES_COUNT", "1000");
    assertThat(env.get("FDB_RATE_EPS")).isEqualTo("1");
    assertThat(env.get("FDB_TOPOLOGY_METRICS_FILE"))
        .isEqualTo("benchmark-runner/output/benchmark-runs/bench-a/runs/bench-a-none-cells1000-eps1/topology-metrics.json");
  }
}

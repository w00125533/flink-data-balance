package com.fdb.benchmark;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Method;
import java.util.Map;
import org.junit.jupiter.api.Test;

class JavaSimulatorProcessManagerTest {
  @Test
  void passes_cell_level_as_site_count_and_target_cells() throws Exception {
    JavaSimulatorProcessManager manager = new JavaSimulatorProcessManager(Map.of());

    Map<String, String> env = envFor(manager, new BenchmarkRunPlan(
        "bench-a", BenchmarkSink.NONE, 1000, 1, "bench-a-none-cells1000-eps1", "small"));

    assertThat(env).containsEntry("FDB_TOPOLOGY_TARGET_CELLS", "1000");
    assertThat(env).containsEntry("FDB_SITES_COUNT", "1000");
    assertThat(env).containsEntry("FDB_BENCHMARK_ANOMALY_INJECTION_RATIO", "0.05");
  }

  @Test
  void preserves_explicit_anomaly_injection_ratio_env() throws Exception {
    JavaSimulatorProcessManager manager = new JavaSimulatorProcessManager(Map.of(
        "FDB_BENCHMARK_ANOMALY_INJECTION_RATIO", "0.12"));

    Map<String, String> env = envFor(manager, new BenchmarkRunPlan(
        "bench-a", BenchmarkSink.NONE, 1000, 1, "bench-a-none-cells1000-eps1", "small"));

    assertThat(env).containsEntry("FDB_BENCHMARK_ANOMALY_INJECTION_RATIO", "0.12");
  }

  @SuppressWarnings("unchecked")
  private static Map<String, String> envFor(JavaSimulatorProcessManager manager, BenchmarkRunPlan plan)
      throws Exception {
    Method method = JavaSimulatorProcessManager.class.getDeclaredMethod("envFor", BenchmarkRunPlan.class);
    method.setAccessible(true);
    return (Map<String, String>) method.invoke(manager, plan);
  }
}

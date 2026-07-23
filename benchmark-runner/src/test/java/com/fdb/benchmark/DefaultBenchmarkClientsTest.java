package com.fdb.benchmark;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.junit.jupiter.api.Test;

class DefaultBenchmarkClientsTest {
  @Test
  void probe_commands_default_to_short_outer_timeout() {
    Map<String, String> env = DefaultBenchmarkClients.probeCommandEnv(Map.of());

    assertThat(env).containsEntry("FDB_BENCHMARK_COMMAND_TIMEOUT_SEC", "20");
  }

  @Test
  void probe_commands_use_hdfs_probe_timeout_when_configured() {
    Map<String, String> env = DefaultBenchmarkClients.probeCommandEnv(Map.of(
        "FDB_HDFS_PROBE_TIMEOUT_SEC", "7"));

    assertThat(env).containsEntry("FDB_BENCHMARK_COMMAND_TIMEOUT_SEC", "7");
  }

  @Test
  void probe_commands_keep_explicit_command_timeout() {
    Map<String, String> env = DefaultBenchmarkClients.probeCommandEnv(Map.of(
        "FDB_HDFS_PROBE_TIMEOUT_SEC", "7",
        "FDB_BENCHMARK_COMMAND_TIMEOUT_SEC", "11"));

    assertThat(env).containsEntry("FDB_BENCHMARK_COMMAND_TIMEOUT_SEC", "11");
  }
}

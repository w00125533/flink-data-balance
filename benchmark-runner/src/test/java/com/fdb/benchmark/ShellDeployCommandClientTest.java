package com.fdb.benchmark;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ShellDeployCommandClientTest {
  @Test
  void uses_configured_bash_for_deploy_commands() {
    ShellDeployCommandClient client = new ShellDeployCommandClient(
        "local",
        new ProcessCommandRunner(),
        Map.of("FDB_BENCHMARK_BASH", "C:/Program Files/Git/usr/bin/bash.exe"));

    assertThat(client.deployCommand("submit")).isEqualTo(List.of(
        "C:/Program Files/Git/usr/bin/bash.exe",
        "scripts/deploy.sh",
        "local",
        "submit"));
  }

  @Test
  void exposes_prepare_deploy_command_for_benchmark_data_reset() throws Exception {
    ShellDeployCommandClient client = new ShellDeployCommandClient(
        "external-yarn",
        new ProcessCommandRunner(),
        Map.of("FDB_BENCHMARK_BASH", "bash"));

    assertThat(client.deployCommand("prepare")).isEqualTo(List.of(
        "bash",
        "scripts/deploy.sh",
        "external-yarn",
        "prepare"));
  }
}

package com.fdb.benchmark;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class StorageProbeTest {
  @Test
  void none_sink_uses_noop_probe() throws Exception {
    StorageProbe probe = StorageProbe.forSink(BenchmarkSink.NONE, command -> new CommandResult(0, "", ""));

    assertThat(probe.snapshot().healthy()).isTrue();
    assertThat(probe.snapshot().summary()).contains("no business sink");
  }

  @Test
  void starrocks_probe_marks_command_failure_unhealthy() throws Exception {
    StorageProbe probe = StorageProbe.forSink(BenchmarkSink.STARROCKS,
        command -> new CommandResult(1, "", "mysql unavailable"));

    StorageSnapshot snapshot = probe.snapshot();

    assertThat(snapshot.healthy()).isFalse();
    assertThat(snapshot.summary()).contains("mysql unavailable");
  }
}

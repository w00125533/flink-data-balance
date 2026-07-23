package com.fdb.benchmark;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
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

  @Test
  void kafka_probe_has_container_fallback_for_local_benchmark() throws Exception {
    CapturingCommandRunner runner = new CapturingCommandRunner(new CommandResult(0, "", ""));

    StorageProbe.forSink(BenchmarkSink.KAFKA, runner).snapshot();

    assertThat(runner.shell()).contains("docker exec ${FDB_SHARED_KAFKA_CONTAINER:-shared-data-infra-kafka-1}");
  }

  @Test
  void starrocks_probe_has_container_fallback_for_local_benchmark() throws Exception {
    CapturingCommandRunner runner = new CapturingCommandRunner(new CommandResult(0, "", ""));

    StorageProbe.forSink(BenchmarkSink.STARROCKS, runner).snapshot();

    assertThat(runner.shell()).contains("docker exec ${FDB_SHARED_STARROCKS_FE_CONTAINER:-shared-data-infra-starrocks-fe-1}");
  }

  @Test
  void starrocks_probe_executes_container_query_without_nested_shell_command_string() throws Exception {
    CapturingCommandRunner runner = new CapturingCommandRunner(new CommandResult(0, "starrocks tcp open", ""));

    StorageProbe.forSink(BenchmarkSink.STARROCKS, runner).snapshot();

    assertThat(runner.shell())
        .doesNotContain("container_mysql=")
        .doesNotContain("bash -lc \"$container_mysql\"")
        .contains("container_output=$(timeout $probe_timeout docker exec")
        .contains("docker exec ${FDB_SHARED_STARROCKS_FE_CONTAINER:-shared-data-infra-starrocks-fe-1}");
  }

  @Test
  void starrocks_probe_falls_back_to_container_when_host_query_returns_zero_rows() throws Exception {
    CapturingCommandRunner runner = new CapturingCommandRunner(new CommandResult(0, "0", ""));

    StorageProbe.forSink(BenchmarkSink.STARROCKS, runner).snapshot();

    assertThat(runner.shell())
        .contains("host_rows")
        .contains("$host_rows != 0")
        .contains("container_output=$(timeout $probe_timeout docker exec");
  }

  @Test
  void starrocks_probe_reports_business_table_rows() throws Exception {
    StorageProbe probe = StorageProbe.forSink(BenchmarkSink.STARROCKS,
        command -> new CommandResult(0, "12345\n", ""));

    StorageSnapshot snapshot = probe.snapshot();

    assertThat(snapshot.healthy()).isTrue();
    assertThat(snapshot.records()).isEqualTo(12_345);
    assertThat(snapshot.summary()).contains("starrocks rows=12345");
  }

  @Test
  void starrocks_probe_queries_result_table_counts() throws Exception {
    CapturingCommandRunner runner = new CapturingCommandRunner(new CommandResult(0, "0", ""));

    StorageProbe.forSink(BenchmarkSink.STARROCKS, runner).snapshot();

    assertThat(runner.shell())
        .contains("FROM cell_kpi")
        .contains("FROM cell_anomaly_events")
        .contains("FROM user_anomaly_events")
        .contains("FROM grid_anomaly_events");
  }

  @Test
  void starrocks_probe_bounds_query_time() throws Exception {
    CapturingCommandRunner runner = new CapturingCommandRunner(new CommandResult(0, "0", ""));

    StorageProbe.forSink(BenchmarkSink.STARROCKS, runner).snapshot();

    assertThat(runner.shell())
        .contains("probe_timeout=${FDB_STARROCKS_PROBE_TIMEOUT_SEC:-10}")
        .contains("timeout $probe_timeout");
  }

  @Test
  void hive_probe_has_hdfs_container_fallback_for_local_benchmark() throws Exception {
    CapturingCommandRunner runner = new CapturingCommandRunner(new CommandResult(0, "0", ""));

    StorageProbe.forSink(BenchmarkSink.HIVE, runner).snapshot();

    assertThat(runner.shell())
        .contains("export MSYS_NO_PATHCONV=1")
        .contains("run_with_timeout docker exec ${FDB_SHARED_HDFS_CONTAINER:-shared-data-infra-namenode-1}");
  }

  @Test
  void hive_probe_reports_in_progress_files_without_failing_the_run() throws Exception {
    StorageProbe probe = StorageProbe.forSink(BenchmarkSink.HIVE,
        command -> new CommandResult(0, "20", ""));

    StorageSnapshot snapshot = probe.snapshot();

    assertThat(snapshot.healthy()).isTrue();
    assertThat(snapshot.inProgressFiles()).isEqualTo(20);
    assertThat(snapshot.summary()).contains("hive in-progress files=20");
  }

  @Test
  void hive_probe_bounds_hdfs_find_time() throws Exception {
    CapturingCommandRunner runner = new CapturingCommandRunner(new CommandResult(0, "0", ""));

    StorageProbe.forSink(BenchmarkSink.HIVE, runner).snapshot();

    assertThat(runner.shell())
        .contains("probe_timeout=${FDB_HDFS_PROBE_TIMEOUT_SEC:-10}")
        .contains("timeout \"$probe_timeout\"");
  }

  @Test
  void iceberg_probe_has_hdfs_container_fallback_for_local_benchmark() throws Exception {
    CapturingCommandRunner runner = new CapturingCommandRunner(new CommandResult(0, "0", ""));

    StorageProbe.forSink(BenchmarkSink.ICEBERG, runner).snapshot();

    assertThat(runner.shell())
        .contains("export MSYS_NO_PATHCONV=1")
        .contains("run_with_timeout docker exec ${FDB_SHARED_HDFS_CONTAINER:-shared-data-infra-namenode-1}");
  }

  @Test
  void iceberg_probe_reports_in_progress_files_without_failing_the_run() throws Exception {
    StorageProbe probe = StorageProbe.forSink(BenchmarkSink.ICEBERG,
        command -> new CommandResult(0, "3", ""));

    StorageSnapshot snapshot = probe.snapshot();

    assertThat(snapshot.healthy()).isTrue();
    assertThat(snapshot.inProgressFiles()).isEqualTo(3);
    assertThat(snapshot.summary()).contains("iceberg in-progress files=3");
  }

  @Test
  void iceberg_probe_bounds_hdfs_find_time() throws Exception {
    CapturingCommandRunner runner = new CapturingCommandRunner(new CommandResult(0, "0", ""));

    StorageProbe.forSink(BenchmarkSink.ICEBERG, runner).snapshot();

    assertThat(runner.shell())
        .contains("probe_timeout=${FDB_HDFS_PROBE_TIMEOUT_SEC:-10}")
        .contains("timeout \"$probe_timeout\"");
  }

  private static final class CapturingCommandRunner implements CommandRunner {
    private final CommandResult result;
    private List<String> command = new ArrayList<>();

    private CapturingCommandRunner(CommandResult result) {
      this.result = result;
    }

    @Override
    public CommandResult run(List<String> command) {
      this.command = new ArrayList<>(command);
      return result;
    }

    private String shell() {
      assertThat(command).hasSizeGreaterThanOrEqualTo(3);
      return command.get(2);
    }
  }
}

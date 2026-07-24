package com.fdb.benchmark;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class BenchmarkRunnerMainTest {
  @TempDir Path tempDir;

  @Test
  void dry_run_writes_artifacts_without_starting_external_processes() throws Exception {
    Path env = tempDir.resolve(".env");
    Files.writeString(env, """
        FDB_BENCHMARK_ID=bench-dry
        FDB_BENCHMARK_SINKS=none
        FDB_BENCHMARK_CELL_LEVELS=1000
        FDB_BENCHMARK_CHR_EPS_PER_CELL=0.1
        FDB_BENCHMARK_PM_EPS_PER_CELL=0.2
        """);

    Path runDir = BenchmarkPaths.outputRoot().resolve("bench-dry");
    try {
      int exit = BenchmarkRunnerMain.run(new String[] {"local", "--env", env.toString(), "--dry-run"});

      assertThat(exit).isZero();
      assertThat(runDir.resolve("index.html")).exists();
      Path report = runDir.resolve("runs/bench-dry-none-cells1000-chr-eps0.1/report.html");
      assertThat(report).exists();
      assertThat(Files.readString(report))
          .contains("<tr><th>目标 CHR EPS</th><td>0.1 条/小区/s</td></tr>")
          .contains("<tr><th>全局 CHR EPS</th><td>100 条/s</td></tr>")
          .contains("<tr><th>目标 PM EPS</th><td>0.2 条/小区/s</td></tr>")
          .contains("<tr><th>全局 PM EPS</th><td>200 条/s</td></tr>")
          .contains("<tr><th>Source 吞吐达成率</th><td>98.00%</td></tr>")
          .contains("<tr><th>CHR 总量</th><td>200</td><td>98.00</td>")
          .contains("算子流程与指标")
          .contains("flow-edge");
    } finally {
      deleteRecursively(runDir);
    }
  }

  @Test
  void dry_run_allows_set_arguments_to_override_env_file_values() throws Exception {
    Path env = tempDir.resolve(".env");
    Files.writeString(env, """
        FDB_BENCHMARK_ID=bench-set
        FDB_BENCHMARK_SINKS=none
        FDB_BENCHMARK_CELL_LEVELS=1000
        FDB_BENCHMARK_CHR_EPS_PER_CELL=0.1
        FDB_BENCHMARK_PM_EPS_PER_CELL=0.2
        """);

    Path runDir = BenchmarkPaths.outputRoot().resolve("bench-set");
    try {
      int exit = BenchmarkRunnerMain.run(new String[] {
          "local",
          "--env", env.toString(),
          "--set", "FDB_BENCHMARK_CELL_LEVELS=2000",
          "--set", "FDB_BENCHMARK_CHR_EPS_PER_CELL=0.2",
          "--dry-run"});

      assertThat(exit).isZero();
      Path report = runDir.resolve("runs/bench-set-none-cells2000-chr-eps0.2/report.html");
      assertThat(report).exists();
      assertThat(Files.readString(report))
          .contains("<tr><th>目标 CHR EPS</th><td>0.2 条/小区/s</td></tr>")
          .contains("<tr><th>全局 CHR EPS</th><td>400 条/s</td></tr>");
      assertThat(runDir.resolve("runs/bench-set-none-cells1000-chr-eps0.1/report.html")).doesNotExist();
    } finally {
      deleteRecursively(runDir);
    }
  }

  private static void deleteRecursively(Path path) throws IOException {
    if (!Files.exists(path)) {
      return;
    }
    try (var stream = Files.walk(path)) {
      for (Path item : stream.sorted(Comparator.reverseOrder()).toList()) {
        Files.deleteIfExists(item);
      }
    }
  }
}

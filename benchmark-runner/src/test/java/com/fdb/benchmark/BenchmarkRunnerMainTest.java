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
        """);

    Path runDir = Path.of("benchmark-runner/output/benchmark-runs/bench-dry");
    try {
      int exit = BenchmarkRunnerMain.run(new String[] {"local", "--env", env.toString(), "--dry-run"});

      assertThat(exit).isZero();
      assertThat(runDir.resolve("index.html")).exists();
      Path report = runDir.resolve("runs/bench-dry-none-cells1000-eps100/report.html");
      assertThat(report).exists();
      assertThat(Files.readString(report))
          .contains("<tr><th>Target CHR EPS</th><td>100</td></tr>")
          .contains("<tr><th>Source Throughput Attainment</th><td>98.00%</td></tr>")
          .contains("<tr><th>CHR total</th><td>200</td><td>98.00</td>")
          .contains("Operator Flow")
          .contains("flow-edge");
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

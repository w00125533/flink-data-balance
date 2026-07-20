package com.fdb.benchmark;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class BenchmarkPathsTest {
  @TempDir Path tempDir;

  @Test
  void output_root_resolves_to_repo_benchmark_runner_from_module_working_directory() throws Exception {
    Path repo = repoFixture();
    Path moduleCwd = repo.resolve("benchmark-runner");
    Files.createDirectories(moduleCwd.resolve("benchmark-runner"));

    Path outputRoot = BenchmarkPaths.outputRoot(moduleCwd);

    assertThat(outputRoot).isEqualTo(repo.resolve("benchmark-runner/output/benchmark-runs").normalize());
    assertThat(outputRoot.toString()).doesNotContain("benchmark-runner" + outputRoot.getFileSystem().getSeparator()
        + "benchmark-runner");
  }

  @Test
  void logs_dir_resolves_to_repo_logs_from_module_working_directory() throws Exception {
    Path repo = repoFixture();

    Path logsDir = BenchmarkPaths.logsDir(repo.resolve("benchmark-runner"));

    assertThat(logsDir).isEqualTo(repo.resolve("logs").normalize());
  }

  private Path repoFixture() throws Exception {
    Path repo = tempDir.resolve("flink-data-balance");
    Files.createDirectories(repo.resolve("benchmark-runner"));
    Files.createDirectories(repo.resolve("scripts"));
    Files.writeString(repo.resolve("pom.xml"), "<project/>");
    return repo;
  }
}

package com.fdb.benchmark;

import java.nio.file.Files;
import java.nio.file.Path;

final class BenchmarkPaths {
  private BenchmarkPaths() {
  }

  static Path outputRoot() {
    return outputRoot(currentWorkingDirectory());
  }

  static Path outputRoot(Path cwd) {
    return repoRoot(cwd).resolve("benchmark-runner/output/benchmark-runs").normalize();
  }

  static Path logsDir() {
    return logsDir(currentWorkingDirectory());
  }

  static Path logsDir(Path cwd) {
    return repoRoot(cwd).resolve("logs").normalize();
  }

  private static Path repoRoot(Path cwd) {
    Path current = cwd.toAbsolutePath().normalize();
    while (current != null) {
      if (Files.isRegularFile(current.resolve("pom.xml"))
          && Files.isDirectory(current.resolve("benchmark-runner"))
          && Files.isDirectory(current.resolve("scripts"))) {
        return current;
      }
      current = current.getParent();
    }
    return cwd.toAbsolutePath().normalize();
  }

  private static Path currentWorkingDirectory() {
    return Path.of("").toAbsolutePath().normalize();
  }
}

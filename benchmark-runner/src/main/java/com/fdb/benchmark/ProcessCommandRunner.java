package com.fdb.benchmark;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public final class ProcessCommandRunner implements CommandRunner {
  private static final long DEFAULT_COMMAND_TIMEOUT_SEC = 900;

  @Override
  public CommandResult run(List<String> command) throws IOException, InterruptedException {
    return run(command, Map.of());
  }

  public CommandResult run(List<String> command, Map<String, String> env) throws IOException, InterruptedException {
    ProcessBuilder builder = new ProcessBuilder(resolveCommand(command, env));
    builder.environment().putAll(env);
    Process process = builder.start();
    CompletableFuture<String> stdout = readAsync(process.getInputStream());
    CompletableFuture<String> stderr = readAsync(process.getErrorStream());
    int exitCode;
    try {
      long timeoutSec = commandTimeoutSec(env);
      if (!process.waitFor(timeoutSec, TimeUnit.SECONDS)) {
        destroyProcessTree(process);
        process.waitFor();
        throw new IOException("command timed out after " + timeoutSec + "s: " + String.join(" ", command));
      }
      exitCode = process.exitValue();
    } catch (InterruptedException e) {
      destroyProcessTree(process);
      throw e;
    }
    return new CommandResult(exitCode, stdout.join(), stderr.join());
  }

  static List<String> resolveCommand(List<String> command, Map<String, String> env) {
    if (command.isEmpty()) {
      return command;
    }
    String configuredBash = env.get("FDB_BENCHMARK_BASH");
    if (!"bash".equals(command.get(0)) || configuredBash == null || configuredBash.isBlank()) {
      return command;
    }
    List<String> resolved = new ArrayList<>(command);
    resolved.set(0, configuredBash);
    return resolved;
  }

  private static long commandTimeoutSec(Map<String, String> env) {
    String raw = env.get("FDB_BENCHMARK_COMMAND_TIMEOUT_SEC");
    if (raw == null || raw.isBlank()) {
      return DEFAULT_COMMAND_TIMEOUT_SEC;
    }
    try {
      long value = Long.parseLong(raw.trim());
      return value > 0 ? value : DEFAULT_COMMAND_TIMEOUT_SEC;
    } catch (NumberFormatException e) {
      return DEFAULT_COMMAND_TIMEOUT_SEC;
    }
  }

  private static void destroyProcessTree(Process process) {
    ProcessHandle handle = process.toHandle();
    handle.descendants().forEach(ProcessHandle::destroyForcibly);
    handle.destroyForcibly();
  }

  private static CompletableFuture<String> readAsync(InputStream input) {
    return CompletableFuture.supplyAsync(() -> {
      try (input) {
        return new String(input.readAllBytes(), StandardCharsets.UTF_8);
      } catch (IOException e) {
        return e.getMessage();
      }
    });
  }
}

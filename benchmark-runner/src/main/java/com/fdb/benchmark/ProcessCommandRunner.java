package com.fdb.benchmark;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public final class ProcessCommandRunner implements CommandRunner {
  @Override
  public CommandResult run(List<String> command) throws IOException, InterruptedException {
    return run(command, Map.of());
  }

  public CommandResult run(List<String> command, Map<String, String> env) throws IOException, InterruptedException {
    ProcessBuilder builder = new ProcessBuilder(command);
    builder.environment().putAll(env);
    Process process = builder.start();
    CompletableFuture<String> stdout = readAsync(process.getInputStream());
    CompletableFuture<String> stderr = readAsync(process.getErrorStream());
    int exitCode;
    try {
      exitCode = process.waitFor();
    } catch (InterruptedException e) {
      process.destroyForcibly();
      throw e;
    }
    return new CommandResult(exitCode, stdout.join(), stderr.join());
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

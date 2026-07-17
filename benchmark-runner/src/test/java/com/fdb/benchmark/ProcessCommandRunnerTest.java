package com.fdb.benchmark;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ProcessCommandRunnerTest {
  @Test
  void resolveCommand_uses_configured_bash_when_command_starts_with_bash() {
    List<String> command = ProcessCommandRunner.resolveCommand(
        List.of("bash", "-lc", "echo ok"),
        Map.of("FDB_BENCHMARK_BASH", "C:/Program Files/Git/usr/bin/bash.exe"));

    assertThat(command)
        .containsExactly("C:/Program Files/Git/usr/bin/bash.exe", "-lc", "echo ok");
  }

  @Test
  void resolveCommand_keeps_non_bash_command_unchanged() {
    List<String> command = ProcessCommandRunner.resolveCommand(
        List.of("java", "-version"),
        Map.of("FDB_BENCHMARK_BASH", "C:/Program Files/Git/usr/bin/bash.exe"));

    assertThat(command).containsExactly("java", "-version");
  }

  @Test
  void run_times_out_long_running_commands() throws Exception {
    ProcessCommandRunner runner = new ProcessCommandRunner();

    assertThatThrownBy(() -> runner.run(
        sleepingJavaCommand(),
        Map.of("FDB_BENCHMARK_COMMAND_TIMEOUT_SEC", "1")))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("command timed out after 1s");
  }

  private static List<String> sleepingJavaCommand() throws Exception {
    String javaBin = Path.of(
        System.getProperty("java.home"),
        "bin",
        isWindows() ? "java.exe" : "java").toString();
    String testClasses = Path.of(
        SleepForCommand.class.getProtectionDomain().getCodeSource().getLocation().toURI()).toString();
    return List.of(javaBin, "-cp", testClasses, SleepForCommand.class.getName());
  }

  private static boolean isWindows() {
    return System.getProperty("os.name", "").toLowerCase().contains("win");
  }
}

final class SleepForCommand {
  private SleepForCommand() {
  }

  public static void main(String[] args) throws InterruptedException {
    Thread.sleep(5_000);
  }
}

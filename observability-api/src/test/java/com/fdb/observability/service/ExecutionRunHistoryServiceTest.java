package com.fdb.observability.service;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ExecutionRunHistoryServiceTest {

  @TempDir
  Path tempDir;

  @Test
  void readsPersistedExecutionRuns() throws Exception {
    Path runDir = tempDir.resolve("20260603-010203");
    Files.createDirectories(runDir);
    Files.writeString(runDir.resolve("meta.json"), """
        {
          "runId": "20260603-010203",
          "status": "success",
          "startedAt": "2026-06-03T01:02:03Z",
          "completedAt": "2026-06-03T01:04:03Z",
          "summaryFile": "logs-summary.log"
        }
        """);
    Files.writeString(runDir.resolve("logs-summary.log"), """
        [summary] Execution | run id | 20260603-010203
        [summary] StarRocks KPI | rows_by_window_kind | MIN_1=10
        [summary] Execution | status | success
        """);

    ExecutionRunHistoryService service = new ExecutionRunHistoryService(tempDir);

    assertThat(service.listRuns()).hasSize(1);
    assertThat(service.listRuns().get(0).runId()).isEqualTo("20260603-010203");
    assertThat(service.runDetail("20260603-010203")).isPresent();
    assertThat(service.runDetail("20260603-010203").orElseThrow().metrics()).hasSize(3);
    assertThat(service.runDetail("../bad")).isEmpty();
  }
}

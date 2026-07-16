package com.fdb.benchmark;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public final class BenchmarkResultWriter {
  private static final ObjectMapper MAPPER = new ObjectMapper()
      .enable(SerializationFeature.INDENT_OUTPUT);

  private final Path outputRoot;

  public BenchmarkResultWriter(Path outputRoot) {
    this.outputRoot = outputRoot;
  }

  public Path write(BenchmarkConfig config, List<BenchmarkRunResult> results) throws IOException {
    Path benchmarkDir = outputRoot.resolve(config.benchmarkId());
    Files.createDirectories(benchmarkDir);
    MAPPER.writeValue(benchmarkDir.resolve("benchmark-config.json").toFile(), config);
    MAPPER.writeValue(benchmarkDir.resolve("benchmark-results.json").toFile(), results);
    Files.writeString(benchmarkDir.resolve("benchmark-summary.csv"), csv(results));
    for (BenchmarkRunResult result : results) {
      Path runDir = benchmarkDir.resolve("runs").resolve(result.plan().runId());
      Files.createDirectories(runDir);
      MAPPER.writeValue(runDir.resolve("run.json").toFile(), result);
      MAPPER.writeValue(runDir.resolve("flink-snapshot.json").toFile(), result.flink());
      MAPPER.writeValue(runDir.resolve("fdb-metrics-snapshot.json").toFile(), result.fdb());
      MAPPER.writeValue(runDir.resolve("storage-snapshot.json").toFile(), result.storage());
    }
    return benchmarkDir;
  }

  private static String csv(List<BenchmarkRunResult> results) {
    StringBuilder out = new StringBuilder();
    out.append("sink,cellLevel,targetChrEps,status,recordsInPerSec,recordsOutPerSec,kpi1mP95Ms,kpi5mP95Ms,")
        .append("sinkP95Ms,checkpointDurationMs,backpressureRatio,bottleneckReason,runId\n");
    for (BenchmarkRunResult result : results) {
      BenchmarkRunPlan plan = result.plan();
      out.append(plan.sink().value()).append(',')
          .append(plan.cellLevel()).append(',')
          .append(plan.targetChrEps()).append(',')
          .append(result.status()).append(',')
          .append(result.flink().recordsInPerSec()).append(',')
          .append(result.flink().recordsOutPerSec()).append(',')
          .append(result.fdb().kpi1mP95Ms()).append(',')
          .append(result.fdb().kpi5mP95Ms()).append(',')
          .append(result.fdb().sinkP95Ms()).append(',')
          .append(result.flink().checkpointDurationMs()).append(',')
          .append(result.flink().backpressureRatio()).append(',')
          .append(csvValue(result.bottleneckReason())).append(',')
          .append(csvValue(plan.runId())).append('\n');
    }
    return out.toString();
  }

  private static String csvValue(String value) {
    if (value == null) {
      return "";
    }
    if (value.contains(",") || value.contains("\"") || value.contains("\n")) {
      return '"' + value.replace("\"", "\"\"") + '"';
    }
    return value;
  }
}

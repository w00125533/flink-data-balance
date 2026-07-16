package com.fdb.benchmark;

import java.util.ArrayList;
import java.util.List;

public final class BenchmarkMatrix {
  private BenchmarkMatrix() {
  }

  public static List<BenchmarkRunPlan> expand(BenchmarkConfig config) {
    List<BenchmarkRunPlan> plans = new ArrayList<>();
    for (BenchmarkSink sink : config.sinks()) {
      for (int cellLevel : config.cellLevels()) {
        long targetChrEps = config.targetChrEps(cellLevel);
        String runId = BenchmarkConfig.sanitize(config.benchmarkId() + "-" + sink.value()
            + "-cells" + cellLevel + "-eps" + targetChrEps);
        plans.add(new BenchmarkRunPlan(config.benchmarkId(), sink, cellLevel, targetChrEps, runId,
            "benchmark-" + sink.value()));
      }
    }
    return List.copyOf(plans);
  }
}

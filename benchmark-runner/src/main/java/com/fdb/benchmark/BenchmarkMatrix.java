package com.fdb.benchmark;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

public final class BenchmarkMatrix {
  private BenchmarkMatrix() {
  }

  public static List<BenchmarkRunPlan> expand(BenchmarkConfig config) {
    List<BenchmarkRunPlan> plans = new ArrayList<>();
    for (BenchmarkSink sink : config.sinks()) {
      for (int cellLevel : config.cellLevels()) {
        double targetChrEpsPerCell = config.chrEpsPerCell();
        long targetChrTotalEps = config.targetChrTotalEps(cellLevel);
        double targetPmEpsPerCell = config.pmEpsPerCell();
        long targetPmTotalEps = config.targetPmTotalEps(cellLevel);
        String runId = BenchmarkConfig.sanitize(config.benchmarkId() + "-" + sink.value()
            + "-cells" + cellLevel + "-chr-eps" + rateToken(targetChrEpsPerCell));
        plans.add(new BenchmarkRunPlan(config.benchmarkId(), sink, cellLevel, targetChrEpsPerCell,
            targetChrTotalEps, targetPmEpsPerCell, targetPmTotalEps, runId, "benchmark-" + sink.value()));
      }
    }
    return List.copyOf(plans);
  }

  static String rateToken(double value) {
    return BigDecimal.valueOf(value).stripTrailingZeros().toPlainString();
  }
}

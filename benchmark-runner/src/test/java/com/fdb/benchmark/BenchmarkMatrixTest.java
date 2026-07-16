package com.fdb.benchmark;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.junit.jupiter.api.Test;

class BenchmarkMatrixTest {
  @Test
  void expands_sink_then_cell_level_order_with_readable_run_ids() {
    BenchmarkConfig config = BenchmarkConfig.from("local", Map.of(
        "FDB_BENCHMARK_ID", "bench-20260716-153000",
        "FDB_BENCHMARK_SINKS", "none starrocks",
        "FDB_BENCHMARK_CELL_LEVELS", "1000 3000",
        "FDB_BENCHMARK_CHR_EPS_PER_CELL", "0.3"));

    assertThat(BenchmarkMatrix.expand(config))
        .extracting(BenchmarkRunPlan::runId)
        .containsExactly(
            "bench-20260716-153000-none-cells1000-eps300",
            "bench-20260716-153000-none-cells3000-eps900",
            "bench-20260716-153000-starrocks-cells1000-eps300",
            "bench-20260716-153000-starrocks-cells3000-eps900");
  }
}

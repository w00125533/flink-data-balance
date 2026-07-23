package com.fdb.benchmark;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;

class WindowMaterializationSnapshotTest {
  @Test
  void counts_closed_minutes_from_window_materialization_metrics() {
    BenchmarkRunPlan plan = new BenchmarkRunPlan("bench-a", BenchmarkSink.ICEBERG, 1_000,
        30.0, 30_000, 1.0, 1_000, "run-a", "cells1000");
    SourceMetricsSnapshot source = new SourceMetricsSnapshot(true,
        30_000, 9_000_000, 30_000.0, 300_000, 0, 0,
        1_000, 300_000, 1_000.0, 300_000, 0, 0,
        500, 50_000, 500.0, 300_000, 0, 0);
    FdbMetricsSnapshot fdb = new FdbMetricsSnapshot(-1, -1, -1, -1, 0, 0,
        List.of(),
        List.of(
            window("window-chr-1m", "chr-1m", "MIN_1@60000", 1_000),
            window("window-chr-1m", "chr-1m", "MIN_1@120000", 1_000),
            window("window-chr-1m", "chr-1m", "MIN_1@180000", 999),
            window("window-pm-1m", "pm-1m", "MIN_1@60000", 1_000),
            window("window-pm-1m", "pm-1m", "MIN_1@120000", 1_000),
            window("window-kpi-1m", "kpi-1m", "MIN_1@60000", 1_000)));

    WindowMaterializationSnapshot snapshot = WindowMaterializationSnapshot.from(plan, source, fdb);

    assertThat(snapshot.expectedClosedMinuteWindows()).isEqualTo(4);
    assertThat(snapshot.chr().closedMinuteWindows()).isEqualTo(2);
    assertThat(snapshot.pm().closedMinuteWindows()).isEqualTo(2);
    assertThat(snapshot.kpi().closedMinuteWindows()).isEqualTo(1);
    assertThat(snapshot.healthy()).isFalse();
    assertThat(snapshot.bottleneckReason()).contains("expected >= 4 closed minutes");
  }

  private static SinkLatencySnapshot window(String sinkName, String dataset, String windowKind, long records) {
    return new SinkLatencySnapshot(
        sinkName,
        "window-materialization",
        dataset,
        windowKind,
        records,
        0,
        0,
        0,
        0,
        0);
  }
}

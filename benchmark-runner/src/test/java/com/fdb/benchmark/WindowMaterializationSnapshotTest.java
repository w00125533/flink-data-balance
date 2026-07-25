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
    assertThat(snapshot.expectedClosedFiveMinuteWindows()).isZero();
    assertThat(snapshot.chr().closedMinuteWindows()).isEqualTo(2);
    assertThat(snapshot.pm().closedMinuteWindows()).isEqualTo(2);
    assertThat(snapshot.kpi().closedMinuteWindows()).isEqualTo(1);
    assertThat(snapshot.healthy()).isFalse();
    assertThat(snapshot.bottleneckReason()).contains("expected >= 4 closed minutes");
  }

  @Test
  void requires_one_kpi_five_minute_window_for_six_minute_source_generation() {
    BenchmarkRunPlan plan = new BenchmarkRunPlan("bench-a", BenchmarkSink.NONE, 1_000,
        30.0, 30_000, 1.0, 1_000, "run-a", "cells1000");
    SourceMetricsSnapshot source = new SourceMetricsSnapshot(true,
        30_000, 10_800_000, 30_000.0, 360_000, 0, 0,
        1_000, 360_000, 1_000.0, 360_000, 0, 0,
        1_000, 1_000, 1_000.0, 100, 0, 0);
    FdbMetricsSnapshot fdb = new FdbMetricsSnapshot(-1, -1, -1, -1, 0, 0,
        List.of(),
        List.of(
            window("window-chr-1m", "chr-1m", "MIN_1@60000", 1_000),
            window("window-chr-1m", "chr-1m", "MIN_1@120000", 1_000),
            window("window-chr-1m", "chr-1m", "MIN_1@180000", 1_000),
            window("window-chr-1m", "chr-1m", "MIN_1@240000", 1_000),
            window("window-chr-1m", "chr-1m", "MIN_1@300000", 1_000),
            window("window-pm-1m", "pm-1m", "MIN_1@60000", 1_000),
            window("window-pm-1m", "pm-1m", "MIN_1@120000", 1_000),
            window("window-pm-1m", "pm-1m", "MIN_1@180000", 1_000),
            window("window-pm-1m", "pm-1m", "MIN_1@240000", 1_000),
            window("window-pm-1m", "pm-1m", "MIN_1@300000", 1_000),
            window("window-kpi-1m", "kpi-1m", "MIN_1@60000", 1_000),
            window("window-kpi-1m", "kpi-1m", "MIN_1@120000", 1_000),
            window("window-kpi-1m", "kpi-1m", "MIN_1@180000", 1_000),
            window("window-kpi-1m", "kpi-1m", "MIN_1@240000", 1_000),
            window("window-kpi-1m", "kpi-1m", "MIN_1@300000", 1_000)));

    WindowMaterializationSnapshot missing5m = WindowMaterializationSnapshot.from(plan, source, fdb);

    assertThat(missing5m.expectedClosedFiveMinuteWindows()).isEqualTo(1);
    assertThat(missing5m.oneMinuteHealthy()).isTrue();
    assertThat(missing5m.fiveMinuteHealthy()).isFalse();
    assertThat(missing5m.bottleneckReason()).contains("5m KPI window materialization");

    WindowMaterializationSnapshot present5m = WindowMaterializationSnapshot.from(plan, source,
        new FdbMetricsSnapshot(-1, -1, -1, -1, 0, 0, List.of(),
            List.of(window("window-kpi-5m", "kpi-5m", "MIN_5@300000", 1_000))));

    assertThat(present5m.fiveMinuteHealthy()).isTrue();
  }

  @Test
  void counts_kpi_five_minute_flink_probe_input_when_output_is_zero() {
    BenchmarkRunPlan plan = new BenchmarkRunPlan("bench-a", BenchmarkSink.NONE, 1_000,
        30.0, 30_000, 1.0, 1_000, "run-a", "cells1000");
    SourceMetricsSnapshot source = new SourceMetricsSnapshot(true,
        30_000, 10_800_000, 30_000.0, 360_000, 0, 0,
        1_000, 360_000, 1_000.0, 360_000, 0, 0,
        1_000, 1_000, 1_000.0, 100, 0, 0);
    FlinkSnapshot flink = new FlinkSnapshot("RUNNING", 0, 30_000, 0, 0, 0, 0, 0, 1, 6, List.of(
        new FlinkOperatorSnapshot("kpi-5m", "kpi-5m-rollup -> kpi-5m-metrics", 6,
            1_000, 0, 1_000, 0, 8_192, 0, 0.2, 0.8, 0.0)));

    WindowMaterializationSnapshot snapshot = WindowMaterializationSnapshot.from(plan, source,
        new FdbMetricsSnapshot(-1, -1, -1, -1, 0, 0, List.of(), List.of()), flink);

    assertThat(snapshot.kpi5m().recordsInTotal()).isEqualTo(1_000);
    assertThat(snapshot.kpi5m().recordsOutTotal()).isZero();
    assertThat(snapshot.kpi5m().closedMinuteWindows()).isEqualTo(1);
    assertThat(snapshot.fiveMinuteHealthy()).isTrue();
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

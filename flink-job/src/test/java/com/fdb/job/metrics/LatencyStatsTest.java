package com.fdb.job.metrics;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class LatencyStatsTest {
  @Test
  void empty_snapshot_reports_latency_as_not_available() {
    LatencyStats.Snapshot snapshot = new LatencyStats().snapshotAndReset();

    assertThat(snapshot.p50Ms()).isEqualTo(-1L);
    assertThat(snapshot.p95Ms()).isEqualTo(-1L);
    assertThat(snapshot.p99Ms()).isEqualTo(-1L);
    assertThat(snapshot.maxMs()).isEqualTo(-1L);
  }

  @Test
  void snapshot_reports_percentiles_when_samples_exist() {
    LatencyStats stats = new LatencyStats();
    stats.recordObservedAt(100L, 90L);
    stats.recordObservedAt(100L, 80L);
    stats.recordObservedAt(100L, 70L);
    stats.recordObservedAt(100L, 60L);

    LatencyStats.Snapshot snapshot = stats.snapshotAndReset();

    assertThat(snapshot.p50Ms()).isEqualTo(20L);
    assertThat(snapshot.p95Ms()).isEqualTo(40L);
    assertThat(snapshot.p99Ms()).isEqualTo(40L);
    assertThat(snapshot.maxMs()).isEqualTo(40L);
  }
}

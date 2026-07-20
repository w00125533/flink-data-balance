package com.fdb.benchmark;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

import java.util.List;
import org.junit.jupiter.api.Test;

class FlinkSnapshotTest {
  @Test
  void measurement_window_keeps_last_totals_but_averages_non_zero_rates() {
    FlinkSnapshot first = new FlinkSnapshot("RUNNING", 0.01, 1_000, 0,
        100.0, 80.0, 1_000, 800, 1, 4, List.of(
            new FlinkOperatorSnapshot("source", "Source: chr-source", 4,
                100.0, 80.0, 1_000, 800, 100, 80, 0.2, 0.8, 0.01, 2)));
    FlinkSnapshot second = new FlinkSnapshot("RUNNING", 0.02, 2_000, 0,
        120.0, 90.0, 2_000, 1_700, 1, 4, List.of(
            new FlinkOperatorSnapshot("source", "Source: chr-source", 4,
                120.0, 90.0, 2_000, 1_700, 120, 90, 0.3, 0.7, 0.02, 5)));
    FlinkSnapshot finalIdle = new FlinkSnapshot("RUNNING", 0.0, 1_500, 0,
        0.0, 0.0, 2_100, 1_750, 1, 4, List.of(
            new FlinkOperatorSnapshot("source", "Source: chr-source", 4,
                0.0, 0.0, 2_100, 1_750, 0, 0, 0.0, 1.0, 0.0, 0)));

    FlinkSnapshot window = FlinkSnapshot.measurementWindow(List.of(first, second, finalIdle));

    assertThat(window.recordsInPerSec()).isCloseTo(110.0, within(0.0001));
    assertThat(window.recordsOutPerSec()).isCloseTo(85.0, within(0.0001));
    assertThat(window.recordsInTotal()).isEqualTo(2_100);
    assertThat(window.recordsOutTotal()).isEqualTo(1_750);
    assertThat(window.checkpointDurationMs()).isEqualTo(2_000);
    assertThat(window.backpressureRatio()).isCloseTo(0.02, within(0.0001));
    assertThat(window.sourceBacklogRecords()).isEqualTo(5);
    assertThat(window.operators()).singleElement().satisfies(operator -> {
      assertThat(operator.recordsInPerSec()).isCloseTo(110.0, within(0.0001));
      assertThat(operator.recordsOutPerSec()).isCloseTo(85.0, within(0.0001));
      assertThat(operator.recordsInTotal()).isEqualTo(2_100);
      assertThat(operator.recordsOutTotal()).isEqualTo(1_750);
    });
  }
}

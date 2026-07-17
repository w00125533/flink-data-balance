package com.fdb.simulator;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class ChrSimulatorTest {
    @Test
    void calculates_due_events_from_global_target_eps() {
        assertThat(ChrSimulator.dueEvents(15_000, 1_000L, 2_000L, 0L)).isEqualTo(15_000);
        assertThat(ChrSimulator.dueEvents(15_000, 1_000L, 2_000L, 14_900L)).isEqualTo(100);
        assertThat(ChrSimulator.dueEvents(15_000, 1_000L, 1_500L, 10_000L)).isZero();
    }

    @Test
    void calculates_backlog_from_expected_vs_submitted_and_undelivered_from_submitted_vs_delivered() {
        assertThat(ChrSimulator.backlogRecords(1_000L, 900L)).isEqualTo(100L);
        assertThat(ChrSimulator.backlogRecords(900L, 1_000L)).isZero();
        assertThat(ChrSimulator.undeliveredRecords(1_000L, 960L)).isEqualTo(40L);
        assertThat(ChrSimulator.undeliveredRecords(960L, 1_000L)).isZero();
    }

    @Test
    void selects_stable_anomaly_cohort_by_ratio() {
        long selected = java.util.stream.IntStream.range(0, 10_000)
            .mapToObj(i -> "id-" + i)
            .filter(id -> ChrSimulator.inAnomalyCohort(id, 0.05d))
            .count();

        assertThat(selected).isBetween(350L, 650L);
        assertThat(ChrSimulator.inAnomalyCohort("stable-id", 0.05d))
            .isEqualTo(ChrSimulator.inAnomalyCohort("stable-id", 0.05d));
        assertThat(ChrSimulator.inAnomalyCohort("stable-id", 0.0d)).isFalse();
        assertThat(ChrSimulator.inAnomalyCohort("stable-id", -0.1d)).isFalse();
        assertThat(ChrSimulator.inAnomalyCohort(null, 0.05d)).isFalse();
        assertThat(ChrSimulator.inAnomalyCohort(" ", 0.05d)).isFalse();
        assertThat(ChrSimulator.inAnomalyCohort("stable-id", 1.0d)).isTrue();
        assertThat(ChrSimulator.inAnomalyCohort("stable-id", 1.5d)).isTrue();
    }
}

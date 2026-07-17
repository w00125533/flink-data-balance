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
}

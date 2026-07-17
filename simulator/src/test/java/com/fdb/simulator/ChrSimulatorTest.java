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
}

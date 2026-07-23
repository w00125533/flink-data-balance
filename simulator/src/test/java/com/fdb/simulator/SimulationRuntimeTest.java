package com.fdb.simulator;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class SimulationRuntimeTest {
    @Test
    void unbounded_duration_keeps_running() {
        assertThat(SimulationRuntime.shouldContinue(1_000L, 10_000L, 0L)).isTrue();
    }

    @Test
    void bounded_duration_stops_at_deadline() {
        assertThat(SimulationRuntime.shouldContinue(1_000L, 60_999L, 60L)).isTrue();
        assertThat(SimulationRuntime.shouldContinue(1_000L, 61_000L, 60L)).isFalse();
    }

    @Test
    void metric_duration_is_capped_at_configured_duration() {
        assertThat(SimulationRuntime.metricDurationMs(1_000L, 62_500L, 60L)).isEqualTo(60_000L);
        assertThat(SimulationRuntime.metricDurationMs(1_000L, 42_000L, 60L)).isEqualTo(41_000L);
        assertThat(SimulationRuntime.metricDurationMs(1_000L, 62_500L, 0L)).isEqualTo(61_500L);
    }
}

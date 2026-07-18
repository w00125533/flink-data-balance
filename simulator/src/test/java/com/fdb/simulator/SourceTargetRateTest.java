package com.fdb.simulator;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class SourceTargetRateTest {
    @Test
    void pm_target_eps_is_window_average_rate() {
        assertThat(PmSimulator.targetEpsForWindow(1000)).isEqualTo(100);
        assertThat(PmSimulator.targetEpsForWindow(1)).isEqualTo(1);
        assertThat(PmSimulator.targetEpsForWindow(0)).isZero();
    }

    @Test
    void pm_target_eps_uses_configured_per_cell_rate() {
        assertThat(PmSimulator.targetEpsForCellRate(1000, 1.0d)).isEqualTo(1000);
        assertThat(PmSimulator.targetEpsForCellRate(1000, 0.1d)).isEqualTo(100);
        assertThat(PmSimulator.targetEpsForCellRate(0, 1.0d)).isZero();
    }

    @Test
    void pm_publish_interval_uses_configured_per_cell_rate() {
        assertThat(PmSimulator.publishIntervalMs(1.0d)).isEqualTo(1000L);
        assertThat(PmSimulator.publishIntervalMs(0.1d)).isEqualTo(10_000L);
        assertThat(PmSimulator.publishIntervalMs(2.0d)).isEqualTo(500L);
    }

    @Test
    void cfg_target_eps_is_recent_batch_average_rate() {
        assertThat(CfgSimulator.targetEpsForBatch(1000, 4_000)).isEqualTo(250);
        assertThat(CfgSimulator.targetEpsForBatch(1, 10_000)).isEqualTo(1);
        assertThat(CfgSimulator.targetEpsForBatch(0, 10_000)).isZero();
    }
}

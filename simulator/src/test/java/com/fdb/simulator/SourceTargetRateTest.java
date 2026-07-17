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
    void cfg_target_eps_is_recent_batch_average_rate() {
        assertThat(CfgSimulator.targetEpsForBatch(1000, 4_000)).isEqualTo(250);
        assertThat(CfgSimulator.targetEpsForBatch(1, 10_000)).isEqualTo(1);
        assertThat(CfgSimulator.targetEpsForBatch(0, 10_000)).isZero();
    }
}

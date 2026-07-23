package com.fdb.simulator;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class CfgSimulatorTest {
    @Test
    void fixed_duration_benchmark_mode_skips_continuous_cfg_updates() {
        assertThat(CfgSimulator.shouldRunContinuousUpdates(300)).isFalse();
    }

    @Test
    void default_mode_keeps_continuous_cfg_updates() {
        assertThat(CfgSimulator.shouldRunContinuousUpdates(0)).isTrue();
    }
}

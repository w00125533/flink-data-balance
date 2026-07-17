package com.fdb.simulator;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class PmSimulatorTest {
    @Test
    void anomalous_values_cross_cell_kpi_thresholds() {
        AnomalyValues values = PmSimulator.anomalousValues();

        assertThat(values.avgRsrp()).isLessThan(-110.0f);
        assertThat(values.avgSinr()).isLessThan(-3.0f);
        assertThat(values.attachSuccessRate()).isLessThan(0.95f);
        assertThat(values.dropRate()).isGreaterThan(0.05f);
    }
}

package com.fdb.simulator;

import static org.assertj.core.api.Assertions.assertThat;

import com.fdb.common.avro.PmStat;
import com.fdb.common.avro.TopoCellType;
import com.fdb.common.avro.TopologyRecord;
import org.junit.jupiter.api.Test;

class PmSimulatorTest {
    @Test
    void anomalous_values_cross_cell_kpi_thresholds() {
        AnomalyValues values = PmSimulator.anomalousValues();

        assertThat(values.avgRsrp()).isLessThan(-110.0f);
        assertThat(values.avgSinr()).isLessThan(-3.0f);
        assertThat(values.dropRate()).isGreaterThan(0.05f);
        assertThat(values.avgLatencyMs()).isGreaterThan(200.0f);
    }

    @Test
    void anomalous_pm_stat_overrides_pm_kpi_inputs() {
        PmStat stat = new PmSimulator("unused").generatePmStat(cell("cell-a"), 1_000L, 11_000L, 1.0d);

        assertThat(stat.getAvgRsrp()).isLessThan(-110.0f);
        assertThat(stat.getAvgSinr()).isLessThan(-3.0f);
        assertThat(stat.getDroppedConnections()).isGreaterThan(10);
        assertThat(stat.getAvgLatencyMs()).isGreaterThan(200.0f);
    }

    @Test
    void caps_pm_window_end_lag_to_configured_out_of_order_bound() {
        assertThat(PmSimulator.boundedWindowEnd(101_000L, 2_000L)).isEqualTo(100_000L);
        assertThat(PmSimulator.boundedWindowEnd(109_000L, 2_000L)).isEqualTo(107_000L);
        assertThat(109_000L - PmSimulator.boundedWindowEnd(109_000L, 2_000L)).isLessThanOrEqualTo(2_000L);
    }

    private static TopologyRecord cell(String cellId) {
        return TopologyRecord.newBuilder()
            .setSiteId("site-a")
            .setCellId(cellId)
            .setSiteLat(39.9d)
            .setSiteLon(116.4d)
            .setCellType(TopoCellType.NR_SA)
            .setCellIndex(1)
            .setPci(10)
            .setTac(20)
            .setEci(30L)
            .setMcc("460")
            .setMnc("00")
            .setFrequencyBand("n78")
            .setArfcn(640_000)
            .setBandwidthMhz(100)
            .setAzimuth(0)
            .setCoverageRadiusM(500)
            .setMaxPowerDbm(46.0f)
            .setVersion(1L)
            .build();
    }
}

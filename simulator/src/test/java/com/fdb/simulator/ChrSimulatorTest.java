package com.fdb.simulator;

import static org.assertj.core.api.Assertions.assertThat;

import com.fdb.common.avro.ChrEvent;
import com.fdb.common.avro.ChrEventType;
import com.fdb.common.avro.TopoCellType;
import com.fdb.common.avro.TopologyRecord;
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

    @Test
    void anomalous_event_overrides_signal_result_and_latency_fields() {
        ChrEvent event = new ChrSimulator("unused").generateEvent(
            cell("cell-a"), "imsi-a", 123_456L, 0.0d, 5_000L, 1.0d);

        assertThat(event.getEventType()).isEqualTo(ChrEventType.RRC_SETUP_FAIL);
        assertThat(event.getResultCode()).isEqualTo(1);
        assertThat(event.getRsrp()).isEqualTo(-125.0f);
        assertThat(event.getSinr()).isEqualTo(-8.0f);
        assertThat(event.getRsrq()).isEqualTo(-18.0f);
        assertThat(event.getCqi()).isEqualTo(1);
        assertThat(event.getMcs()).isEqualTo(1);
        assertThat(event.getDurationMs()).isEqualTo(60_000L);
        assertThat(event.getLatencyMs()).isEqualTo(2_500.0f);
    }

    @Test
    void rejects_non_finite_anomaly_injection_ratio() {
        for (double ratio : java.util.List.of(Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY)) {
            org.assertj.core.api.Assertions.assertThatThrownBy(() -> ChrSimulator.validateAnomalyInjectionRatio(ratio))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("FDB_BENCHMARK_ANOMALY_INJECTION_RATIO");
        }
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

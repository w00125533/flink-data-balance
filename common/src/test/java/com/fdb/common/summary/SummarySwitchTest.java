package com.fdb.common.summary;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class SummarySwitchTest {

    @Test
    void is_disabled_by_default() {
        assertThat(SummarySwitch.enabled(Map.of())).isFalse();
    }

    @Test
    void e2e_summary_enables_code_summary() {
        assertThat(SummarySwitch.enabled(Map.of("FDB_E2E_SUMMARY", "1"))).isTrue();
        assertThat(SummarySwitch.enabled(Map.of("FDB_E2E_SUMMARY", "true"))).isTrue();
        assertThat(SummarySwitch.enabled(Map.of("FDB_E2E_SUMMARY", "yes"))).isTrue();
    }

    @Test
    void module_summary_enables_code_summary() {
        assertThat(SummarySwitch.enabled(Map.of("FDB_SIM_SUMMARY", "on"))).isTrue();
        assertThat(SummarySwitch.enabled(Map.of("FDB_TOPOLOGY_SUMMARY", "TRUE"))).isTrue();
        assertThat(SummarySwitch.enabled(Map.of("FDB_FLINK_SUMMARY", "1"))).isTrue();
    }

    @Test
    void false_like_values_disable_code_summary() {
        assertThat(SummarySwitch.enabled(Map.of("FDB_E2E_SUMMARY", "0"))).isFalse();
        assertThat(SummarySwitch.enabled(Map.of("FDB_SIM_SUMMARY", "false"))).isFalse();
        assertThat(SummarySwitch.enabled(Map.of("FDB_TOPOLOGY_SUMMARY", "off"))).isFalse();
    }

    @Test
    void formats_stable_summary_line() {
        assertThat(SummarySwitch.format("sim-chr", "events", 128))
            .isEqualTo("[summary-code] sim-chr | events | 128");
    }
}

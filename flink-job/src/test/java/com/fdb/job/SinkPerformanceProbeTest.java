package com.fdb.job;

import com.fdb.common.avro.CellKpi;
import com.fdb.common.avro.WindowKind;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SinkPerformanceProbeTest {

    @Test
    void counts_records_and_estimated_bytes() throws Exception {
        SinkPerformanceProbe probe = new SinkPerformanceProbe("hive-cell-kpi-1m", 2);
        CellKpi first = kpi(1000L, "a");
        CellKpi second = kpi(2000L, "bb");

        assertThat(probe.record(first)).isSameAs(first);
        assertThat(probe.records()).isEqualTo(1);
        assertThat(probe.approxBytes()).isEqualTo(83);

        probe.record(second);

        assertThat(probe.records()).isEqualTo(2);
        assertThat(probe.approxBytes()).isEqualTo(169);
        assertThat(probe.summaryLine()).contains(
            "[summary-code]",
            "sink=hive-cell-kpi-1m",
            "records=2",
            "approx_bytes=169",
            "first_record_ts=1000",
            "latest_record_ts=2000",
            "records_per_sec=");
    }

    private static CellKpi kpi(long windowStartTs, String suffix) {
        return new CellKpi(
            windowStartTs,
            windowStartTs + 60000L,
            WindowKind.MIN_1,
            "site-" + suffix,
            "cell-" + suffix,
            "grid-" + suffix,
            1L,
            2L,
            3.0f,
            4.0f,
            5.0f,
            6.0f,
            7.0f,
            8.0f,
            9.0f);
    }
}

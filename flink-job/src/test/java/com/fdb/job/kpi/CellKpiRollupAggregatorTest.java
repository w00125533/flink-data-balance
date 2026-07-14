package com.fdb.job.kpi;

import com.fdb.common.avro.CellKpi;
import com.fdb.common.avro.JoinQuality;
import com.fdb.common.avro.WindowKind;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.offset;

class CellKpiRollupAggregatorTest {

    private static final long MINUTE_MS = 60_000L;
    private static final TimeWindow FIVE_MINUTE_WINDOW = new TimeWindow(0L, 5 * MINUTE_MS);

    @Test
    void rolls_five_joined_one_minute_kpis_to_one_five_minute_kpi() {
        CellKpi rolled = CellKpiRollupAggregator.rollUp("cell-a", FIVE_MINUTE_WINDOW, List.of(
            kpi(0, JoinQuality.JOINED, 100L, 10L, -100.0f, 5.0f, 0.50f, 50.0f, 0.01f, 0.90f, 0.95f),
            kpi(1, JoinQuality.JOINED, 100L, 11L, -99.0f, 6.0f, 0.60f, 60.0f, 0.02f, 0.91f, 0.96f),
            kpi(2, JoinQuality.JOINED, 100L, 12L, -98.0f, 7.0f, 0.70f, 70.0f, 0.03f, 0.92f, 0.97f),
            kpi(3, JoinQuality.JOINED, 100L, 13L, -97.0f, 8.0f, 0.80f, 80.0f, 0.04f, 0.93f, 0.98f),
            kpi(4, JoinQuality.JOINED, 100L, 14L, -96.0f, 9.0f, 0.90f, 90.0f, 0.05f, 0.94f, 0.99f)));

        assertThat(rolled.getWindowStartTs()).isEqualTo(0L);
        assertThat(rolled.getWindowEndTs()).isEqualTo(5 * MINUTE_MS);
        assertThat(rolled.getWindowKind()).isEqualTo(WindowKind.MIN_5);
        assertThat(rolled.getJoinQuality()).isEqualTo(JoinQuality.JOINED);
        assertThat(rolled.getSiteId()).isEqualTo("site-a");
        assertThat(rolled.getCellId()).isEqualTo("cell-a");
        assertThat(rolled.getGridId()).isEqualTo("grid-a");
        assertThat(rolled.getNumChrEvents()).isEqualTo(500L);
        assertThat(rolled.getNumUsers()).isEqualTo(60L);
        assertThat(rolled.getAvgPrbUsageDl()).isCloseTo(0.70f, offset(0.0001f));
        assertThat(rolled.getAvgRsrp()).isCloseTo(-98.0f, offset(0.0001f));
        assertThat(rolled.getAttachSuccessRate()).isCloseTo(0.97f, offset(0.0001f));
    }

    @Test
    void all_chr_only_children_roll_up_to_chr_only() {
        CellKpi rolled = CellKpiRollupAggregator.rollUp("cell-a", FIVE_MINUTE_WINDOW, List.of(
            kpi(0, JoinQuality.CHR_ONLY, 20L, 3L, -100.0f, 5.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.80f),
            kpi(1, JoinQuality.CHR_ONLY, 30L, 4L, -90.0f, 15.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.90f)));

        assertThat(rolled.getJoinQuality()).isEqualTo(JoinQuality.CHR_ONLY);
        assertThat(rolled.getNumChrEvents()).isEqualTo(50L);
        assertThat(rolled.getAvgRsrp()).isCloseTo(-94.0f, offset(0.0001f));
        assertThat(rolled.getAvgPrbUsageDl()).isZero();
    }

    @Test
    void all_pm_only_children_roll_up_to_pm_only() {
        CellKpi rolled = CellKpiRollupAggregator.rollUp("cell-a", FIVE_MINUTE_WINDOW, List.of(
            kpi(0, JoinQuality.PM_ONLY, 0L, 0L, 0.0f, 0.0f, 0.60f, 40.0f, 0.02f, 0.80f, 0.0f),
            kpi(1, JoinQuality.PM_ONLY, 0L, 0L, 0.0f, 0.0f, 0.80f, 60.0f, 0.04f, 0.90f, 0.0f)));

        assertThat(rolled.getJoinQuality()).isEqualTo(JoinQuality.PM_ONLY);
        assertThat(rolled.getNumChrEvents()).isZero();
        assertThat(rolled.getAvgPrbUsageDl()).isCloseTo(0.70f, offset(0.0001f));
        assertThat(rolled.getThroughputDlMbpsAvg()).isCloseTo(50.0f, offset(0.0001f));
    }

    @Test
    void mixed_chr_only_and_pm_only_children_roll_up_to_joined() {
        CellKpi rolled = CellKpiRollupAggregator.rollUp("cell-a", FIVE_MINUTE_WINDOW, List.of(
            kpi(0, JoinQuality.CHR_ONLY, 10L, 2L, -90.0f, 10.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.75f),
            kpi(1, JoinQuality.PM_ONLY, 0L, 0L, 0.0f, 0.0f, 0.80f, 80.0f, 0.03f, 0.90f, 0.0f)));

        assertThat(rolled.getJoinQuality()).isEqualTo(JoinQuality.JOINED);
        assertThat(rolled.getNumChrEvents()).isEqualTo(10L);
        assertThat(rolled.getAvgRsrp()).isCloseTo(-90.0f, offset(0.0001f));
        assertThat(rolled.getAvgPrbUsageDl()).isCloseTo(0.80f, offset(0.0001f));
    }

    @Test
    void mixed_joined_and_one_sided_children_preserve_both_evidence_and_roll_up_fields() {
        CellKpi rolled = CellKpiRollupAggregator.rollUp("cell-a", FIVE_MINUTE_WINDOW, List.of(
            kpi(0, JoinQuality.JOINED, 100L, 10L, -90.0f, 10.0f, 0.60f, 60.0f, 0.02f, 0.90f, 0.80f),
            kpi(1, JoinQuality.CHR_ONLY, 50L, 5L, -100.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.70f),
            kpi(2, JoinQuality.PM_ONLY, 0L, 0L, 0.0f, 0.0f, 0.80f, 80.0f, 0.04f, 0.70f, 0.0f)));

        assertThat(rolled.getJoinQuality()).isEqualTo(JoinQuality.JOINED);
        assertThat(rolled.getNumChrEvents()).isEqualTo(150L);
        assertThat(rolled.getNumUsers()).isEqualTo(15L);
        assertThat(rolled.getAvgRsrp()).isCloseTo(-93.3333f, offset(0.0001f));
        assertThat(rolled.getAvgSinr()).isCloseTo(6.6667f, offset(0.0001f));
        assertThat(rolled.getAttachSuccessRate()).isCloseTo(0.7667f, offset(0.0001f));
        assertThat(rolled.getAvgPrbUsageDl()).isCloseTo(0.70f, offset(0.0001f));
        assertThat(rolled.getThroughputDlMbpsAvg()).isCloseTo(70.0f, offset(0.0001f));
    }

    @Test
    void weights_chr_averages_and_attach_rate_by_their_own_denominators() {
        CellKpi rolled = CellKpiRollupAggregator.rollUp("cell-a", FIVE_MINUTE_WINDOW, List.of(
            kpi(0, JoinQuality.JOINED, 100L, 10L, -100.0f, 0.0f, 0.60f, 60.0f, 0.02f, 0.90f, 0.50f,
                2L, 1L, 10L),
            kpi(1, JoinQuality.JOINED, 20L, 5L, -80.0f, 20.0f, 0.80f, 80.0f, 0.04f, 0.80f, 0.90f,
                8L, 9L, 30L)));

        assertThat(rolled.getAvgRsrp()).isCloseTo(-84.0f, offset(0.0001f));
        assertThat(rolled.getAvgSinr()).isCloseTo(18.0f, offset(0.0001f));
        assertThat(rolled.getAttachSuccessRate()).isCloseTo(0.80f, offset(0.0001f));
        assertThat(rolled.getRsrpSampleCount()).isEqualTo(10L);
        assertThat(rolled.getSinrSampleCount()).isEqualTo(10L);
        assertThat(rolled.getAttachAttempts()).isEqualTo(40L);
    }

    @Test
    void rejects_non_minute_child_rows() {
        CellKpi child = CellKpi.newBuilder(kpi(0, JoinQuality.JOINED, 100L, 10L,
                -100.0f, 5.0f, 0.50f, 50.0f, 0.01f, 0.90f, 0.95f))
            .setWindowKind(WindowKind.MIN_5)
            .build();

        assertThatThrownBy(() -> CellKpiRollupAggregator.rollUp("cell-a", FIVE_MINUTE_WINDOW, List.of(child)))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("windowKind");
    }

    @Test
    void rejects_child_rows_for_a_different_cell() {
        CellKpi child = CellKpi.newBuilder(kpi(0, JoinQuality.JOINED, 100L, 10L,
                -100.0f, 5.0f, 0.50f, 50.0f, 0.01f, 0.90f, 0.95f))
            .setCellId("cell-b")
            .build();

        assertThatThrownBy(() -> CellKpiRollupAggregator.rollUp("cell-a", FIVE_MINUTE_WINDOW, List.of(child)))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("cellId");
    }

    private static CellKpi kpi(
        int minute,
        JoinQuality joinQuality,
        long numChrEvents,
        long numUsers,
        float avgRsrp,
        float avgSinr,
        float avgPrbUsageDl,
        float throughputDlMbpsAvg,
        float dropRate,
        float hoSuccessRate,
        float attachSuccessRate) {
        return kpi(minute, joinQuality, numChrEvents, numUsers, avgRsrp, avgSinr, avgPrbUsageDl,
            throughputDlMbpsAvg, dropRate, hoSuccessRate, attachSuccessRate,
            numChrEvents, numChrEvents, numChrEvents);
    }

    private static CellKpi kpi(
        int minute,
        JoinQuality joinQuality,
        long numChrEvents,
        long numUsers,
        float avgRsrp,
        float avgSinr,
        float avgPrbUsageDl,
        float throughputDlMbpsAvg,
        float dropRate,
        float hoSuccessRate,
        float attachSuccessRate,
        long rsrpSampleCount,
        long sinrSampleCount,
        long attachAttempts) {
        return CellKpi.newBuilder()
            .setWindowStartTs(minute * MINUTE_MS)
            .setWindowEndTs((minute + 1L) * MINUTE_MS)
            .setWindowKind(WindowKind.MIN_1)
            .setJoinQuality(joinQuality)
            .setSiteId("site-a")
            .setCellId("cell-a")
            .setGridId("grid-a")
            .setNumChrEvents(numChrEvents)
            .setNumUsers(numUsers)
            .setRsrpSampleCount(rsrpSampleCount)
            .setSinrSampleCount(sinrSampleCount)
            .setAttachAttempts(attachAttempts)
            .setAvgRsrp(avgRsrp)
            .setAvgSinr(avgSinr)
            .setAvgPrbUsageDl(avgPrbUsageDl)
            .setThroughputDlMbpsAvg(throughputDlMbpsAvg)
            .setDropRate(dropRate)
            .setHoSuccessRate(hoSuccessRate)
            .setAttachSuccessRate(attachSuccessRate)
            .build();
    }
}

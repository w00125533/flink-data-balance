package com.fdb.job.kpi;

import com.fdb.job.model.ChrMinuteFact;
import com.fdb.job.model.MinuteFactEnvelope;
import com.fdb.job.model.PmMinuteFact;
import com.fdb.common.avro.CellKpi;
import com.fdb.common.avro.CellType;
import com.fdb.common.avro.CfgConfig;
import com.fdb.common.avro.JoinQuality;
import com.fdb.common.avro.MimoMode;
import com.fdb.common.avro.NssaiEntry;
import com.fdb.common.avro.WindowKind;
import com.fdb.common.geo.Geohash;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class MinuteKpiJoinFunctionTest {

    private static final long MINUTE = 60_000L;

    @Test
    void emits_joined_kpi_when_chr_and_pm_facts_arrive_for_same_minute() throws Exception {
        try (var harness = harness(Duration.ofMinutes(2))) {
            harness.open();

            harness.processElement(MinuteFactEnvelope.chr(new ChrMinuteFact(
                "cell-a", "site-a", 0L, 2L, 2L, -190.0, 20.0, 2L, 1L, 2L, 2L)), 0L);
            harness.processElement(MinuteFactEnvelope.pm(new PmMinuteFact(
                "cell-a", "site-a", 0L, 2L, 1.20, 220.0, 100L, 1L, 9L, 1L)), 1L);

            List<CellKpi> output = harness.extractOutputValues();
            assertThat(output).hasSize(1);
            CellKpi kpi = output.get(0);
            assertThat(kpi.getCellId()).isEqualTo("cell-a");
            assertThat(kpi.getWindowKind()).isEqualTo(WindowKind.MIN_1);
            assertThat(kpi.getJoinQuality()).isEqualTo(JoinQuality.JOINED);
            assertThat(kpi.getNumChrEvents()).isEqualTo(2L);
            assertThat(kpi.getRsrpSampleCount()).isEqualTo(2L);
            assertThat(kpi.getSinrSampleCount()).isEqualTo(2L);
            assertThat(kpi.getAttachAttempts()).isEqualTo(2L);
            assertThat(kpi.getAvgPrbUsageDl()).isEqualTo(0.60f);
            assertThat(kpi.getDropRate()).isEqualTo(0.01f);

            harness.processWatermark(3 * MINUTE);
            assertThat(harness.extractOutputValues()).hasSize(1);
        }
    }

    @Test
    void propagates_weighted_source_event_time_from_chr_and_pm_facts() throws Exception {
        try (var harness = harness(Duration.ofMinutes(2))) {
            harness.open();

            harness.processElement(MinuteFactEnvelope.chr(new ChrMinuteFact(
                "cell-a", "site-a", 0L, 3L, 3L, -270.0, 30.0, 3L, 3L, 3L, 3L,
                20_000L, 10_000L, 30_000L, 3L)), 0L);
            harness.processElement(MinuteFactEnvelope.pm(new PmMinuteFact(
                "cell-a", "site-a", 0L, 2L, 1.20, 220.0, 100L, 1L, 9L, 1L,
                50_000L, 40_000L, 60_000L, 2L)), 1L);

            CellKpi kpi = harness.extractOutputValues().get(0);

            assertThat(kpi.getSourceEventTsAvg()).isEqualTo(32_000L);
            assertThat(kpi.getSourceEventTsMin()).isEqualTo(10_000L);
            assertThat(kpi.getSourceEventTsMax()).isEqualTo(60_000L);
            assertThat(kpi.getSourceEventCount()).isEqualTo(5L);
        }
    }

    @Test
    void joins_chr_with_pm_fact_from_boundary_source_minute() throws Exception {
        try (var harness = harness(Duration.ofMinutes(2))) {
            harness.open();

            long pmMinuteTs = 0L;

            harness.processElement(MinuteFactEnvelope.chr(new ChrMinuteFact(
                "cell-a", "site-a", 0L, 1L, 1L, -95.0, 10.0, 1L, 1L, 1L, 1L)), 0L);
            harness.processElement(MinuteFactEnvelope.pm(new PmMinuteFact(
                "cell-a", "site-a", pmMinuteTs, 1L, 0.60, 110.0, 0L, 4L, 1L)), MINUTE);

            List<CellKpi> output = harness.extractOutputValues();
            assertThat(output).hasSize(1);
            assertThat(output.get(0).getWindowStartTs()).isEqualTo(0L);
            assertThat(output.get(0).getJoinQuality()).isEqualTo(JoinQuality.JOINED);
        }
    }

    @Test
    void emits_chr_only_after_lateness_timer() throws Exception {
        try (var harness = harness(Duration.ofMinutes(2))) {
            harness.open();

            harness.processElement(MinuteFactEnvelope.chr(new ChrMinuteFact(
                "cell-a", "site-a", 0L, 2L, 1L, -90.0, 12.0, 1L, 1L, 1L, 1L)), 0L);
            assertThat(harness.extractOutputValues()).isEmpty();

            harness.processWatermark(3 * MINUTE);

            List<CellKpi> output = harness.extractOutputValues();
            assertThat(output).hasSize(1);
            assertThat(output.get(0).getJoinQuality()).isEqualTo(JoinQuality.CHR_ONLY);
            assertThat(output.get(0).getNumChrEvents()).isEqualTo(2L);
            assertThat(output.get(0).getRsrpSampleCount()).isEqualTo(1L);
            assertThat(output.get(0).getSinrSampleCount()).isEqualTo(1L);
            assertThat(output.get(0).getAttachAttempts()).isEqualTo(1L);
            assertThat(output.get(0).getAvgRsrp()).isEqualTo(-90.0f);
        }
    }

    @Test
    void emits_pm_only_after_lateness_timer() throws Exception {
        try (var harness = harness(Duration.ofMinutes(2))) {
            harness.open();

            harness.processElement(MinuteFactEnvelope.pm(new PmMinuteFact(
                "cell-a", "site-a", 0L, 2L, 1.20, 220.0, 1L, 9L, 1L)), 0L);
            assertThat(harness.extractOutputValues()).isEmpty();

            harness.processWatermark(3 * MINUTE);

            List<CellKpi> output = harness.extractOutputValues();
            assertThat(output).hasSize(1);
            assertThat(output.get(0).getJoinQuality()).isEqualTo(JoinQuality.PM_ONLY);
            assertThat(output.get(0).getNumChrEvents()).isZero();
            assertThat(output.get(0).getAvgPrbUsageDl()).isEqualTo(0.60f);
        }
    }

    @Test
    void cfg_updates_by_cell_and_tombstone_removes_grid_context() throws Exception {
        try (var harness = harness(Duration.ofMinutes(2))) {
            harness.open();

            CfgConfig cfg = cfg("cell-a", "site-a", 3L, false);
            harness.processElement(MinuteFactEnvelope.cfg(cfg), 0L);
            harness.processElement(MinuteFactEnvelope.pm(new PmMinuteFact(
                "cell-a", "site-a", 0L, 1L, 0.5, 50.0, 0L, 1L, 0L)), 1L);
            harness.processWatermark(3 * MINUTE);

            List<CellKpi> beforeTombstone = harness.extractOutputValues();
            assertThat(beforeTombstone).hasSize(1);
            assertThat(beforeTombstone.get(0).getGridId())
                .isEqualTo(Geohash.encode(cfg.getCenterLat(), cfg.getCenterLon(), 6));

            harness.processElement(MinuteFactEnvelope.cfg(cfg("cell-a", "site-a", 4L, true)), 4 * MINUTE);
            harness.processElement(MinuteFactEnvelope.pm(new PmMinuteFact(
                "cell-a", "site-a", 4 * MINUTE, 1L, 0.7, 70.0, 0L, 1L, 0L)), 4 * MINUTE);
            harness.processWatermark(7 * MINUTE);

            List<CellKpi> afterTombstone = harness.extractOutputValues();
            assertThat(afterTombstone).hasSize(2);
            assertThat(afterTombstone.get(1).getGridId()).isEmpty();
        }
    }

    @Test
    void older_cfg_tombstone_does_not_clear_newer_active_config() throws Exception {
        try (var harness = harness(Duration.ofMinutes(2))) {
            harness.open();

            CfgConfig active = cfg("cell-a", "site-a", 5L, false);
            harness.processElement(MinuteFactEnvelope.cfg(active), 0L);
            harness.processElement(MinuteFactEnvelope.cfg(cfg("cell-a", "site-a", 4L, true)), 1L);
            harness.processElement(MinuteFactEnvelope.pm(new PmMinuteFact(
                "cell-a", "site-a", 0L, 1L, 0.5, 50.0, 0L, 1L, 0L)), 2L);
            harness.processWatermark(3 * MINUTE);

            List<CellKpi> output = harness.extractOutputValues();
            assertThat(output).hasSize(1);
            assertThat(output.get(0).getGridId())
                .isEqualTo(Geohash.encode(active.getCenterLat(), active.getCenterLon(), 6));
        }
    }

    @Test
    void older_cfg_update_after_tombstone_does_not_resurrect_config() throws Exception {
        try (var harness = harness(Duration.ofMinutes(2))) {
            harness.open();

            harness.processElement(MinuteFactEnvelope.cfg(cfg("cell-a", "site-a", 6L, true)), 0L);
            harness.processElement(MinuteFactEnvelope.cfg(cfg("cell-a", "site-a", 5L, false)), 1L);
            harness.processElement(MinuteFactEnvelope.pm(new PmMinuteFact(
                "cell-a", "site-a", 0L, 1L, 0.5, 50.0, 0L, 1L, 0L)), 2L);
            harness.processWatermark(3 * MINUTE);

            List<CellKpi> output = harness.extractOutputValues();
            assertThat(output).hasSize(1);
            assertThat(output.get(0).getGridId()).isEmpty();
        }
    }

    @Test
    void newer_cfg_update_after_tombstone_restores_config() throws Exception {
        try (var harness = harness(Duration.ofMinutes(2))) {
            harness.open();

            CfgConfig restored = cfg("cell-a", "site-a", 7L, false);
            harness.processElement(MinuteFactEnvelope.cfg(cfg("cell-a", "site-a", 6L, true)), 0L);
            harness.processElement(MinuteFactEnvelope.cfg(restored), 1L);
            harness.processElement(MinuteFactEnvelope.pm(new PmMinuteFact(
                "cell-a", "site-a", 0L, 1L, 0.5, 50.0, 0L, 1L, 0L)), 2L);
            harness.processWatermark(3 * MINUTE);

            List<CellKpi> output = harness.extractOutputValues();
            assertThat(output).hasSize(1);
            assertThat(output.get(0).getGridId())
                .isEqualTo(Geohash.encode(restored.getCenterLat(), restored.getCenterLon(), 6));
        }
    }

    private static KeyedOneInputStreamOperatorTestHarness<String, MinuteFactEnvelope, CellKpi> harness(
        Duration wait) throws Exception {
        KeyedOneInputStreamOperatorTestHarness<String, MinuteFactEnvelope, CellKpi> harness =
            new KeyedOneInputStreamOperatorTestHarness<>(
            new KeyedProcessOperator<>(new MinuteKpiJoinFunction(wait)),
            MinuteFactEnvelope::cellId,
            Types.STRING);
        harness.setup(new KryoSerializer<>(CellKpi.class, new ExecutionConfig()));
        return harness;
    }

    private static CfgConfig cfg(String cellId, String siteId, long version, boolean tombstone) {
        return CfgConfig.newBuilder()
            .setSiteId(siteId)
            .setCellId(cellId)
            .setEffectiveTs(0L)
            .setVersion(version)
            .setCellType(CellType.NR_SA)
            .setBandwidthMhz(100)
            .setFrequencyBand("n78")
            .setArfcn(632448)
            .setMaxPowerDbm(49.0f)
            .setAzimuth(90)
            .setCenterLat(39.9)
            .setCenterLon(116.4)
            .setCoverageRadiusM(500)
            .setPci(100)
            .setTac(40001)
            .setEci(1000L)
            .setMcc("460")
            .setMnc("00")
            .setNumerology(1)
            .setMimoMode(MimoMode.MIMO_4x4)
            .setAntennaPorts(4)
            .setNssai(List.of(NssaiEntry.newBuilder().setSst(1).setSd("000001").build()))
            .setNeighborCells(List.of("cell-b"))
            .setTombstone(tombstone)
            .build();
    }

    private static com.fdb.common.avro.PmStat pm(long start, long end) {
        return com.fdb.common.avro.PmStat.newBuilder()
            .setSiteId("site-a")
            .setCellId("cell-a")
            .setWindowStartTs(start)
            .setWindowEndTs(end)
            .setEventTs(start + ((end - start) / 2L))
            .setPrbUsageDl(0.6f)
            .setPrbUsageUl(0.25f)
            .setActiveUsers(42)
            .setAvgRsrp(-92.0f)
            .setAvgRsrq(-8.0f)
            .setAvgSinr(12.0f)
            .setAvgCqi(10.0f)
            .setAvgMcs(18.0f)
            .setAvgBler(0.02f)
            .setThroughputDlMbps(110.0f)
            .setThroughputUlMbps(30.0f)
            .setDroppedConnections(0)
            .setHandoverSuccess(4)
            .setHandoverFailure(1)
            .setPrachAttempt(9)
            .setPrachFailure(1)
            .setRrcEstabAttempt(20)
            .setRrcEstabSuccess(19)
            .setAvgLatencyMs(18.0f)
            .setPacketLossRate(0.001f)
            .build();
    }
}

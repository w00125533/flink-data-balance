package com.fdb.job;

import com.fdb.job.anomaly.CellKpiCepAnomalyDetector;
import com.fdb.job.anomaly.CoverageHoleDetector;
import com.fdb.job.anomaly.UserEventCepAnomalyDetector;
import com.fdb.job.config.RuleConfig;
import com.fdb.job.model.EnrichedChr;
import com.fdb.common.avro.*;
import com.fdb.common.geo.Geohash;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class FlinkJobE2ETest {

    private static final String SITE_ID = "SITE-001";
    private static final String CELL_ID = "CELL-001";
    private static final long BASE_TS = 1_000_000L;

    // ─────────────────────────────────────────────────
    // Anomaly Detection Pipeline
    // ─────────────────────────────────────────────────

    @Test
    void entity_anomaly_pipelines_emit_cell_user_and_grid_types() throws Exception {
        List<AnomalyEvent> cell = runCellKpiCep();
        List<AnomalyEvent> user = runUserCep();
        List<AnomalyEvent> grid = runGridCoverage();

        assertThat(cell).extracting(AnomalyEvent::getAnomalyType)
            .contains(AnomalyType.CELL_RADIO_BAD);
        assertThat(cell.get(0).getSourceEventTsAvg()).isGreaterThan(0L);
        assertThat(cell.get(0).getSourceEventCount()).isGreaterThan(0L);
        assertThat(user).extracting(AnomalyEvent::getAnomalyType)
            .contains(AnomalyType.USER_FAILURE);
        assertThat(user.get(0).getSourceEventTsAvg()).isEqualTo(BASE_TS + 120_000L);
        assertThat(user.get(0).getSourceEventCount()).isEqualTo(1L);
        assertThat(grid).extracting(AnomalyEvent::getAnomalyType)
            .containsExactly(AnomalyType.COVERAGE_HOLE);
        assertThat(grid.get(0).getSourceEventTsAvg()).isEqualTo(BASE_TS);
        assertThat(grid.get(0).getSourceEventCount()).isEqualTo(1L);
    }

    // ─────────────────────────────────────────────────
    // ─────────────────────────────────────────────────

    @Test
    void coverage_hole_pipeline_groups_by_grid() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        List<AnomalyEvent> anomalies = runGridCoverage(env);
        assertThat(anomalies).extracting(AnomalyEvent::getAnomalyType)
            .containsExactly(AnomalyType.COVERAGE_HOLE);
    }

    private static List<AnomalyEvent> runCellKpiCep() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        List<CellKpi> input = List.of(
            cellKpi(0, -111f, 1f, 0.99f, 0.99f, 0.0f),
            cellKpi(60_000, -112f, 1f, 0.99f, 0.99f, 0.0f),
            cellKpi(120_000, -113f, 1f, 0.99f, 0.99f, 0.0f));
        List<AnomalyEvent> anomalies = new ArrayList<>();
        try (CloseableIterator<AnomalyEvent> it = CellKpiCepAnomalyDetector
            .detect(env.fromCollection(input, new GenericTypeInfo<>(CellKpi.class)), RuleConfig.defaults())
            .executeAndCollect()) {
            while (it.hasNext()) anomalies.add(it.next());
        }
        return anomalies;
    }

    private static List<AnomalyEvent> runUserCep() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        List<EnrichedChr> input = List.of(
            enrichedChr(chrBaseAt(BASE_TS).setEventType(ChrEventType.ATTACH).setResultCode(1).build()),
            enrichedChr(chrBaseAt(BASE_TS + 60_000).setEventType(ChrEventType.ATTACH).setResultCode(2).build()),
            enrichedChr(chrBaseAt(BASE_TS + 120_000).setEventType(ChrEventType.ATTACH).setResultCode(3).build()));
        List<AnomalyEvent> anomalies = new ArrayList<>();
        try (CloseableIterator<AnomalyEvent> it = UserEventCepAnomalyDetector
            .detect(env.fromCollection(input, new GenericTypeInfo<>(EnrichedChr.class)), RuleConfig.defaults())
            .executeAndCollect()) {
            while (it.hasNext()) anomalies.add(it.next());
        }
        return anomalies;
    }

    private static List<AnomalyEvent> runGridCoverage() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        return runGridCoverage(env);
    }

    private static List<AnomalyEvent> runGridCoverage(StreamExecutionEnvironment env) throws Exception {
        List<EnrichedChr> events = new ArrayList<>();
        for (int i = 0; i < 50; i++) events.add(enrichedChrWith(-115f, -5f, 0, null));
        List<AnomalyEvent> anomalies = new ArrayList<>();
        try (CloseableIterator<AnomalyEvent> it = env
            .fromCollection(events, new GenericTypeInfo<>(EnrichedChr.class))
            .keyBy(ec -> Geohash.encode(ec.chrEvent().getLatitude(), ec.chrEvent().getLongitude(), 6))
            .process(new CoverageHoleDetector(RuleConfig.defaults()), new GenericTypeInfo<>(AnomalyEvent.class))
            .executeAndCollect()) {
            while (it.hasNext()) anomalies.add(it.next());
        }
        return anomalies;
    }

    // ─────────────────────────────────────────────────
    // Test Data Builders
    // ─────────────────────────────────────────────────

    private static List<ChrEvent> buildChrEvents() {
        List<ChrEvent> events = new ArrayList<>();

        // 53 low-signal events -> COVERAGE_HOLE
        for (int i = 0; i < 53; i++) {
            events.add(chrBase()
                .setEventType(ChrEventType.DATA_SESSION)
                .setRsrp(-115f).setSinr(-5f)
                .setChrId("chr-low-" + i)
                .build());
        }

        // 10 attach failures -> ATTACH_FAILURE_BURST
        for (int i = 0; i < 10; i++) {
            events.add(chrBase()
                .setEventType(ChrEventType.ATTACH).setResultCode(1)
                .setChrId("chr-attach-" + i)
                .build());
        }

        // 21 handovers (14 success + 7 failure) -> HANDOVER_FAIL_PATTERN
        for (int i = 0; i < 14; i++) {
            events.add(chrBase()
                .setEventType(ChrEventType.HANDOVER).setResultCode(0)
                .setChrId("chr-ho-ok-" + i)
                .build());
        }
        for (int i = 0; i < 7; i++) {
            events.add(chrBase()
                .setEventType(ChrEventType.HANDOVER).setResultCode(1)
                .setChrId("chr-ho-fail-" + i)
                .build());
        }

        // Explicit low-signal event
        events.add(chrBase()
            .setEventType(ChrEventType.DATA_SESSION)
            .setRsrp(-120f).setSinr(-8f)
            .setChrId("chr-explicit-low")
            .build());

        // Config mismatch event (TAC/PCI/ECI different from CFG)
        events.add(chrBase()
            .setEventType(ChrEventType.DATA_SESSION)
            .setTac(40999).setPci(200).setEci(9999L).setArfcn(1500)
            .setChrId("chr-mismatch")
            .build());

        // 10 normal events (no anomaly)
        for (int i = 0; i < 10; i++) {
            events.add(chrBase()
                .setEventType(ChrEventType.DATA_SESSION)
                .setRsrp(-90f).setSinr(10f).setResultCode(0)
                .setChrId("chr-normal-" + i)
                .build());
        }

        return events;
    }

    private static List<CfgConfig> buildCfgConfigs() {
        List<CfgConfig> configs = new ArrayList<>();
        configs.add(cfgConfig(40001, 100, 1000L, 1300));
        return configs;
    }

    private static List<PmStat> buildPmStats() {
        List<PmStat> stats = new ArrayList<>();
        stats.add(pmStat(0.5f));
        return stats;
    }

    private static ChrEvent.Builder chrBase() {
        return chrBaseAt(BASE_TS);
    }

    private static ChrEvent.Builder chrBaseAt(long eventTs) {
        return ChrEvent.newBuilder()
            .setChrId(java.util.UUID.randomUUID().toString())
            .setEventTs(eventTs)
            .setImsi("460001234567890")
            .setSiteId(SITE_ID)
            .setCellId(CELL_ID)
            .setRatType(RatType.LTE)
            .setPci(100)
            .setTac(40001)
            .setEci(1000L)
            .setMcc("460")
            .setMnc("00")
            .setResultCode(0)
            .setLatitude(39.9)
            .setLongitude(116.4);
    }

    private static EnrichedChr enrichedChr(ChrEvent chr) {
        return new EnrichedChr(chr, cfgConfig(40001, 100, 1000L, 1300), pmStat(0.5f));
    }

    private static CellKpi cellKpi(long startTs, float rsrp, float sinr, float attach, float ho, float drop) {
        return CellKpi.newBuilder()
            .setWindowStartTs(startTs)
            .setWindowEndTs(startTs + 60_000)
            .setSourceEventTsAvg(startTs + 30_000)
            .setSourceEventTsMin(startTs + 1_000)
            .setSourceEventTsMax(startTs + 59_000)
            .setSourceEventCount(100)
            .setWindowKind(WindowKind.MIN_1)
            .setJoinQuality(JoinQuality.JOINED)
            .setSiteId(SITE_ID)
            .setCellId(CELL_ID)
            .setGridId("wx4g0e")
            .setNumChrEvents(100)
            .setNumUsers(10)
            .setRsrpSampleCount(100)
            .setSinrSampleCount(100)
            .setAttachAttempts(100)
            .setAvgRsrp(rsrp)
            .setAvgSinr(sinr)
            .setAvgPrbUsageDl(0.5f)
            .setThroughputDlMbpsAvg(100f)
            .setDropRate(drop)
            .setHoSuccessRate(ho)
            .setAttachSuccessRate(attach)
            .build();
    }

    private static CfgConfig cfgConfig(int tac, int pci, long eci, int arfcn) {
        return CfgConfig.newBuilder()
            .setSiteId(SITE_ID).setCellId(CELL_ID).setEffectiveTs(BASE_TS).setVersion(1)
            .setCellType(CellType.LTE).setBandwidthMhz(20).setFrequencyBand("BAND_3")
            .setArfcn(arfcn).setMaxPowerDbm(43f).setAzimuth(0).setCenterLat(39.9)
            .setCenterLon(116.4).setCoverageRadiusM(500).setPci(pci).setTac(tac)
            .setEci(eci).setMcc("460").setMnc("00").setAntennaPorts(2)
            .setNssai(new java.util.ArrayList<>()).setNeighborCells(new java.util.ArrayList<>()).setTombstone(false)
            .build();
    }

    private static PmStat pmStat(float prbUsageDl) {
        return PmStat.newBuilder()
            .setSiteId(SITE_ID).setCellId(CELL_ID)
            .setWindowStartTs(BASE_TS).setWindowEndTs(BASE_TS + 10_000)
            .setPrbUsageDl(prbUsageDl).setPrbUsageUl(0.3f).setActiveUsers(5)
            .setAvgRsrp(-95f).setAvgRsrq(-10f).setAvgSinr(8f).setAvgCqi(10f)
            .setAvgMcs(18f).setAvgBler(0.01f).setThroughputDlMbps(50f)
            .setThroughputUlMbps(10f).setDroppedConnections(0)
            .setHandoverSuccess(5).setHandoverFailure(0)
            .setPrachAttempt(2).setPrachFailure(0)
            .setRrcEstabAttempt(5).setRrcEstabSuccess(5)
            .setAvgLatencyMs(15f).setPacketLossRate(0.001f)
            .build();
    }

    private static EnrichedChr enrichedChrWith(Float rsrp, Float sinr, int resultCode, PmStat pm) {
        ChrEvent.Builder b = chrBase()
            .setEventType(ChrEventType.DATA_SESSION)
            .setResultCode(resultCode);
        if (rsrp != null) b.setRsrp(rsrp);
        if (sinr != null) b.setSinr(sinr);
        CfgConfig cfg = cfgConfig(40001, 100, 1000L, 1300);
        return new EnrichedChr(b.build(), cfg, pm);
    }
}

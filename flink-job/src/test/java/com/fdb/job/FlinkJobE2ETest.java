package com.fdb.job;

import com.fdb.common.avro.*;
import org.apache.flink.api.common.typeinfo.TypeInformation;
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
    void anomaly_pipeline_detects_all_rules() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().enableForceKryo();

        List<ChrEvent> chrEvents = buildChrEvents();
        List<CmConfig> cmConfigs = buildCmConfigs();
        List<MrStat> mrStats = buildMrStats();

        List<EnrichedChr> enrichedList = new ArrayList<>();
        CmConfig cm = cmConfigs.get(0);
        MrStat mr = mrStats.isEmpty() ? null : mrStats.get(0);

        for (ChrEvent chr : chrEvents) {
            enrichedList.add(new EnrichedChr(chr, cm, mr));
        }

        TypeInformation<EnrichedChr> typeInfo = new GenericTypeInfo<>(EnrichedChr.class);
        List<AnomalyEvent> anomalies = new ArrayList<>();

        try (CloseableIterator<AnomalyEvent> it = env
                .fromCollection(enrichedList, typeInfo)
                .keyBy(ec -> ec.chrEvent().getCellId().toString())
                .process(new AnomalyDetector(), new GenericTypeInfo<>(AnomalyEvent.class))
                .executeAndCollect()) {
            while (it.hasNext()) {
                anomalies.add(it.next());
            }
        }

        // ── Verify ──
        assertThat(anomalies).isNotEmpty();

        long lowSignal = anomalies.stream()
            .filter(a -> a.getAnomalyType() == AnomalyType.LOW_SIGNAL).count();
        long attachBurst = anomalies.stream()
            .filter(a -> a.getAnomalyType() == AnomalyType.ATTACH_FAILURE_BURST).count();
        long hoPattern = anomalies.stream()
            .filter(a -> a.getAnomalyType() == AnomalyType.HANDOVER_FAIL_PATTERN).count();
        long configMismatch = anomalies.stream()
            .filter(a -> a.getAnomalyType() == AnomalyType.CONFIG_MISMATCH).count();
        long coverageHole = anomalies.stream()
            .filter(a -> a.getAnomalyType() == AnomalyType.COVERAGE_HOLE).count();

        assertThat(lowSignal).as("LOW_SIGNAL").isGreaterThanOrEqualTo(54);
        assertThat(attachBurst).as("ATTACH_FAILURE_BURST").isPositive();
        assertThat(hoPattern).as("HANDOVER_FAIL_PATTERN").isPositive();
        assertThat(configMismatch).as("CONFIG_MISMATCH").isEqualTo(1);
        assertThat(coverageHole).as("COVERAGE_HOLE").isPositive();
    }

    // ─────────────────────────────────────────────────
    // KPI Aggregation Unit Test
    // ─────────────────────────────────────────────────

    @Test
    void kpi_aggregation_computes_correct_metrics() {
        KpiAggregator agg = new KpiAggregator(WindowKind.MIN_1);
        KpiAccumulator acc = agg.createAccumulator();

        EnrichedChr enriched = enrichedChrWith(-100f, 5f, 0, mrStat(0.6f));
        acc = agg.add(enriched, acc);

        EnrichedChr enriched2 = enrichedChrWith(-80f, 15f, 1, null);
        acc = agg.add(enriched2, acc);

        acc.windowStartTs = BASE_TS;
        acc.windowEndTs = BASE_TS + 60_000;
        acc.siteId = SITE_ID;
        acc.cellId = CELL_ID;

        CellKpi result = agg.getResult(acc);

        assertThat(result.getWindowKind()).isEqualTo(WindowKind.MIN_1);
        assertThat(result.getWindowStartTs()).isEqualTo(BASE_TS);
        assertThat(result.getWindowEndTs()).isEqualTo(BASE_TS + 60_000);
        assertThat(result.getSiteId()).isEqualTo(SITE_ID);
        assertThat(result.getCellId()).isEqualTo(CELL_ID);
        assertThat(result.getNumChrEvents()).isEqualTo(2);
        assertThat(result.getAvgRsrp()).isEqualTo(-90f);
        assertThat(result.getAvgSinr()).isEqualTo(10f);
        assertThat(result.getDropRate()).isEqualTo(0.5f);
        assertThat(result.getAvgPrbUsageDl()).isEqualTo(0.6f);
        assertThat(result.getAttachSuccessRate()).isEqualTo(0.5f);
    }

    // ─────────────────────────────────────────────────
    // Test Data Builders
    // ─────────────────────────────────────────────────

    private static List<ChrEvent> buildChrEvents() {
        List<ChrEvent> events = new ArrayList<>();

        // 53 low-signal events → COVERAGE_HOLE
        for (int i = 0; i < 53; i++) {
            events.add(chrBase()
                .setEventType(ChrEventType.DATA_SESSION)
                .setRsrp(-115f).setSinr(-5f)
                .setChrId("chr-low-" + i)
                .build());
        }

        // 10 attach failures → ATTACH_FAILURE_BURST
        for (int i = 0; i < 10; i++) {
            events.add(chrBase()
                .setEventType(ChrEventType.ATTACH).setResultCode(1)
                .setChrId("chr-attach-" + i)
                .build());
        }

        // 21 handovers (14 success + 7 failure) → HANDOVER_FAIL_PATTERN
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

        // Config mismatch event (TAC/PCI/ECI different from CM)
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

    private static List<CmConfig> buildCmConfigs() {
        List<CmConfig> configs = new ArrayList<>();
        configs.add(cmConfig(40001, 100, 1000L, 1300));
        return configs;
    }

    private static List<MrStat> buildMrStats() {
        List<MrStat> stats = new ArrayList<>();
        stats.add(mrStat(0.5f));
        return stats;
    }

    private static ChrEvent.Builder chrBase() {
        return ChrEvent.newBuilder()
            .setChrId(java.util.UUID.randomUUID().toString())
            .setEventTs(BASE_TS)
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

    private static CmConfig cmConfig(int tac, int pci, long eci, int arfcn) {
        return CmConfig.newBuilder()
            .setSiteId(SITE_ID).setCellId(CELL_ID).setEffectiveTs(BASE_TS).setVersion(1)
            .setCellType(CellType.LTE).setBandwidthMhz(20).setFrequencyBand("BAND_3")
            .setArfcn(arfcn).setMaxPowerDbm(43f).setAzimuth(0).setCenterLat(39.9)
            .setCenterLon(116.4).setCoverageRadiusM(500).setPci(pci).setTac(tac)
            .setEci(eci).setMcc("460").setMnc("00").setAntennaPorts(2)
            .setNssai(new java.util.ArrayList<>()).setNeighborCells(new java.util.ArrayList<>()).setTombstone(false)
            .build();
    }

    private static MrStat mrStat(float prbUsageDl) {
        return MrStat.newBuilder()
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

    private static EnrichedChr enrichedChrWith(Float rsrp, Float sinr, int resultCode, MrStat mr) {
        ChrEvent.Builder b = chrBase()
            .setEventType(ChrEventType.DATA_SESSION)
            .setResultCode(resultCode);
        if (rsrp != null) b.setRsrp(rsrp);
        if (sinr != null) b.setSinr(sinr);
        CmConfig cm = cmConfig(40001, 100, 1000L, 1300);
        return new EnrichedChr(b.build(), cm, mr);
    }
}

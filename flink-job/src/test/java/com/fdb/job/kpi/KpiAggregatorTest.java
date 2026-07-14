package com.fdb.job.kpi;

import com.fdb.common.avro.CellKpi;
import com.fdb.common.avro.CellType;
import com.fdb.common.avro.CfgConfig;
import com.fdb.common.avro.ChrEvent;
import com.fdb.common.avro.ChrEventType;
import com.fdb.common.avro.PmStat;
import com.fdb.common.avro.RatType;
import com.fdb.common.avro.WindowKind;
import com.fdb.job.model.EnrichedChr;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class KpiAggregatorTest {

    private static final String SITE_ID = "SITE-001";
    private static final String CELL_ID = "CELL-001";
    private static final long BASE_TS = 1_000_000L;

    @Test
    void computes_correct_metrics() {
        KpiAggregator agg = new KpiAggregator(WindowKind.MIN_1);
        KpiAccumulator acc = agg.createAccumulator();

        EnrichedChr enriched = enrichedChrWith(-100f, 5f, 0, pmStat(0.6f));
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
        assertThat(result.getRsrpSampleCount()).isEqualTo(2);
        assertThat(result.getSinrSampleCount()).isEqualTo(2);
        assertThat(result.getAttachAttempts()).isZero();
        assertThat(result.getAvgRsrp()).isEqualTo(-90f);
        assertThat(result.getAvgSinr()).isEqualTo(10f);
        assertThat(result.getDropRate()).isZero();
        assertThat(result.getAvgPrbUsageDl()).isEqualTo(0.6f);
        assertThat(result.getAttachSuccessRate()).isZero();
        assertThat(result.getNumUsers()).isEqualTo(1);
        assertThat(result.getThroughputDlMbpsAvg()).isEqualTo(50f);
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
}

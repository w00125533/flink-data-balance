package com.fdb.job.balance;

import com.fdb.job.model.InputEnvelope;
import com.fdb.common.avro.CellType;
import com.fdb.common.avro.CfgConfig;
import com.fdb.common.avro.ChrEvent;
import com.fdb.common.avro.ChrEventType;
import com.fdb.common.avro.MimoMode;
import com.fdb.common.avro.PmStat;
import com.fdb.common.avro.RatType;
import com.fdb.common.hash.Hashes;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;

import static org.assertj.core.api.Assertions.assertThat;

class RoutingAssignerTest {

    @Test
    void route_key_uses_cell_id_for_chr_pm_and_cfg_envelopes() {
        assertThat(RoutingAssigner.routeKey(new InputEnvelope.ChrEnv(chr("SITE-CHR", "CELL-CHR"))))
            .isEqualTo("CELL-CHR");
        assertThat(RoutingAssigner.routeKey(new InputEnvelope.PmEnv(pm("SITE-PM", "CELL-PM"))))
            .isEqualTo("CELL-PM");
        assertThat(RoutingAssigner.routeKey(new InputEnvelope.CfgEnv(cfg("SITE-CFG", "CELL-CFG"))))
            .isEqualTo("CELL-CFG");
    }

    @Test
    void vbucket_hash_uses_cell_route_key_not_site_id() {
        InputEnvelope envelope = new InputEnvelope.ChrEnv(chr("SITE-ROUTE", "CELL-ROUTE"));
        int shift = 17;

        assertThat(RoutingAssigner.vbucketId(envelope, shift))
            .isEqualTo(Hashes.toVBucketWithShift("CELL-ROUTE", 1024, shift));
        assertThat(Hashes.toVBucketWithShift("CELL-ROUTE", 1024, shift))
            .isNotEqualTo(Hashes.toVBucketWithShift("SITE-ROUTE", 1024, shift));
    }

    private static ChrEvent chr(String siteId, String cellId) {
        return ChrEvent.newBuilder()
            .setChrId("chr-" + cellId)
            .setEventTs(1_000L)
            .setImsi("460001234567890")
            .setSiteId(siteId)
            .setCellId(cellId)
            .setEventType(ChrEventType.DATA_SESSION)
            .setRatType(RatType.LTE)
            .setPci(100)
            .setTac(40001)
            .setEci(1000L)
            .setMcc("460")
            .setMnc("00")
            .setResultCode(0)
            .setLatitude(39.9)
            .setLongitude(116.4)
            .build();
    }

    private static PmStat pm(String siteId, String cellId) {
        return PmStat.newBuilder()
            .setSiteId(siteId)
            .setCellId(cellId)
            .setWindowStartTs(1_000L)
            .setWindowEndTs(11_000L)
            .setPrbUsageDl(0.65f)
            .setPrbUsageUl(0.25f)
            .setActiveUsers(42)
            .setAvgRsrp(-92.0f)
            .setAvgRsrq(-8.0f)
            .setAvgSinr(12.0f)
            .setAvgCqi(10.0f)
            .setAvgMcs(18.0f)
            .setAvgBler(0.02f)
            .setThroughputDlMbps(120.0f)
            .setThroughputUlMbps(30.0f)
            .setDroppedConnections(1)
            .setHandoverSuccess(18)
            .setHandoverFailure(2)
            .setPrachAttempt(9)
            .setPrachFailure(1)
            .setRrcEstabAttempt(20)
            .setRrcEstabSuccess(19)
            .setAvgLatencyMs(18.0f)
            .setPacketLossRate(0.001f)
            .build();
    }

    private static CfgConfig cfg(String siteId, String cellId) {
        return CfgConfig.newBuilder()
            .setSiteId(siteId)
            .setCellId(cellId)
            .setEffectiveTs(1_000L)
            .setVersion(1L)
            .setCellType(CellType.LTE)
            .setBandwidthMhz(20)
            .setFrequencyBand("BAND_3")
            .setArfcn(1300)
            .setMaxPowerDbm(43f)
            .setAzimuth(0)
            .setCenterLat(39.9)
            .setCenterLon(116.4)
            .setCoverageRadiusM(500)
            .setPci(100)
            .setTac(40001)
            .setEci(1000L)
            .setMcc("460")
            .setMnc("00")
            .setAntennaPorts(2)
            .setNssai(new ArrayList<>())
            .setNeighborCells(new ArrayList<>())
            .setMimoMode(MimoMode.MIMO_2x2)
            .setTombstone(false)
            .build();
    }
}

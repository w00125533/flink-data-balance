package com.fdb.job.kpi;

import com.fdb.common.avro.ChrEvent;
import com.fdb.common.avro.ChrEventType;
import com.fdb.common.avro.RatType;
import com.fdb.job.model.ChrMinuteFact;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ChrMinuteFactAggregateFunctionTest {

    @Test
    void incrementally_aggregates_chr_events_without_replaying_the_window() {
        ChrMinuteFactAggregateFunction function = new ChrMinuteFactAggregateFunction();
        ChrMinuteFactAccumulator acc = function.createAccumulator();

        acc = function.add(chr("1", "imsi-1", ChrEventType.ATTACH, 0, -90.0f, 10.0f), acc);
        acc = function.add(chr("2", "imsi-1", ChrEventType.ATTACH, 7, null, 14.0f), acc);
        acc = function.add(chr("3", "imsi-2", ChrEventType.DATA_SESSION, 0, -100.0f, null), acc);

        assertThat(function.getResult(acc).toMinuteFact("cell-a", 120_000L)).isEqualTo(new ChrMinuteFact(
            "cell-a", "site-a", 120_000L, 3L, 2L, -190.0, 24.0, 2L, 1L, 2L, 2L,
            120_001L, 120_001L, 120_001L, 3L));
    }

    @Test
    void merges_partial_accumulators_for_parallel_window_preaggregation() {
        ChrMinuteFactAggregateFunction function = new ChrMinuteFactAggregateFunction();

        ChrMinuteFactAccumulator left = function.add(
            chr("1", "imsi-1", ChrEventType.ATTACH, 0, -90.0f, 10.0f),
            function.createAccumulator());
        ChrMinuteFactAccumulator right = function.add(
            chr("2", "imsi-2", ChrEventType.DATA_SESSION, 0, -100.0f, null),
            function.createAccumulator());

        assertThat(function.getResult(function.merge(left, right)).toMinuteFact("cell-a", 120_000L))
            .isEqualTo(new ChrMinuteFact(
                "cell-a", "site-a", 120_000L, 2L, 2L, -190.0, 10.0, 1L, 1L, 2L, 1L,
                120_001L, 120_001L, 120_001L, 2L));
    }

    @Test
    void carries_source_event_time_statistics_in_chr_minute_fact() {
        ChrMinuteFactAggregateFunction function = new ChrMinuteFactAggregateFunction();
        ChrMinuteFactAccumulator acc = function.createAccumulator();

        acc = function.add(chrAt("1", "imsi-1", 120_010L), acc);
        acc = function.add(chrAt("2", "imsi-2", 120_040L), acc);
        acc = function.add(chrAt("3", "imsi-3", 120_070L), acc);

        ChrMinuteFact fact = function.getResult(acc).toMinuteFact("cell-a", 120_000L);

        assertThat(fact.sourceEventTsAvg()).isEqualTo(120_040L);
        assertThat(fact.sourceEventTsMin()).isEqualTo(120_010L);
        assertThat(fact.sourceEventTsMax()).isEqualTo(120_070L);
        assertThat(fact.sourceEventCount()).isEqualTo(3L);
    }

    private static ChrEvent chr(
        String chrId,
        String imsi,
        ChrEventType type,
        int resultCode,
        Float rsrp,
        Float sinr) {
        return chr(chrId, imsi, type, resultCode, rsrp, sinr, 120_001L);
    }

    private static ChrEvent chrAt(String chrId, String imsi, long eventTs) {
        return chr(chrId, imsi, ChrEventType.DATA_SESSION, 0, -90.0f, 10.0f, eventTs);
    }

    private static ChrEvent chr(
        String chrId,
        String imsi,
        ChrEventType type,
        int resultCode,
        Float rsrp,
        Float sinr,
        long eventTs) {
        return ChrEvent.newBuilder()
            .setChrId(chrId)
            .setEventTs(eventTs)
            .setImsi(imsi)
            .setSiteId("site-a")
            .setCellId("cell-a")
            .setEventType(type)
            .setRatType(RatType.NR_SA)
            .setPci(123)
            .setTac(40001)
            .setEci(1234567890L)
            .setMcc("460")
            .setMnc("00")
            .setResultCode(resultCode)
            .setLatitude(39.9042)
            .setLongitude(116.4074)
            .setRsrp(rsrp)
            .setSinr(sinr)
            .build();
    }
}

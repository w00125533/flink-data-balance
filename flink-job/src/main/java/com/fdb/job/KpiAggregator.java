package com.fdb.job;

import com.fdb.common.avro.CellKpi;
import com.fdb.common.avro.WindowKind;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class KpiAggregator implements AggregateFunction<EnrichedChr, KpiAccumulator, CellKpi> {

    private final WindowKind windowKind;

    public KpiAggregator(WindowKind windowKind) {
        this.windowKind = windowKind;
    }

    @Override
    public KpiAccumulator createAccumulator() {
        return new KpiAccumulator();
    }

    @Override
    public KpiAccumulator add(EnrichedChr enriched, KpiAccumulator acc) {
        var chr = enriched.chrEvent();
        if (acc.siteId == null) acc.siteId = chr.getSiteId().toString();
        if (acc.cellId == null) acc.cellId = chr.getCellId().toString();
        acc.count++;
        acc.rsrpSum += chr.getRsrp() != null ? chr.getRsrp() : 0;
        acc.sinrSum += chr.getSinr() != null ? chr.getSinr() : 0;
        acc.rsrpCount += chr.getRsrp() != null ? 1 : 0;
        acc.sinrCount += chr.getSinr() != null ? 1 : 0;
        if (chr.getResultCode() != 0) acc.failureCount++;

        if (enriched.latestMr() != null) {
            acc.prbUsageDlSum += enriched.latestMr().getPrbUsageDl();
            acc.prbUsageCount++;
        }

        return acc;
    }

    @Override
    public CellKpi getResult(KpiAccumulator acc) {
        float avgRsrp = acc.rsrpCount > 0 ? acc.rsrpSum / acc.rsrpCount : 0;
        float avgSinr = acc.sinrCount > 0 ? acc.sinrSum / acc.sinrCount : 0;
        float avgPrb = acc.prbUsageCount > 0 ? acc.prbUsageDlSum / acc.prbUsageCount : 0;
        float dropRate = acc.count > 0 ? (float) acc.failureCount / acc.count : 0;
        float attachSuccessRate = acc.count > 0 ? (float) (acc.count - acc.failureCount) / acc.count : 0;

        return CellKpi.newBuilder()
            .setWindowStartTs(acc.windowStartTs)
            .setWindowEndTs(acc.windowEndTs)
            .setWindowKind(windowKind)
            .setSiteId(acc.siteId != null ? acc.siteId : "")
            .setCellId(acc.cellId != null ? acc.cellId : "")
            .setGridId("")
            .setNumChrEvents((long) acc.count)
            .setNumUsers(0L)
            .setAvgRsrp(avgRsrp)
            .setAvgSinr(avgSinr)
            .setAvgPrbUsageDl(avgPrb)
            .setThroughputDlMbpsAvg(0f)
            .setDropRate(dropRate)
            .setHoSuccessRate(0f)
            .setAttachSuccessRate(attachSuccessRate)
            .build();
    }

    @Override
    public KpiAccumulator merge(KpiAccumulator a, KpiAccumulator b) {
        a.count += b.count;
        a.rsrpSum += b.rsrpSum;
        a.rsrpCount += b.rsrpCount;
        a.sinrSum += b.sinrSum;
        a.sinrCount += b.sinrCount;
        a.failureCount += b.failureCount;
        a.prbUsageDlSum += b.prbUsageDlSum;
        a.prbUsageCount += b.prbUsageCount;
        if (a.siteId == null) a.siteId = b.siteId;
        if (a.cellId == null) a.cellId = b.cellId;
        if (a.windowStartTs == 0 || b.windowStartTs < a.windowStartTs) a.windowStartTs = b.windowStartTs;
        if (b.windowEndTs > a.windowEndTs) a.windowEndTs = b.windowEndTs;
        return a;
    }
}

class KpiAccumulator {
    long count = 0;
    float rsrpSum = 0;
    int rsrpCount = 0;
    float sinrSum = 0;
    int sinrCount = 0;
    int failureCount = 0;
    float prbUsageDlSum = 0;
    int prbUsageCount = 0;
    String siteId = null;
    String cellId = null;
    long windowStartTs = 0;
    long windowEndTs = 0;
}

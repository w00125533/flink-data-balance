package com.fdb.job;

import com.fdb.common.avro.CellKpi;
import com.fdb.common.avro.WindowKind;
import com.fdb.common.geo.Geohash;
import org.apache.flink.api.common.functions.AggregateFunction;

import java.util.HashSet;
import java.util.Set;

public class KpiAggregator implements AggregateFunction<EnrichedChr, KpiAccumulator, CellKpi> {

    private final WindowKind windowKind;

    public KpiAggregator(WindowKind windowKind) {
        this.windowKind = windowKind;
    }

    @Override
    public KpiAccumulator createAccumulator() { return new KpiAccumulator(); }

    @Override
    public KpiAccumulator add(EnrichedChr enriched, KpiAccumulator acc) {
        var chr = enriched.chrEvent();
        if (acc.siteId == null) acc.siteId = chr.getSiteId().toString();
        if (acc.cellId == null) acc.cellId = chr.getCellId().toString();
        acc.count++;
        acc.users.add(chr.getImsi().toString());
        if (chr.getRsrp() != null) { acc.rsrpSum += chr.getRsrp(); acc.rsrpCount++; }
        if (chr.getSinr() != null) { acc.sinrSum += chr.getSinr(); acc.sinrCount++; }
        if ("ATTACH".equals(chr.getEventType().toString())) {
            acc.attachAttempts++;
            if (chr.getResultCode() == 0) acc.attachSuccess++;
        }

        if (enriched.latestMr() != null && acc.mrWindows.add(enriched.latestMr().getWindowEndTs())) {
            var mr = enriched.latestMr();
            acc.prbUsageDlSum += mr.getPrbUsageDl();
            acc.throughputDlSum += mr.getThroughputDlMbps();
            acc.droppedConnections += mr.getDroppedConnections();
            acc.handoverSuccess += mr.getHandoverSuccess();
            acc.handoverFailure += mr.getHandoverFailure();
            acc.mrCount++;
        }
        if (enriched.cmConfig() != null && acc.gridId == null) {
            acc.gridId = Geohash.encode(enriched.cmConfig().getCenterLat(),
                enriched.cmConfig().getCenterLon(), 6);
        }
        return acc;
    }

    @Override
    public CellKpi getResult(KpiAccumulator acc) {
        int hoAttempts = acc.handoverSuccess + acc.handoverFailure;
        return CellKpi.newBuilder()
            .setWindowStartTs(acc.windowStartTs).setWindowEndTs(acc.windowEndTs)
            .setWindowKind(windowKind).setSiteId(valueOrEmpty(acc.siteId))
            .setCellId(valueOrEmpty(acc.cellId)).setGridId(valueOrEmpty(acc.gridId))
            .setNumChrEvents(acc.count).setNumUsers((long) acc.users.size())
            .setAvgRsrp(avg(acc.rsrpSum, acc.rsrpCount)).setAvgSinr(avg(acc.sinrSum, acc.sinrCount))
            .setAvgPrbUsageDl(avg(acc.prbUsageDlSum, acc.mrCount))
            .setThroughputDlMbpsAvg(avg(acc.throughputDlSum, acc.mrCount))
            .setDropRate(avg(acc.droppedConnections, acc.mrCount))
            .setHoSuccessRate(avg(acc.handoverSuccess, hoAttempts))
            .setAttachSuccessRate(avg(acc.attachSuccess, acc.attachAttempts))
            .build();
    }

    @Override
    public KpiAccumulator merge(KpiAccumulator a, KpiAccumulator b) {
        a.count += b.count; a.rsrpSum += b.rsrpSum; a.rsrpCount += b.rsrpCount;
        a.sinrSum += b.sinrSum; a.sinrCount += b.sinrCount;
        a.attachAttempts += b.attachAttempts; a.attachSuccess += b.attachSuccess;
        a.prbUsageDlSum += b.prbUsageDlSum; a.throughputDlSum += b.throughputDlSum;
        a.droppedConnections += b.droppedConnections; a.handoverSuccess += b.handoverSuccess;
        a.handoverFailure += b.handoverFailure; a.mrCount += b.mrCount;
        a.users.addAll(b.users); a.mrWindows.addAll(b.mrWindows);
        if (a.siteId == null) a.siteId = b.siteId;
        if (a.cellId == null) a.cellId = b.cellId;
        if (a.gridId == null) a.gridId = b.gridId;
        if (a.windowStartTs == 0 || b.windowStartTs < a.windowStartTs) a.windowStartTs = b.windowStartTs;
        if (b.windowEndTs > a.windowEndTs) a.windowEndTs = b.windowEndTs;
        return a;
    }

    private static float avg(float sum, int count) { return count > 0 ? sum / count : 0; }
    private static String valueOrEmpty(String value) { return value == null ? "" : value; }
}

class KpiAccumulator {
    long count;
    float rsrpSum;
    int rsrpCount;
    float sinrSum;
    int sinrCount;
    int attachAttempts;
    int attachSuccess;
    float prbUsageDlSum;
    float throughputDlSum;
    int droppedConnections;
    int handoverSuccess;
    int handoverFailure;
    int mrCount;
    Set<String> users = new HashSet<>();
    Set<Long> mrWindows = new HashSet<>();
    String siteId;
    String cellId;
    String gridId;
    long windowStartTs;
    long windowEndTs;
}

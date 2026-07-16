package com.fdb.job.kpi;

import com.fdb.job.model.EnrichedChr;
import com.fdb.common.avro.CellKpi;
import com.fdb.common.avro.JoinQuality;
import com.fdb.common.avro.WindowKind;
import com.fdb.common.geo.Geohash;
import org.apache.flink.api.common.functions.AggregateFunction;

import java.util.HashSet;
import java.util.Set;

class KpiAggregator implements AggregateFunction<EnrichedChr, KpiAccumulator, CellKpi> {

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

        if (enriched.latestPm() != null && acc.pmWindows.add(enriched.latestPm().getWindowEndTs())) {
            var pm = enriched.latestPm();
            acc.prbUsageDlSum += pm.getPrbUsageDl();
            acc.throughputDlSum += pm.getThroughputDlMbps();
            acc.activeUsersSum += Math.max(0, pm.getActiveUsers());
            acc.droppedConnections += pm.getDroppedConnections();
            acc.handoverSuccess += pm.getHandoverSuccess();
            acc.handoverFailure += pm.getHandoverFailure();
            acc.pmCount++;
        }
        if (enriched.cfgConfig() != null && acc.gridId == null) {
            acc.gridId = Geohash.encode(enriched.cfgConfig().getCenterLat(),
                enriched.cfgConfig().getCenterLon(), 6);
        }
        return acc;
    }

    @Override
    public CellKpi getResult(KpiAccumulator acc) {
        int hoAttempts = acc.handoverSuccess + acc.handoverFailure;
        return CellKpi.newBuilder()
            .setWindowStartTs(acc.windowStartTs).setWindowEndTs(acc.windowEndTs)
            .setWindowKind(windowKind).setJoinQuality(JoinQuality.JOINED).setSiteId(valueOrEmpty(acc.siteId))
            .setCellId(valueOrEmpty(acc.cellId)).setGridId(valueOrEmpty(acc.gridId))
            .setNumChrEvents(acc.count).setNumUsers((long) acc.users.size())
            .setRsrpSampleCount(acc.rsrpCount).setSinrSampleCount(acc.sinrCount)
            .setAttachAttempts(acc.attachAttempts)
            .setAvgRsrp(avg(acc.rsrpSum, acc.rsrpCount)).setAvgSinr(avg(acc.sinrSum, acc.sinrCount))
            .setAvgPrbUsageDl(avg(acc.prbUsageDlSum, acc.pmCount))
            .setThroughputDlMbpsAvg(avg(acc.throughputDlSum, acc.pmCount))
            .setDropRate(avg(acc.droppedConnections, acc.activeUsersSum))
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
        a.activeUsersSum += b.activeUsersSum; a.droppedConnections += b.droppedConnections; a.handoverSuccess += b.handoverSuccess;
        a.handoverFailure += b.handoverFailure; a.pmCount += b.pmCount;
        a.users.addAll(b.users); a.pmWindows.addAll(b.pmWindows);
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
    int activeUsersSum;
    int droppedConnections;
    int handoverSuccess;
    int handoverFailure;
    int pmCount;
    Set<String> users = new HashSet<>();
    Set<Long> pmWindows = new HashSet<>();
    String siteId;
    String cellId;
    String gridId;
    long windowStartTs;
    long windowEndTs;
}

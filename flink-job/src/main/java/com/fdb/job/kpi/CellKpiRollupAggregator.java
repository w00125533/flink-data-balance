package com.fdb.job.kpi;

import com.fdb.common.avro.CellKpi;
import com.fdb.common.avro.JoinQuality;
import com.fdb.common.avro.WindowKind;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class CellKpiRollupAggregator
    extends ProcessWindowFunction<CellKpi, CellKpi, String, TimeWindow> {

    @Override
    public void process(String cellId, Context ctx, Iterable<CellKpi> elements, Collector<CellKpi> out) {
        CellKpi rolled = rollUp(cellId, ctx.window(), elements);
        if (rolled != null) {
            out.collect(rolled);
        }
    }

    static CellKpi rollUp(String key, TimeWindow window, Iterable<CellKpi> children) {
        long numChrEvents = 0L;
        long numUsers = 0L;
        double rsrpWeightedSum = 0.0;
        double sinrWeightedSum = 0.0;
        double attachWeightedSum = 0.0;
        long rsrpSampleCount = 0L;
        long sinrSampleCount = 0L;
        long attachAttempts = 0L;
        double prbUsageDlSum = 0.0;
        double throughputDlSum = 0.0;
        double dropRateSum = 0.0;
        double hoSuccessRateSum = 0.0;
        long pmMinutes = 0L;
        boolean sawChild = false;
        boolean allJoined = true;
        boolean allChrOnly = true;
        boolean allPmOnly = true;
        boolean hasChrEvidence = false;
        boolean hasPmEvidence = false;
        String siteId = "";
        String cellId = "";
        String gridId = "";

        for (CellKpi child : children) {
            if (child == null) {
                continue;
            }
            validateChild(key, window, child);
            sawChild = true;
            siteId = firstNonBlank(siteId, child.getSiteId());
            cellId = firstNonBlank(cellId, child.getCellId());
            gridId = firstNonBlank(gridId, child.getGridId());

            JoinQuality joinQuality = child.getJoinQuality();
            allJoined &= joinQuality == JoinQuality.JOINED;
            allChrOnly &= joinQuality == JoinQuality.CHR_ONLY;
            allPmOnly &= joinQuality == JoinQuality.PM_ONLY;

            long childChrEvents = child.getNumChrEvents();
            numChrEvents += childChrEvents;
            numUsers += child.getNumUsers();
            if (child.getRsrpSampleCount() > 0L) {
                rsrpSampleCount += child.getRsrpSampleCount();
                rsrpWeightedSum += child.getAvgRsrp() * child.getRsrpSampleCount();
            }
            if (child.getSinrSampleCount() > 0L) {
                sinrSampleCount += child.getSinrSampleCount();
                sinrWeightedSum += child.getAvgSinr() * child.getSinrSampleCount();
            }
            if (child.getAttachAttempts() > 0L) {
                attachAttempts += child.getAttachAttempts();
                attachWeightedSum += child.getAttachSuccessRate() * child.getAttachAttempts();
            }

            boolean childHasChr = joinQuality == JoinQuality.JOINED
                || joinQuality == JoinQuality.CHR_ONLY
                || childChrEvents > 0L;
            boolean childHasPm = joinQuality == JoinQuality.JOINED
                || joinQuality == JoinQuality.PM_ONLY
                || hasPmMetric(child);
            hasChrEvidence |= childHasChr;
            hasPmEvidence |= childHasPm;
            if (childHasPm) {
                pmMinutes++;
                prbUsageDlSum += child.getAvgPrbUsageDl();
                throughputDlSum += child.getThroughputDlMbpsAvg();
                dropRateSum += child.getDropRate();
                hoSuccessRateSum += child.getHoSuccessRate();
            }
        }

        if (!sawChild) {
            return null;
        }

        return CellKpi.newBuilder()
            .setWindowStartTs(window.getStart())
            .setWindowEndTs(window.getEnd())
            .setWindowKind(WindowKind.MIN_5)
            .setJoinQuality(rollupJoinQuality(allJoined, allChrOnly, allPmOnly, hasChrEvidence, hasPmEvidence))
            .setSiteId(siteId)
            .setCellId(firstNonBlank(cellId, key))
            .setGridId(gridId)
            .setNumChrEvents(numChrEvents)
            .setNumUsers(numUsers)
            .setRsrpSampleCount(rsrpSampleCount)
            .setSinrSampleCount(sinrSampleCount)
            .setAttachAttempts(attachAttempts)
            .setAvgRsrp(weightedAvg(rsrpWeightedSum, rsrpSampleCount))
            .setAvgSinr(weightedAvg(sinrWeightedSum, sinrSampleCount))
            .setAvgPrbUsageDl(avg(prbUsageDlSum, pmMinutes))
            .setThroughputDlMbpsAvg(avg(throughputDlSum, pmMinutes))
            .setDropRate(avg(dropRateSum, pmMinutes))
            .setHoSuccessRate(avg(hoSuccessRateSum, pmMinutes))
            .setAttachSuccessRate(weightedAvg(attachWeightedSum, attachAttempts))
            .build();
    }

    private static void validateChild(String key, TimeWindow window, CellKpi child) {
        if (child.getWindowKind() != WindowKind.MIN_1) {
            throw new IllegalArgumentException("Expected MIN_1 child windowKind, got " + child.getWindowKind());
        }
        String childCellId = child.getCellId() == null ? "" : child.getCellId().toString();
        if (key != null && !key.isBlank() && !childCellId.isBlank() && !key.equals(childCellId)) {
            throw new IllegalArgumentException("Child cellId " + childCellId + " differs from key " + key);
        }
        if (child.getWindowEndTs() <= child.getWindowStartTs()) {
            throw new IllegalArgumentException(
                "Invalid child window bounds: " + child.getWindowStartTs() + ".." + child.getWindowEndTs());
        }
        if (child.getWindowStartTs() < window.getStart() || child.getWindowEndTs() > window.getEnd()) {
            throw new IllegalArgumentException(
                "Child window outside parent window: " + child.getWindowStartTs() + ".." + child.getWindowEndTs()
                    + " not within " + window.getStart() + ".." + window.getEnd());
        }
    }

    private static JoinQuality rollupJoinQuality(
        boolean allJoined,
        boolean allChrOnly,
        boolean allPmOnly,
        boolean hasChrEvidence,
        boolean hasPmEvidence) {
        if (allJoined) {
            return JoinQuality.JOINED;
        }
        if (allChrOnly) {
            return JoinQuality.CHR_ONLY;
        }
        if (allPmOnly) {
            return JoinQuality.PM_ONLY;
        }
        if (hasChrEvidence && hasPmEvidence) {
            return JoinQuality.JOINED;
        }
        return hasChrEvidence ? JoinQuality.CHR_ONLY : JoinQuality.PM_ONLY;
    }

    private static boolean hasPmMetric(CellKpi child) {
        return child.getAvgPrbUsageDl() != 0.0f
            || child.getThroughputDlMbpsAvg() != 0.0f
            || child.getDropRate() != 0.0f
            || child.getHoSuccessRate() != 0.0f;
    }

    private static float weightedAvg(double sum, long weight) {
        return weight > 0L ? (float) (sum / weight) : 0.0f;
    }

    private static float avg(double sum, long count) {
        return count > 0L ? (float) (sum / count) : 0.0f;
    }

    private static String firstNonBlank(String current, CharSequence next) {
        if (current != null && !current.isBlank()) {
            return current;
        }
        return next == null ? "" : next.toString();
    }
}

package com.fdb.job.kpi;

import com.fdb.job.model.ChrMinuteFact;
import com.fdb.job.model.MinuteFactEnvelope;
import com.fdb.job.model.PmMinuteFact;
import com.fdb.common.avro.CellKpi;
import com.fdb.common.avro.CfgConfig;
import com.fdb.common.avro.JoinQuality;
import com.fdb.common.avro.WindowKind;
import com.fdb.common.geo.Geohash;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class MinuteKpiJoinFunction
    extends KeyedProcessFunction<String, MinuteFactEnvelope, CellKpi> {

    private static final long MINUTE_MS = 60_000L;

    private final long waitMs;

    private transient MapState<Long, ChrMinuteFact> chrFactsByMinute;
    private transient MapState<Long, PmMinuteFact> pmFactsByMinute;
    private transient ValueState<CfgConfig> latestCfgByCell;
    private transient ValueState<Long> latestCfgVersionByCell;

    public MinuteKpiJoinFunction(Duration wait) {
        this.waitMs = wait.toMillis();
    }

    @Override
    public void open(Configuration parameters) {
        chrFactsByMinute = getRuntimeContext().getMapState(new MapStateDescriptor<>(
            "chr-facts-by-minute",
            Types.LONG,
            new GenericTypeInfo<>(ChrMinuteFact.class)));
        pmFactsByMinute = getRuntimeContext().getMapState(new MapStateDescriptor<>(
            "pm-facts-by-minute",
            Types.LONG,
            new GenericTypeInfo<>(PmMinuteFact.class)));
        latestCfgByCell = getRuntimeContext().getState(new ValueStateDescriptor<>(
            "latest-cfg-by-cell",
            new GenericTypeInfo<>(CfgConfig.class)));
        latestCfgVersionByCell = getRuntimeContext().getState(new ValueStateDescriptor<>(
            "latest-cfg-version-by-cell",
            Types.LONG));
    }

    @Override
    public void processElement(MinuteFactEnvelope envelope, Context ctx, Collector<CellKpi> out)
        throws Exception {
        switch (envelope.kind()) {
            case CFG -> processCfg(envelope.cfgConfig());
            case CHR -> processChr(envelope.chrFact(), ctx, out);
            case PM -> processPm(envelope.pmFact(), ctx, out);
        }
    }

    private void processCfg(CfgConfig cfg) throws Exception {
        Long latestVersion = latestCfgVersionByCell.value();
        CfgConfig existing = latestCfgByCell.value();
        if (latestVersion == null && existing != null) {
            latestVersion = existing.getVersion();
        }
        if (latestVersion != null && cfg.getVersion() <= latestVersion) {
            return;
        }

        latestCfgVersionByCell.update(cfg.getVersion());
        if (cfg.getTombstone()) {
            latestCfgByCell.clear();
        } else {
            latestCfgByCell.update(cfg);
        }
    }

    private void processChr(ChrMinuteFact chr, Context ctx, Collector<CellKpi> out) throws Exception {
        long timerTs = timerTs(chr.minuteTs());
        if (ctx.timerService().currentWatermark() >= timerTs) {
            return;
        }

        PmMinuteFact pm = pmFactsByMinute.get(chr.minuteTs());
        if (pm != null) {
            out.collect(toKpi(ctx.getCurrentKey(), chr, pm, JoinQuality.JOINED));
            clearMinute(chr.minuteTs(), ctx);
        } else {
            chrFactsByMinute.put(chr.minuteTs(), chr);
            ctx.timerService().registerEventTimeTimer(timerTs);
        }
    }

    private void processPm(PmMinuteFact pm, Context ctx, Collector<CellKpi> out) throws Exception {
        long timerTs = timerTs(pm.minuteTs());
        if (ctx.timerService().currentWatermark() >= timerTs) {
            return;
        }

        ChrMinuteFact chr = chrFactsByMinute.get(pm.minuteTs());
        if (chr != null) {
            out.collect(toKpi(ctx.getCurrentKey(), chr, pm, JoinQuality.JOINED));
            clearMinute(pm.minuteTs(), ctx);
        } else {
            pmFactsByMinute.put(pm.minuteTs(), pm);
            ctx.timerService().registerEventTimeTimer(timerTs);
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<CellKpi> out) throws Exception {
        long minuteTs = timestamp - MINUTE_MS - waitMs;
        ChrMinuteFact chr = chrFactsByMinute.get(minuteTs);
        PmMinuteFact pm = pmFactsByMinute.get(minuteTs);
        if (chr != null || pm != null) {
            JoinQuality joinQuality = chr != null && pm != null
                ? JoinQuality.JOINED
                : chr != null ? JoinQuality.CHR_ONLY : JoinQuality.PM_ONLY;
            out.collect(toKpi(ctx.getCurrentKey(), chr, pm, joinQuality));
            clearMinute(minuteTs, ctx);
        }
    }

    private void clearMinute(long minuteTs, KeyedProcessFunction<String, MinuteFactEnvelope, CellKpi>.Context ctx)
        throws Exception {
        chrFactsByMinute.remove(minuteTs);
        pmFactsByMinute.remove(minuteTs);
        ctx.timerService().deleteEventTimeTimer(timerTs(minuteTs));
    }

    private long timerTs(long minuteTs) {
        return minuteTs + MINUTE_MS + waitMs;
    }

    private CellKpi toKpi(String key, ChrMinuteFact chr, PmMinuteFact pm, JoinQuality joinQuality)
        throws Exception {
        CfgConfig cfg = latestCfgByCell.value();
        long minuteTs = chr != null ? chr.minuteTs() : pm.minuteTs();
        String cellId = chr != null ? chr.cellId() : pm != null ? pm.cellId() : key;
        String siteId = firstNonBlank(
            chr != null ? chr.siteId() : "",
            pm != null ? pm.siteId() : "",
            cfg != null ? cfg.getSiteId().toString() : "");
        String gridId = cfg == null ? "" : Geohash.encode(cfg.getCenterLat(), cfg.getCenterLon(), 6);
        long hoAttempts = pm == null ? 0L : pm.handoverSuccess() + pm.handoverFailure();

        return CellKpi.newBuilder()
            .setWindowStartTs(minuteTs)
            .setWindowEndTs(minuteTs + MINUTE_MS)
            .setWindowKind(WindowKind.MIN_1)
            .setJoinQuality(joinQuality)
            .setSiteId(siteId)
            .setCellId(cellId)
            .setGridId(gridId)
            .setNumChrEvents(chr == null ? 0L : chr.count())
            .setNumUsers(chr == null ? 0L : chr.uniqueUsers())
            .setRsrpSampleCount(chr == null ? 0L : chr.rsrpCount())
            .setSinrSampleCount(chr == null ? 0L : chr.sinrCount())
            .setAttachAttempts(chr == null ? 0L : chr.attachAttempts())
            .setAvgRsrp(chr == null ? 0.0f : avg(chr.rsrpSum(), chr.rsrpCount()))
            .setAvgSinr(chr == null ? 0.0f : avg(chr.sinrSum(), chr.sinrCount()))
            .setAvgPrbUsageDl(pm == null ? 0.0f : avg(pm.prbUsageDlSum(), pm.pmWindowCount()))
            .setThroughputDlMbpsAvg(pm == null ? 0.0f : avg(pm.throughputDlMbpsSum(), pm.pmWindowCount()))
            .setDropRate(pm == null ? 0.0f : avg(pm.dropCount(), pm.pmWindowCount()))
            .setHoSuccessRate(pm == null ? 0.0f : avg(pm.handoverSuccess(), hoAttempts))
            .setAttachSuccessRate(chr == null ? 0.0f : avg(chr.attachSuccess(), chr.attachAttempts()))
            .build();
    }

    private static float avg(double sum, long count) {
        return count > 0L ? (float) (sum / count) : 0.0f;
    }

    private static String firstNonBlank(String... values) {
        for (String value : values) {
            if (value != null && !value.isBlank()) {
                return value;
            }
        }
        return "";
    }
}

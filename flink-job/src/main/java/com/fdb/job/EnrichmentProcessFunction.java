package com.fdb.job;

import com.fdb.common.avro.ChrEvent;
import com.fdb.common.avro.CfgConfig;
import com.fdb.common.avro.PmStat;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class EnrichmentProcessFunction
    extends KeyedProcessFunction<String, RoutedEnvelope, EnrichedChr> {

    private static final Logger log = LoggerFactory.getLogger(EnrichmentProcessFunction.class);

    private transient ValueState<CfgConfig> cfgState;
    private transient ListState<PmStat> pmRing;
    private transient ListState<ChrEvent> bufferState;
    private transient ValueState<Long> bufferTimerState;
    public static final OutputTag<ChrEvent> CHR_DLQ =
        new OutputTag<>("chr-dlq", new GenericTypeInfo<>(ChrEvent.class));

    @Override
    public void open(Configuration parameters) {
        cfgState = getRuntimeContext().getState(
            new ValueStateDescriptor<>("cfg-config", new GenericTypeInfo<>(CfgConfig.class)));
        pmRing = getRuntimeContext().getListState(
            new ListStateDescriptor<>("pm-ring", new GenericTypeInfo<>(PmStat.class)));
        bufferState = getRuntimeContext().getListState(
            new ListStateDescriptor<>("chr-buffer", new GenericTypeInfo<>(ChrEvent.class)));
        bufferTimerState = getRuntimeContext().getState(
            new ValueStateDescriptor<>("chr-buffer-timer", Long.class));
    }

    @Override
    public void processElement(RoutedEnvelope routed, Context ctx, Collector<EnrichedChr> out) throws Exception {
        InputEnvelope envelope = routed.envelope();
        if (envelope instanceof InputEnvelope.ChrEnv chrEnv) {
            processChr(chrEnv.chrEvent(), ctx, out);
        } else if (envelope instanceof InputEnvelope.PmEnv pmEnv) {
            processPm(pmEnv.pmStat());
        } else if (envelope instanceof InputEnvelope.CfgEnv cfgEnv) {
            processCfg(cfgEnv.cfgConfig(), ctx, out);
        }
    }

    private void processChr(ChrEvent chr, Context ctx, Collector<EnrichedChr> out) throws Exception {
        CfgConfig cfg = cfgState.value();
        if (cfg == null) {
            bufferState.add(chr);
            if (bufferTimerState.value() == null) {
                long timer = ctx.timerService().currentProcessingTime() + 30_000;
                bufferTimerState.update(timer);
                ctx.timerService().registerProcessingTimeTimer(timer);
            }
            return;
        }

        PmStat latestPm = null;
        for (PmStat pm : pmRing.get()) {
            latestPm = pm;
        }

        out.collect(new EnrichedChr(chr, cfg, latestPm));
    }

    private void processPm(PmStat pm) throws Exception {
        pmRing.add(pm);
        List<PmStat> all = new ArrayList<>();
        pmRing.get().forEach(all::add);
        if (all.size() > 6) {
            pmRing.update(all.subList(all.size() - 6, all.size()));
        }
    }

    private void processCfg(CfgConfig cfg, Context ctx, Collector<EnrichedChr> out) throws Exception {
        CfgConfig existing = cfgState.value();
        if (cfg.getTombstone()) {
            cfgState.clear();
        } else if (existing == null || cfg.getVersion() > existing.getVersion()) {
            cfgState.update(cfg);
            flushBuffer(ctx, out);
        }
    }

    private void flushBuffer(Context ctx, Collector<EnrichedChr> out) throws Exception {
        CfgConfig cfg = cfgState.value();
        if (cfg == null) return;
        PmStat latestPm = null;
        for (PmStat pm : pmRing.get()) latestPm = pm;

        for (ChrEvent chr : bufferState.get()) {
            out.collect(new EnrichedChr(chr, cfg, latestPm));
        }
        bufferState.clear();
        bufferTimerState.clear();
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<EnrichedChr> out) throws Exception {
        if (bufferTimerState.value() == null || bufferTimerState.value() != timestamp) return;
        for (ChrEvent chr : bufferState.get()) ctx.output(CHR_DLQ, chr);
        bufferState.clear();
        bufferTimerState.clear();
        log.warn("Sent buffered CHR events to DLQ after CFG timeout for cell={}", ctx.getCurrentKey());
    }
}

package com.fdb.job;

import com.fdb.common.avro.ChrEvent;
import com.fdb.common.avro.CmConfig;
import com.fdb.common.avro.MrStat;
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

    private transient ValueState<CmConfig> cmState;
    private transient ListState<MrStat> mrRing;
    private transient ListState<ChrEvent> bufferState;
    private transient ValueState<Long> bufferTimerState;
    public static final OutputTag<ChrEvent> CHR_DLQ =
        new OutputTag<>("chr-dlq", new GenericTypeInfo<>(ChrEvent.class));

    @Override
    public void open(Configuration parameters) {
        cmState = getRuntimeContext().getState(
            new ValueStateDescriptor<>("cm-config", new GenericTypeInfo<>(CmConfig.class)));
        mrRing = getRuntimeContext().getListState(
            new ListStateDescriptor<>("mr-ring", new GenericTypeInfo<>(MrStat.class)));
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
        } else if (envelope instanceof InputEnvelope.MrEnv mrEnv) {
            processMr(mrEnv.mrStat());
        } else if (envelope instanceof InputEnvelope.CmEnv cmEnv) {
            processCm(cmEnv.cmConfig(), ctx, out);
        }
    }

    private void processChr(ChrEvent chr, Context ctx, Collector<EnrichedChr> out) throws Exception {
        CmConfig cm = cmState.value();
        if (cm == null) {
            bufferState.add(chr);
            if (bufferTimerState.value() == null) {
                long timer = ctx.timerService().currentProcessingTime() + 30_000;
                bufferTimerState.update(timer);
                ctx.timerService().registerProcessingTimeTimer(timer);
            }
            return;
        }

        MrStat latestMr = null;
        for (MrStat mr : mrRing.get()) {
            latestMr = mr;
        }

        out.collect(new EnrichedChr(chr, cm, latestMr));
    }

    private void processMr(MrStat mr) throws Exception {
        mrRing.add(mr);
        List<MrStat> all = new ArrayList<>();
        mrRing.get().forEach(all::add);
        if (all.size() > 6) {
            mrRing.update(all.subList(all.size() - 6, all.size()));
        }
    }

    private void processCm(CmConfig cm, Context ctx, Collector<EnrichedChr> out) throws Exception {
        CmConfig existing = cmState.value();
        if (cm.getTombstone()) {
            cmState.clear();
        } else if (existing == null || cm.getVersion() > existing.getVersion()) {
            cmState.update(cm);
            flushBuffer(ctx, out);
        }
    }

    private void flushBuffer(Context ctx, Collector<EnrichedChr> out) throws Exception {
        CmConfig cm = cmState.value();
        if (cm == null) return;
        MrStat latestMr = null;
        for (MrStat mr : mrRing.get()) latestMr = mr;

        for (ChrEvent chr : bufferState.get()) {
            out.collect(new EnrichedChr(chr, cm, latestMr));
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
        log.warn("Sent buffered CHR events to DLQ after CM timeout for cell={}", ctx.getCurrentKey());
    }
}

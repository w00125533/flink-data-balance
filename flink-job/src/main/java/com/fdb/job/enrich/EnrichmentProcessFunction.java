package com.fdb.job.enrich;

import com.fdb.job.model.EnrichedChr;
import com.fdb.job.model.InputEnvelope;
import com.fdb.job.model.RoutedEnvelope;
import com.fdb.common.avro.ChrEvent;
import com.fdb.common.avro.CfgConfig;
import com.fdb.common.avro.PmStat;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

public class EnrichmentProcessFunction
    extends KeyedProcessFunction<String, RoutedEnvelope, EnrichedChr> {

    private static final Duration CFG_STATE_TTL = Duration.ofHours(24);
    private static final Duration LATEST_PM_STATE_TTL = Duration.ofMinutes(10);

    private transient ValueState<CfgConfig> cfgState;
    private transient ValueState<PmStat> latestPmState;
    public static final OutputTag<ChrEvent> ENRICHMENT_LATE =
        new OutputTag<>("enrichment-late", new GenericTypeInfo<>(ChrEvent.class));

    @Override
    public void open(Configuration parameters) {
        cfgState = getRuntimeContext().getState(cfgStateDescriptor());
        latestPmState = getRuntimeContext().getState(latestPmStateDescriptor());
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
        PmStat latestPm = latestPmState.value();
        if (cfg == null) {
            ctx.output(ENRICHMENT_LATE, chr);
            out.collect(new EnrichedChr(chr, null, latestPm));
            return;
        }

        out.collect(new EnrichedChr(chr, cfg, latestPm));
    }

    private void processPm(PmStat pm) throws Exception {
        latestPmState.update(pm);
    }

    private void processCfg(CfgConfig cfg, Context ctx, Collector<EnrichedChr> out) throws Exception {
        CfgConfig existing = cfgState.value();
        if (cfg.getTombstone()) {
            cfgState.clear();
        } else if (existing == null || cfg.getVersion() > existing.getVersion()) {
            cfgState.update(cfg);
        }
    }

    static ValueStateDescriptor<CfgConfig> cfgStateDescriptor() {
        ValueStateDescriptor<CfgConfig> descriptor =
            new ValueStateDescriptor<>("cfg-config", new GenericTypeInfo<>(CfgConfig.class));
        descriptor.enableTimeToLive(StateTtlConfig.newBuilder(CFG_STATE_TTL)
            .updateTtlOnReadAndWrite()
            .neverReturnExpired()
            .cleanupFullSnapshot()
            .build());
        return descriptor;
    }

    static ValueStateDescriptor<PmStat> latestPmStateDescriptor() {
        ValueStateDescriptor<PmStat> descriptor =
            new ValueStateDescriptor<>("latest-pm", new GenericTypeInfo<>(PmStat.class));
        descriptor.enableTimeToLive(StateTtlConfig.newBuilder(LATEST_PM_STATE_TTL)
            .updateTtlOnCreateAndWrite()
            .neverReturnExpired()
            .cleanupFullSnapshot()
            .build());
        return descriptor;
    }
}

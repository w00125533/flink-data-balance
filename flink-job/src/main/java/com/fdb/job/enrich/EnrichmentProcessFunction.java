package com.fdb.job.enrich;

import com.fdb.job.model.EnrichedChr;
import com.fdb.job.model.InputEnvelope;
import com.fdb.job.model.RoutedEnvelope;
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

import java.util.ArrayList;
import java.util.List;

public class EnrichmentProcessFunction
    extends KeyedProcessFunction<String, RoutedEnvelope, EnrichedChr> {

    private transient ValueState<CfgConfig> cfgState;
    private transient ListState<PmStat> pmRing;
    public static final OutputTag<ChrEvent> ENRICHMENT_LATE =
        new OutputTag<>("enrichment-late", new GenericTypeInfo<>(ChrEvent.class));

    @Override
    public void open(Configuration parameters) {
        cfgState = getRuntimeContext().getState(
            new ValueStateDescriptor<>("cfg-config", new GenericTypeInfo<>(CfgConfig.class)));
        pmRing = getRuntimeContext().getListState(
            new ListStateDescriptor<>("pm-ring", new GenericTypeInfo<>(PmStat.class)));
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
            PmStat latestPm = latestPm();
            ctx.output(ENRICHMENT_LATE, chr);
            out.collect(new EnrichedChr(chr, null, latestPm));
            return;
        }

        out.collect(new EnrichedChr(chr, cfg, latestPm()));
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
        }
    }

    private PmStat latestPm() throws Exception {
        PmStat latestPm = null;
        for (PmStat pm : pmRing.get()) latestPm = pm;
        return latestPm;
    }
}

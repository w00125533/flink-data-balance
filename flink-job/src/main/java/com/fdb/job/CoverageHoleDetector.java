package com.fdb.job;

import com.fdb.common.avro.AnomalyEvent;
import com.fdb.common.avro.AnomalyType;
import com.fdb.common.avro.Severity;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class CoverageHoleDetector extends KeyedProcessFunction<String, EnrichedChr, AnomalyEvent> {
    private static final long WINDOW_MS = 300_000;
    private final RuleConfig rules;
    private transient ValueState<Long> bucketState;
    private transient ValueState<Integer> countState;
    private transient ValueState<Boolean> emittedState;

    public CoverageHoleDetector(RuleConfig rules) { this.rules = rules; }

    @Override
    public void open(Configuration parameters) {
        bucketState = getRuntimeContext().getState(new ValueStateDescriptor<>("coverage-bucket", Long.class));
        countState = getRuntimeContext().getState(new ValueStateDescriptor<>("coverage-count", Integer.class));
        emittedState = getRuntimeContext().getState(new ValueStateDescriptor<>("coverage-emitted", Boolean.class));
    }

    @Override
    public void processElement(EnrichedChr enriched, Context ctx, Collector<AnomalyEvent> out) throws Exception {
        var chr = enriched.chrEvent();
        if (chr.getRsrp() == null || chr.getRsrp() >= rules.rsrpThreshold()) return;
        long bucket = chr.getEventTs() / WINDOW_MS;
        if (bucketState.value() == null || bucketState.value() != bucket) {
            bucketState.update(bucket); countState.update(0); emittedState.update(false);
        }
        int count = (countState.value() == null ? 0 : countState.value()) + 1;
        countState.update(count);
        if (count >= rules.coverageHoleThreshold() && !Boolean.TRUE.equals(emittedState.value())) {
            emittedState.update(true);
            out.collect(AnomalyDetector.buildAnomaly(chr, ctx.getCurrentKey(), AnomalyType.COVERAGE_HOLE,
                Severity.HIGH, String.format("{\"low_signal_count\":%d,\"window_ms\":%d}", count, WINDOW_MS)));
        }
    }
}

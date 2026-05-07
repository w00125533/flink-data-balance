package com.fdb.job;

import com.fdb.common.avro.CellKpi;
import com.fdb.common.avro.WindowKind;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class CellKpiWindowFunction
    extends ProcessWindowFunction<EnrichedChr, CellKpi, String, TimeWindow> {

    private final WindowKind windowKind;
    private final KpiAggregator aggregator;

    public CellKpiWindowFunction(WindowKind windowKind) {
        this.windowKind = windowKind;
        this.aggregator = new KpiAggregator(windowKind);
    }

    @Override
    public void process(String cellId, Context ctx, Iterable<EnrichedChr> elements, Collector<CellKpi> out) {
        KpiAccumulator acc = new KpiAccumulator();
        acc.cellId = cellId;

        for (EnrichedChr ec : elements) {
            acc = aggregator.add(ec, acc);
        }

        acc.windowStartTs = ctx.window().getStart();
        acc.windowEndTs = ctx.window().getEnd();

        out.collect(aggregator.getResult(acc));
    }
}

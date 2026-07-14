package com.fdb.job.kpi;

import com.fdb.job.model.EnrichedChr;
import com.fdb.common.avro.CellKpi;
import com.fdb.common.avro.WindowKind;
import com.fdb.common.summary.SummarySwitch;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CellKpiWindowFunction
    extends ProcessWindowFunction<EnrichedChr, CellKpi, String, TimeWindow> {

    private static final Logger log = LoggerFactory.getLogger(CellKpiWindowFunction.class);

    private final WindowKind windowKind;
    private final KpiAggregator aggregator;
    private final boolean summaryEnabled;

    public CellKpiWindowFunction(WindowKind windowKind) {
        this.windowKind = windowKind;
        this.aggregator = new KpiAggregator(windowKind);
        this.summaryEnabled = SummarySwitch.enabled();
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

        CellKpi result = aggregator.getResult(acc);
        if (summaryEnabled) {
            log.info(SummarySwitch.format("flink-kpi", windowKind + ".cell", cellId));
            log.info(SummarySwitch.format("flink-kpi", windowKind + ".num_chr_events", result.getNumChrEvents()));
            log.info(SummarySwitch.format("flink-kpi", windowKind + ".window_ts",
                result.getWindowStartTs() + ".." + result.getWindowEndTs()));
        }
        out.collect(result);
    }
}

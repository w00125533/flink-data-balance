package com.fdb.job.kpi;

import com.fdb.job.model.ChrMinuteFact;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class ChrMinuteFactWindowFunction
    extends ProcessWindowFunction<ChrMinuteFactAccumulator, ChrMinuteFact, String, TimeWindow> {

    @Override
    public void process(
        String cellId,
        Context ctx,
        Iterable<ChrMinuteFactAccumulator> elements,
        Collector<ChrMinuteFact> out) {
        for (ChrMinuteFactAccumulator acc : elements) {
            if (acc.getCount() > 0L) {
                out.collect(acc.toMinuteFact(cellId, ctx.window().getStart()));
            }
        }
    }
}

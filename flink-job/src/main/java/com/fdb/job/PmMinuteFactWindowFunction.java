package com.fdb.job;

import com.fdb.common.avro.PmStat;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class PmMinuteFactWindowFunction
    extends ProcessWindowFunction<PmStat, PmMinuteFact, String, TimeWindow> {

    @Override
    public void process(String cellId, Context ctx, Iterable<PmStat> elements, Collector<PmMinuteFact> out) {
        String siteId = "";
        long count = 0L;
        double prbUsageDlSum = 0.0;
        double throughputDlMbpsSum = 0.0;
        long dropCount = 0L;
        long handoverSuccess = 0L;
        long handoverFailure = 0L;

        for (PmStat pm : elements) {
            if (count == 0L && pm.getSiteId() != null) {
                siteId = pm.getSiteId().toString();
            }
            count++;
            prbUsageDlSum += pm.getPrbUsageDl();
            throughputDlMbpsSum += pm.getThroughputDlMbps();
            dropCount += pm.getDroppedConnections();
            handoverSuccess += pm.getHandoverSuccess();
            handoverFailure += pm.getHandoverFailure();
        }

        if (count > 0L) {
            out.collect(new PmMinuteFact(cellId, siteId, ctx.window().getStart(), count,
                prbUsageDlSum, throughputDlMbpsSum, dropCount, handoverSuccess, handoverFailure));
        }
    }
}

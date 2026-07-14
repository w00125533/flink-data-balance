package com.fdb.job;

import com.fdb.common.avro.ChrEvent;
import com.fdb.common.avro.ChrEventType;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.Set;

public class ChrMinuteFactWindowFunction
    extends ProcessWindowFunction<ChrEvent, ChrMinuteFact, String, TimeWindow> {

    @Override
    public void process(String cellId, Context ctx, Iterable<ChrEvent> elements, Collector<ChrMinuteFact> out) {
        String siteId = "";
        long count = 0L;
        Set<String> users = new HashSet<>();
        double rsrpSum = 0.0;
        long rsrpCount = 0L;
        double sinrSum = 0.0;
        long sinrCount = 0L;
        long attachAttempts = 0L;
        long attachSuccess = 0L;

        for (ChrEvent chr : elements) {
            if (count == 0L && chr.getSiteId() != null) {
                siteId = chr.getSiteId().toString();
            }
            count++;
            if (chr.getImsi() != null) {
                users.add(chr.getImsi().toString());
            }
            if (chr.getRsrp() != null) {
                rsrpSum += chr.getRsrp();
                rsrpCount++;
            }
            if (chr.getSinr() != null) {
                sinrSum += chr.getSinr();
                sinrCount++;
            }
            if (chr.getEventType() == ChrEventType.ATTACH) {
                attachAttempts++;
                if (chr.getResultCode() == 0) {
                    attachSuccess++;
                }
            }
        }

        if (count > 0L) {
            out.collect(new ChrMinuteFact(cellId, siteId, ctx.window().getStart(), count, users.size(),
                rsrpSum, sinrSum, attachAttempts, attachSuccess, rsrpCount, sinrCount));
        }
    }
}

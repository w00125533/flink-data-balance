package com.fdb.job.kpi;

import com.fdb.common.avro.ChrEvent;
import com.fdb.common.avro.ChrEventType;
import org.apache.flink.api.common.functions.AggregateFunction;

public class ChrMinuteFactAggregateFunction
    implements AggregateFunction<ChrEvent, ChrMinuteFactAccumulator, ChrMinuteFactAccumulator> {

    @Override
    public ChrMinuteFactAccumulator createAccumulator() {
        return new ChrMinuteFactAccumulator();
    }

    @Override
    public ChrMinuteFactAccumulator add(ChrEvent chr, ChrMinuteFactAccumulator acc) {
        if (acc.getCount() == 0L) {
            setTextIfPresent(chr.getCellId(), acc::setCellId);
            setTextIfPresent(chr.getSiteId(), acc::setSiteId);
        }
        acc.setCount(acc.getCount() + 1L);
        if (chr.getImsi() != null) {
            acc.addUser(chr.getImsi().toString());
        }
        if (chr.getRsrp() != null) {
            acc.setRsrpSum(acc.getRsrpSum() + chr.getRsrp());
            acc.setRsrpCount(acc.getRsrpCount() + 1L);
        }
        if (chr.getSinr() != null) {
            acc.setSinrSum(acc.getSinrSum() + chr.getSinr());
            acc.setSinrCount(acc.getSinrCount() + 1L);
        }
        if (chr.getEventType() == ChrEventType.ATTACH) {
            acc.setAttachAttempts(acc.getAttachAttempts() + 1L);
            if (chr.getResultCode() == 0) {
                acc.setAttachSuccess(acc.getAttachSuccess() + 1L);
            }
        }
        return acc;
    }

    @Override
    public ChrMinuteFactAccumulator getResult(ChrMinuteFactAccumulator acc) {
        return acc;
    }

    @Override
    public ChrMinuteFactAccumulator merge(ChrMinuteFactAccumulator left, ChrMinuteFactAccumulator right) {
        if (left.getCount() == 0L) {
            return right;
        }
        if (right.getCount() == 0L) {
            return left;
        }
        left.setCount(left.getCount() + right.getCount());
        left.mergeUsers(right.getUsers());
        left.setRsrpSum(left.getRsrpSum() + right.getRsrpSum());
        left.setRsrpCount(left.getRsrpCount() + right.getRsrpCount());
        left.setSinrSum(left.getSinrSum() + right.getSinrSum());
        left.setSinrCount(left.getSinrCount() + right.getSinrCount());
        left.setAttachAttempts(left.getAttachAttempts() + right.getAttachAttempts());
        left.setAttachSuccess(left.getAttachSuccess() + right.getAttachSuccess());
        return left;
    }

    private static void setTextIfPresent(Object value, TextSetter setter) {
        if (value != null) {
            setter.set(value.toString());
        }
    }

    @FunctionalInterface
    private interface TextSetter {
        void set(String value);
    }
}

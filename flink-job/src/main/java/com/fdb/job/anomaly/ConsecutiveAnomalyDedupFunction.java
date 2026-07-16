package com.fdb.job.anomaly;

import com.fdb.common.avro.AnomalyEvent;
import java.time.Duration;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

final class ConsecutiveAnomalyDedupFunction
    extends KeyedProcessFunction<String, AnomalyRuleEvaluation, AnomalyEvent> {
    private final String stateNamePrefix;
    private final int requiredConsecutive;
    private final long windowMillis;
    private transient ValueState<Integer> consecutiveBadCount;
    private transient ValueState<Long> firstBadTs;
    private transient ValueState<Boolean> active;

    ConsecutiveAnomalyDedupFunction(String stateNamePrefix, int requiredConsecutive, Duration window) {
        this.stateNamePrefix = stateNamePrefix;
        this.requiredConsecutive = Math.max(1, requiredConsecutive);
        this.windowMillis = Math.max(1L, window.toMillis());
    }

    @Override
    public void open(Configuration parameters) {
        consecutiveBadCount = getRuntimeContext().getState(
            new ValueStateDescriptor<>(stateNamePrefix + "-consecutive-bad-count", Integer.class));
        firstBadTs = getRuntimeContext().getState(
            new ValueStateDescriptor<>(stateNamePrefix + "-first-bad-ts", Long.class));
        active = getRuntimeContext().getState(
            new ValueStateDescriptor<>(stateNamePrefix + "-active", Boolean.class));
    }

    @Override
    public void processElement(AnomalyRuleEvaluation evaluation, Context ctx, Collector<AnomalyEvent> out)
        throws Exception {
        if (!evaluation.abnormal()) {
            clearStreak();
            active.update(false);
            return;
        }
        int count = updateStreak(evaluation.eventTs());
        if (count >= requiredConsecutive && !Boolean.TRUE.equals(active.value())) {
            active.update(true);
            out.collect(AnomalyEventFactory.fromEvaluation(evaluation));
        }
    }

    private int updateStreak(long eventTs) throws Exception {
        Long firstTs = firstBadTs.value();
        Integer count = consecutiveBadCount.value();
        if (firstTs == null || count == null || eventTs - firstTs > windowMillis) {
            firstBadTs.update(eventTs);
            consecutiveBadCount.update(1);
            return 1;
        }
        int nextCount = count + 1;
        consecutiveBadCount.update(nextCount);
        return nextCount;
    }

    private void clearStreak() throws Exception {
        consecutiveBadCount.clear();
        firstBadTs.clear();
    }
}

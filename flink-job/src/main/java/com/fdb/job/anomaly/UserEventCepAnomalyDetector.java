package com.fdb.job.anomaly;

import com.fdb.common.avro.AnomalyEvent;
import com.fdb.job.config.RuleConfig;
import com.fdb.job.model.EnrichedChr;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

public final class UserEventCepAnomalyDetector {
    private UserEventCepAnomalyDetector() {}

    public static DataStream<AnomalyEvent> detect(DataStream<EnrichedChr> input, RuleConfig rules) {
        return detect(input, rules, false);
    }

    public static DataStream<AnomalyEvent> detect(
        DataStream<EnrichedChr> input,
        RuleConfig rules,
        boolean diagnosticChainingEnabled) {
        return detect(input, rules, diagnosticChainingEnabled, -1);
    }

    public static DataStream<AnomalyEvent> detect(
        DataStream<EnrichedChr> input,
        RuleConfig rules,
        boolean diagnosticChainingEnabled,
        int parallelism) {
        SingleOutputStreamOperator<AnomalyEvent> anomalies = input
            .keyBy(UserEventAnomalyProcessFunction::key)
            .process(new UserEventAnomalyProcessFunction(rules), new GenericTypeInfo<>(AnomalyEvent.class))
            .name("user-event-anomaly-detect")
            .uid("user-event-anomaly-detect");
        if (parallelism > 0) {
            anomalies = anomalies.setParallelism(parallelism);
        }
        return disableChainingIfDiagnostic(anomalies, diagnosticChainingEnabled);
    }

    private static <T> SingleOutputStreamOperator<T> disableChainingIfDiagnostic(
        SingleOutputStreamOperator<T> operator,
        boolean diagnosticChainingEnabled) {
        if (diagnosticChainingEnabled) {
            return operator.disableChaining();
        }
        return operator;
    }

}

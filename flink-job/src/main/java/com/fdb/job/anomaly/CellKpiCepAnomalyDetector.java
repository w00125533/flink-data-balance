package com.fdb.job.anomaly;

import com.fdb.common.avro.AnomalyEvent;
import com.fdb.common.avro.AnomalyType;
import com.fdb.common.avro.CellKpi;
import com.fdb.common.avro.EntityType;
import com.fdb.common.avro.Severity;
import com.fdb.common.avro.WindowKind;
import com.fdb.job.config.RuleConfig;
import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public final class CellKpiCepAnomalyDetector {
    private static final String RADIO_DIMENSION = "cellRadio";
    private static final String SERVICE_DIMENSION = "cellService";

    private CellKpiCepAnomalyDetector() {}

    public static DataStream<AnomalyEvent> detect(DataStream<CellKpi> input, RuleConfig rules) {
        DataStream<AnomalyRuleEvaluation> evaluations = input
            .flatMap((CellKpi kpi, Collector<AnomalyRuleEvaluation> out) -> emitEvaluations(kpi, rules, out))
            .returns(new GenericTypeInfo<>(AnomalyRuleEvaluation.class))
            .assignTimestampsAndWatermarks(WatermarkStrategy
                .<AnomalyRuleEvaluation>forMonotonousTimestamps()
                .withTimestampAssigner((evaluation, timestamp) -> evaluation.eventTs()))
            .name("cell-kpi-anomaly-evaluations");

        DataStream<AnomalySignal> triggers = triggerSignals(evaluations, rules.cellConsecutiveMinutes());
        DataStream<AnomalySignal> recoveries = evaluations
            .filter(evaluation -> !evaluation.abnormal())
            .map(AnomalySignal::recovery)
            .returns(new GenericTypeInfo<>(AnomalySignal.class))
            .name("cell-kpi-anomaly-recoveries");

        return triggers.union(recoveries)
            .keyBy(AnomalySignal::key)
            .process(new ActivationFunction(), new GenericTypeInfo<>(AnomalyEvent.class))
            .name("cell-kpi-cep-anomaly-activation");
    }

    private static DataStream<AnomalySignal> triggerSignals(
        DataStream<AnomalyRuleEvaluation> evaluations,
        int consecutiveMinutes) {
        Pattern<AnomalyRuleEvaluation, ?> pattern = Pattern
            .<AnomalyRuleEvaluation>begin("first", AfterMatchSkipStrategy.skipPastLastEvent())
            .where(new AbnormalCondition())
            .next("second")
            .where(new AbnormalCondition())
            .next("third")
            .where(new AbnormalCondition())
            .within(Duration.ofMinutes(Math.max(1, consecutiveMinutes)));

        return CEP.pattern(evaluations.keyBy(AnomalyRuleEvaluation::key), pattern)
            .process(new PatternProcessFunction<AnomalyRuleEvaluation, AnomalySignal>() {
                @Override
                public void processMatch(
                    Map<String, List<AnomalyRuleEvaluation>> match,
                    Context ctx,
                    Collector<AnomalySignal> out) {
                    out.collect(AnomalySignal.trigger(List.of(
                        match.get("first").get(0),
                        match.get("second").get(0),
                        match.get("third").get(0))));
                }
            })
            .returns(new GenericTypeInfo<>(AnomalySignal.class))
            .name("cell-kpi-cep-anomaly-triggers");
    }

    private static void emitEvaluations(CellKpi kpi, RuleConfig rules, Collector<AnomalyRuleEvaluation> out) {
        if (kpi.getWindowKind() != WindowKind.MIN_1) {
            return;
        }
        out.collect(radioEvaluation(kpi, rules));
        out.collect(serviceEvaluation(kpi, rules));
    }

    private static AnomalyRuleEvaluation radioEvaluation(CellKpi kpi, RuleConfig rules) {
        boolean badRsrp = kpi.getAvgRsrp() < rules.cellRsrpMin();
        boolean badSinr = kpi.getAvgSinr() < rules.cellSinrMin();
        String metric = badRsrp ? "avgRsrp" : "avgSinr";
        double threshold = badRsrp ? rules.cellRsrpMin() : rules.cellSinrMin();
        double observed = badRsrp ? kpi.getAvgRsrp() : kpi.getAvgSinr();
        return evaluation(
            kpi,
            RADIO_DIMENSION,
            badRsrp || badSinr,
            AnomalyType.CELL_RADIO_BAD,
            Severity.MEDIUM,
            rules.ruleVersion(),
            metric,
            threshold,
            observed);
    }

    private static AnomalyRuleEvaluation serviceEvaluation(CellKpi kpi, RuleConfig rules) {
        boolean badAttach = kpi.getAttachSuccessRate() < rules.cellAttachSuccessMin();
        boolean badHo = kpi.getHoSuccessRate() < rules.cellHoSuccessMin();
        boolean badDrop = kpi.getDropRate() > rules.cellDropRateMax();
        String metric;
        double threshold;
        double observed;
        if (badAttach) {
            metric = "attachSuccessRate";
            threshold = rules.cellAttachSuccessMin();
            observed = kpi.getAttachSuccessRate();
        } else if (badHo) {
            metric = "hoSuccessRate";
            threshold = rules.cellHoSuccessMin();
            observed = kpi.getHoSuccessRate();
        } else {
            metric = "dropRate";
            threshold = rules.cellDropRateMax();
            observed = kpi.getDropRate();
        }
        return evaluation(
            kpi,
            SERVICE_DIMENSION,
            badAttach || badHo || badDrop,
            AnomalyType.CELL_SERVICE_BAD,
            Severity.HIGH,
            rules.ruleVersion(),
            metric,
            threshold,
            observed);
    }

    private static AnomalyRuleEvaluation evaluation(
        CellKpi kpi,
        String dimension,
        boolean abnormal,
        AnomalyType anomalyType,
        Severity severity,
        String ruleVersion,
        String metric,
        double threshold,
        double observed) {
        String cellId = value(kpi.getCellId());
        return new AnomalyRuleEvaluation(
            EntityType.CELL,
            cellId,
            dimension,
            abnormal,
            kpi.getWindowStartTs(),
            kpi.getWindowEndTs(),
            kpi.getWindowEndTs(),
            value(kpi.getSiteId()),
            cellId,
            null,
            value(kpi.getGridId()),
            null,
            null,
            anomalyType,
            severity,
            ruleVersion,
            metric,
            threshold,
            observed,
            contextJson(metric, threshold, observed, value(kpi.getWindowKind())));
    }

    private static String contextJson(String metric, double threshold, double observed, String windowKind) {
        return String.format(
            Locale.ROOT,
            "{\"metric\":\"%s\",\"threshold\":%.6f,\"observed\":%.6f,\"windowKind\":\"%s\"}",
            metric,
            threshold,
            observed,
            windowKind);
    }

    private static String value(Object value) {
        return value == null ? null : value.toString();
    }

    private static final class AbnormalCondition extends SimpleCondition<AnomalyRuleEvaluation> {
        @Override
        public boolean filter(AnomalyRuleEvaluation value) {
            return value.abnormal();
        }
    }

    private static final class ActivationFunction
        extends KeyedProcessFunction<String, AnomalySignal, AnomalyEvent> {
        private transient ValueState<Boolean> active;
        private transient ValueState<Long> activeSinceTs;
        private transient ValueState<Long> lastRecoveryTs;

        @Override
        public void open(Configuration parameters) {
            active = getRuntimeContext().getState(
                new ValueStateDescriptor<>("cell-kpi-anomaly-active", Boolean.class));
            activeSinceTs = getRuntimeContext().getState(
                new ValueStateDescriptor<>("cell-kpi-anomaly-active-since-ts", Long.class));
            lastRecoveryTs = getRuntimeContext().getState(
                new ValueStateDescriptor<>("cell-kpi-anomaly-last-recovery-ts", Long.class));
        }

        @Override
        public void processElement(AnomalySignal signal, Context ctx, Collector<AnomalyEvent> out)
            throws Exception {
            if (signal.type() == AnomalySignal.SignalType.RECOVERY) {
                Long previousRecovery = lastRecoveryTs.value();
                long eventTs = signal.current().eventTs();
                if (previousRecovery == null || eventTs > previousRecovery) {
                    lastRecoveryTs.update(eventTs);
                }
                active.update(false);
                return;
            }
            Long recoveryTs = lastRecoveryTs.value();
            Long activeTs = activeSinceTs.value();
            boolean recoveredAfterActivation = recoveryTs != null && (activeTs == null || recoveryTs > activeTs);
            if (!Boolean.TRUE.equals(active.value()) || recoveredAfterActivation) {
                active.update(true);
                activeSinceTs.update(signal.current().eventTs());
                out.collect(AnomalyEventFactory.fromEvaluation(signal.current()));
            }
        }
    }
}

package com.fdb.job.anomaly;

import com.fdb.common.avro.AnomalyEvent;
import com.fdb.common.avro.AnomalyType;
import com.fdb.common.avro.CellKpi;
import com.fdb.common.avro.EntityType;
import com.fdb.common.avro.Severity;
import com.fdb.common.avro.WindowKind;
import com.fdb.job.config.RuleConfig;
import java.time.Duration;
import java.util.Locale;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
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

        int consecutiveMinutes = Math.max(1, rules.cellConsecutiveMinutes());
        return evaluations
            .keyBy(AnomalyRuleEvaluation::key)
            .process(
                new ConsecutiveAnomalyDedupFunction(
                    "cell-kpi-anomaly",
                    consecutiveMinutes,
                    Duration.ofMinutes(consecutiveMinutes)),
                new GenericTypeInfo<>(AnomalyEvent.class))
            .name("cell-kpi-anomaly-detect-dedup");
    }

    private static void emitEvaluations(CellKpi kpi, RuleConfig rules, Collector<AnomalyRuleEvaluation> out) {
        if (kpi.getWindowKind() != WindowKind.MIN_1) {
            return;
        }
        out.collect(radioEvaluation(kpi, rules));
        out.collect(serviceEvaluation(kpi, rules));
    }

    private static AnomalyRuleEvaluation radioEvaluation(CellKpi kpi, RuleConfig rules) {
        boolean hasRsrpSamples = kpi.getRsrpSampleCount() > 0L;
        boolean hasSinrSamples = kpi.getSinrSampleCount() > 0L;
        boolean badRsrp = hasRsrpSamples && kpi.getAvgRsrp() < rules.cellRsrpMin();
        boolean badSinr = hasSinrSamples && kpi.getAvgSinr() < rules.cellSinrMin();
        boolean useRsrpMetric = badRsrp || (!badSinr && hasRsrpSamples);
        String metric = useRsrpMetric ? "avgRsrp" : "avgSinr";
        double threshold = useRsrpMetric ? rules.cellRsrpMin() : rules.cellSinrMin();
        double observed = useRsrpMetric ? kpi.getAvgRsrp() : kpi.getAvgSinr();
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
        boolean badAttach = kpi.getAttachAttempts() > 0L
            && kpi.getAttachSuccessRate() < rules.cellAttachSuccessMin();
        // CellKpi currently carries the HO success ratio but not the attempt denominator.
        // Without sample evidence, this rule is too noisy for cell-level service alarms.
        boolean badDrop = kpi.getDropRate() > rules.cellDropRateMax();
        String metric;
        double threshold;
        double observed;
        if (badAttach) {
            metric = "attachSuccessRate";
            threshold = rules.cellAttachSuccessMin();
            observed = kpi.getAttachSuccessRate();
        } else if (badDrop) {
            metric = "dropRate";
            threshold = rules.cellDropRateMax();
            observed = kpi.getDropRate();
        } else {
            metric = "service";
            threshold = 0.0;
            observed = 0.0;
        }
        return evaluation(
            kpi,
            SERVICE_DIMENSION,
            badAttach || badDrop,
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
            contextJson(metric, threshold, observed, value(kpi.getWindowKind())),
            kpi.getSourceEventTsAvg(),
            kpi.getSourceEventTsMin(),
            kpi.getSourceEventTsMax(),
            kpi.getSourceEventCount());
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

}

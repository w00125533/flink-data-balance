package com.fdb.job.anomaly;

import com.fdb.common.avro.AnomalyEvent;
import com.fdb.common.avro.AnomalyType;
import com.fdb.common.avro.ChrEvent;
import com.fdb.common.avro.ChrEventType;
import com.fdb.common.avro.EntityType;
import com.fdb.common.avro.Severity;
import com.fdb.job.config.RuleConfig;
import com.fdb.job.model.EnrichedChr;
import java.time.Duration;
import java.util.Locale;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;

public final class UserEventCepAnomalyDetector {
    private static final String ACCESS_FAILURE_DIMENSION = "accessFailure";
    private static final String HANDOVER_FAILURE_DIMENSION = "handoverFailure";

    private UserEventCepAnomalyDetector() {}

    public static DataStream<AnomalyEvent> detect(DataStream<EnrichedChr> input, RuleConfig rules) {
        DataStream<AnomalyRuleEvaluation> evaluations = input
            .flatMap((EnrichedChr enriched, Collector<AnomalyRuleEvaluation> out) ->
                emitEvaluations(enriched, rules, out))
            .returns(new GenericTypeInfo<>(AnomalyRuleEvaluation.class))
            .assignTimestampsAndWatermarks(WatermarkStrategy
                .<AnomalyRuleEvaluation>forMonotonousTimestamps()
                .withTimestampAssigner((evaluation, timestamp) -> evaluation.eventTs()))
            .name("user-event-anomaly-evaluations");

        return evaluations
            .keyBy(AnomalyRuleEvaluation::key)
            .process(
                new ConsecutiveAnomalyDedupFunction(
                    "user-event-anomaly",
                    rules.userConsecutiveEvents(),
                    Duration.ofMinutes(Math.max(1, rules.userWindowMinutes()))),
                new GenericTypeInfo<>(AnomalyEvent.class))
            .name("user-event-anomaly-detect-dedup");
    }

    private static void emitEvaluations(
        EnrichedChr enriched,
        RuleConfig rules,
        Collector<AnomalyRuleEvaluation> out) {
        if (enriched == null || enriched.chrEvent() == null) {
            return;
        }
        ChrEvent chr = enriched.chrEvent();
        if (isBlank(chr.getImsi())) {
            return;
        }
        if (chr.getEventType() == ChrEventType.ATTACH
            || chr.getEventType() == ChrEventType.SERVICE_REQUEST
            || chr.getEventType() == ChrEventType.RRC_SETUP_FAIL) {
            out.collect(failureEvaluation(chr, rules, ACCESS_FAILURE_DIMENSION, chr.getResultCode() != 0));
        }
        if (chr.getEventType() == ChrEventType.HANDOVER) {
            out.collect(failureEvaluation(chr, rules, HANDOVER_FAILURE_DIMENSION, chr.getResultCode() != 0));
        }
        if (chr.getRsrp() != null) {
            out.collect(qoeEvaluation(
                chr,
                rules,
                "rsrp",
                chr.getRsrp() < rules.userRsrpMin(),
                rules.userRsrpMin(),
                chr.getRsrp()));
        }
        if (chr.getSinr() != null) {
            out.collect(qoeEvaluation(
                chr,
                rules,
                "sinr",
                chr.getSinr() < rules.userSinrMin(),
                rules.userSinrMin(),
                chr.getSinr()));
        }
        if (chr.getLatencyMs() != null) {
            out.collect(qoeEvaluation(
                chr,
                rules,
                "latencyMs",
                chr.getLatencyMs() > rules.userLatencyMsMax(),
                rules.userLatencyMsMax(),
                chr.getLatencyMs()));
        }
    }

    private static AnomalyRuleEvaluation failureEvaluation(
        ChrEvent chr,
        RuleConfig rules,
        String dimension,
        boolean abnormal) {
        return evaluation(
            chr,
            rules,
            dimension,
            abnormal,
            AnomalyType.USER_FAILURE,
            Severity.HIGH,
            "resultCode",
            0d,
            chr.getResultCode());
    }

    private static AnomalyRuleEvaluation qoeEvaluation(
        ChrEvent chr,
        RuleConfig rules,
        String metric,
        boolean abnormal,
        double threshold,
        double observed) {
        return evaluation(
            chr,
            rules,
            metric,
            abnormal,
            AnomalyType.USER_QOE_BAD,
            Severity.MEDIUM,
            metric,
            threshold,
            observed);
    }

    private static AnomalyRuleEvaluation evaluation(
        ChrEvent chr,
        RuleConfig rules,
        String dimension,
        boolean abnormal,
        AnomalyType anomalyType,
        Severity severity,
        String metric,
        double threshold,
        double observed) {
        long windowMillis = Duration.ofMinutes(Math.max(1, rules.userWindowMinutes())).toMillis();
        String imsi = value(chr.getImsi());
        return new AnomalyRuleEvaluation(
            EntityType.USER,
            imsi,
            dimension,
            abnormal,
            chr.getEventTs() - windowMillis,
            chr.getEventTs(),
            chr.getEventTs(),
            value(chr.getSiteId()),
            value(chr.getCellId()),
            imsi,
            value(chr.getGridId()),
            chr.getLatitude(),
            chr.getLongitude(),
            anomalyType,
            severity,
            rules.ruleVersion(),
            metric,
            threshold,
            observed,
            contextJson(chr, metric, threshold, observed));
    }

    private static String contextJson(ChrEvent chr, String metric, double threshold, double observed) {
        return String.format(
            Locale.ROOT,
            "{\"metric\":\"%s\",\"threshold\":%.6f,\"observed\":%.6f,\"eventType\":\"%s\",\"resultCode\":%d}",
            metric,
            threshold,
            observed,
            value(chr.getEventType()),
            chr.getResultCode());
    }

    private static boolean isBlank(Object value) {
        return value == null || value.toString().trim().isEmpty();
    }

    private static String value(Object value) {
        return value == null ? null : value.toString();
    }

}

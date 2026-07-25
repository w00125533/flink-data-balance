package com.fdb.job.anomaly;

import com.fdb.common.avro.AnomalyEvent;
import com.fdb.common.avro.AnomalyType;
import com.fdb.common.avro.ChrEvent;
import com.fdb.common.avro.ChrEventType;
import com.fdb.common.avro.EntityType;
import com.fdb.common.avro.Severity;
import com.fdb.job.config.RuleConfig;
import com.fdb.job.model.EnrichedChr;
import java.io.Serializable;
import java.time.Duration;
import java.util.Locale;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class UserEventAnomalyProcessFunction extends KeyedProcessFunction<String, EnrichedChr, AnomalyEvent> {
    static final String ACCESS_FAILURE_DIMENSION = "accessFailure";
    static final String HANDOVER_FAILURE_DIMENSION = "handoverFailure";

    private final RuleConfig rules;
    private final int requiredConsecutive;
    private final long windowMillis;
    private transient MapState<String, DimensionState> dimensionStates;

    public UserEventAnomalyProcessFunction(RuleConfig rules) {
        this.rules = rules;
        this.requiredConsecutive = Math.max(1, rules.userConsecutiveEvents());
        this.windowMillis = Duration.ofMinutes(Math.max(1, rules.userWindowMinutes())).toMillis();
    }

    @Override
    public void open(Configuration parameters) {
        dimensionStates = getRuntimeContext().getMapState(new MapStateDescriptor<>(
            "user-event-anomaly-dimension-state", String.class, DimensionState.class));
    }

    @Override
    public void processElement(EnrichedChr enriched, Context ctx, Collector<AnomalyEvent> out) throws Exception {
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
            processEvaluation(
                failureEvaluation(chr, ACCESS_FAILURE_DIMENSION, chr.getResultCode() != 0), out);
        }
        if (chr.getEventType() == ChrEventType.HANDOVER) {
            processEvaluation(
                failureEvaluation(chr, HANDOVER_FAILURE_DIMENSION, chr.getResultCode() != 0), out);
        }
        if (chr.getRsrp() != null) {
            processEvaluation(qoeEvaluation(
                chr,
                "rsrp",
                chr.getRsrp() < rules.userRsrpMin(),
                rules.userRsrpMin(),
                chr.getRsrp()), out);
        }
        if (chr.getSinr() != null) {
            processEvaluation(qoeEvaluation(
                chr,
                "sinr",
                chr.getSinr() < rules.userSinrMin(),
                rules.userSinrMin(),
                chr.getSinr()), out);
        }
        if (chr.getLatencyMs() != null) {
            processEvaluation(qoeEvaluation(
                chr,
                "latencyMs",
                chr.getLatencyMs() > rules.userLatencyMsMax(),
                rules.userLatencyMsMax(),
                chr.getLatencyMs()), out);
        }
    }

    private void processEvaluation(AnomalyRuleEvaluation evaluation, Collector<AnomalyEvent> out) throws Exception {
        String dimension = evaluation.ruleDimension();
        DimensionState state = dimensionStates.get(dimension);
        if (!evaluation.abnormal()) {
            DimensionState reset = state == null ? new DimensionState() : state;
            reset.consecutiveBadCount = 0;
            reset.firstBadTs = 0L;
            reset.active = false;
            dimensionStates.put(dimension, reset);
            return;
        }

        DimensionState next = state == null ? new DimensionState() : state;
        if (next.firstBadTs <= 0L || next.consecutiveBadCount <= 0
            || evaluation.eventTs() - next.firstBadTs > windowMillis) {
            next.firstBadTs = evaluation.eventTs();
            next.consecutiveBadCount = 1;
        } else {
            next.consecutiveBadCount++;
        }
        if (next.consecutiveBadCount >= requiredConsecutive && !next.active) {
            next.active = true;
            out.collect(AnomalyEventFactory.fromEvaluation(evaluation));
        }
        dimensionStates.put(dimension, next);
    }

    private AnomalyRuleEvaluation failureEvaluation(ChrEvent chr, String dimension, boolean abnormal) {
        return evaluation(
            chr,
            dimension,
            abnormal,
            AnomalyType.USER_FAILURE,
            Severity.HIGH,
            "resultCode",
            0d,
            chr.getResultCode());
    }

    private AnomalyRuleEvaluation qoeEvaluation(
        ChrEvent chr,
        String metric,
        boolean abnormal,
        double threshold,
        double observed) {
        return evaluation(
            chr,
            metric,
            abnormal,
            AnomalyType.USER_QOE_BAD,
            Severity.MEDIUM,
            metric,
            threshold,
            observed);
    }

    private AnomalyRuleEvaluation evaluation(
        ChrEvent chr,
        String dimension,
        boolean abnormal,
        AnomalyType anomalyType,
        Severity severity,
        String metric,
        double threshold,
        double observed) {
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
            contextJson(chr, metric, threshold, observed),
            chr.getEventTs(),
            chr.getEventTs(),
            chr.getEventTs(),
            1L);
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

    static String key(EnrichedChr enriched) {
        if (enriched == null || enriched.chrEvent() == null || isBlank(enriched.chrEvent().getImsi())) {
            return "";
        }
        return enriched.chrEvent().getImsi().toString();
    }

    private static boolean isBlank(Object value) {
        return value == null || value.toString().trim().isEmpty();
    }

    private static String value(Object value) {
        return value == null ? null : value.toString();
    }

    public static class DimensionState implements Serializable {
        int consecutiveBadCount;
        long firstBadTs;
        boolean active;

        public DimensionState() {
        }
    }
}

package com.fdb.job.anomaly;

import com.fdb.common.avro.AnomalyEvent;

public final class AnomalyEventFactory {
    private AnomalyEventFactory() {}

    public static AnomalyEvent fromEvaluation(AnomalyRuleEvaluation evaluation) {
        return AnomalyEvent.newBuilder()
            .setDetectionTs(System.currentTimeMillis())
            .setEventTs(evaluation.eventTs())
            .setSourceEventTsAvg(evaluation.sourceEventTsAvg())
            .setSourceEventTsMin(evaluation.sourceEventTsMin())
            .setSourceEventTsMax(evaluation.sourceEventTsMax())
            .setSourceEventCount(evaluation.sourceEventCount())
            .setEntityType(evaluation.entityType())
            .setEntityId(evaluation.entityId())
            .setWindowStartTs(evaluation.windowStartTs())
            .setWindowEndTs(evaluation.windowEndTs())
            .setImsi(evaluation.imsi())
            .setSiteId(evaluation.siteId())
            .setCellId(evaluation.cellId())
            .setGridId(evaluation.gridId())
            .setLatitude(evaluation.latitude())
            .setLongitude(evaluation.longitude())
            .setAnomalyType(evaluation.anomalyType())
            .setSeverity(evaluation.severity())
            .setRuleVersion(evaluation.ruleVersion())
            .setContextJson(evaluation.contextJson())
            .build();
    }
}

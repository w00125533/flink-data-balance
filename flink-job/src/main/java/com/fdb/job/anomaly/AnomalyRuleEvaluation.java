package com.fdb.job.anomaly;

import com.fdb.common.avro.AnomalyType;
import com.fdb.common.avro.EntityType;
import com.fdb.common.avro.Severity;
import java.io.Serializable;

public record AnomalyRuleEvaluation(
    EntityType entityType,
    String entityId,
    String ruleDimension,
    boolean abnormal,
    long windowStartTs,
    long windowEndTs,
    long eventTs,
    String siteId,
    String cellId,
    String imsi,
    String gridId,
    Double latitude,
    Double longitude,
    AnomalyType anomalyType,
    Severity severity,
    String ruleVersion,
    String metricName,
    double threshold,
    double observedValue,
    String contextJson
) implements Serializable {
    public String key() {
        return entityType + "|" + entityId + "|" + ruleDimension;
    }
}

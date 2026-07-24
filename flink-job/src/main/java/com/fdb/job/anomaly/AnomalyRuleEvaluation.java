package com.fdb.job.anomaly;

import com.fdb.common.avro.AnomalyType;
import com.fdb.common.avro.EntityType;
import com.fdb.common.avro.Severity;
import java.io.Serializable;

public class AnomalyRuleEvaluation implements Serializable {
    private EntityType entityType;
    private String entityId;
    private String ruleDimension;
    private boolean abnormal;
    private long windowStartTs;
    private long windowEndTs;
    private long eventTs;
    private long sourceEventTsAvg;
    private long sourceEventTsMin;
    private long sourceEventTsMax;
    private long sourceEventCount;
    private String siteId;
    private String cellId;
    private String imsi;
    private String gridId;
    private Double latitude;
    private Double longitude;
    private AnomalyType anomalyType;
    private Severity severity;
    private String ruleVersion;
    private String metricName;
    private double threshold;
    private double observedValue;
    private String contextJson;

    public AnomalyRuleEvaluation() {}

    public AnomalyRuleEvaluation(
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
        String contextJson) {
        this(
            entityType,
            entityId,
            ruleDimension,
            abnormal,
            windowStartTs,
            windowEndTs,
            eventTs,
            siteId,
            cellId,
            imsi,
            gridId,
            latitude,
            longitude,
            anomalyType,
            severity,
            ruleVersion,
            metricName,
            threshold,
            observedValue,
            contextJson,
            0L,
            0L,
            0L,
            0L);
    }

    public AnomalyRuleEvaluation(
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
        String contextJson,
        long sourceEventTsAvg,
        long sourceEventTsMin,
        long sourceEventTsMax,
        long sourceEventCount) {
        this.entityType = entityType;
        this.entityId = entityId;
        this.ruleDimension = ruleDimension;
        this.abnormal = abnormal;
        this.windowStartTs = windowStartTs;
        this.windowEndTs = windowEndTs;
        this.eventTs = eventTs;
        this.sourceEventTsAvg = sourceEventTsAvg;
        this.sourceEventTsMin = sourceEventTsMin;
        this.sourceEventTsMax = sourceEventTsMax;
        this.sourceEventCount = sourceEventCount;
        this.siteId = siteId;
        this.cellId = cellId;
        this.imsi = imsi;
        this.gridId = gridId;
        this.latitude = latitude;
        this.longitude = longitude;
        this.anomalyType = anomalyType;
        this.severity = severity;
        this.ruleVersion = ruleVersion;
        this.metricName = metricName;
        this.threshold = threshold;
        this.observedValue = observedValue;
        this.contextJson = contextJson;
    }

    public String key() {
        return entityType + "|" + entityId + "|" + ruleDimension;
    }

    public EntityType entityType() { return entityType; }

    public String entityId() { return entityId; }

    public String ruleDimension() { return ruleDimension; }

    public boolean abnormal() { return abnormal; }

    public long windowStartTs() { return windowStartTs; }

    public long windowEndTs() { return windowEndTs; }

    public long eventTs() { return eventTs; }

    public long sourceEventTsAvg() { return sourceEventTsAvg; }

    public long sourceEventTsMin() { return sourceEventTsMin; }

    public long sourceEventTsMax() { return sourceEventTsMax; }

    public long sourceEventCount() { return sourceEventCount; }

    public String siteId() { return siteId; }

    public String cellId() { return cellId; }

    public String imsi() { return imsi; }

    public String gridId() { return gridId; }

    public Double latitude() { return latitude; }

    public Double longitude() { return longitude; }

    public AnomalyType anomalyType() { return anomalyType; }

    public Severity severity() { return severity; }

    public String ruleVersion() { return ruleVersion; }

    public String metricName() { return metricName; }

    public double threshold() { return threshold; }

    public double observedValue() { return observedValue; }

    public String contextJson() { return contextJson; }
}

package com.fdb.observability.model;

public record AnomalyResultRow(
    Long detectionTs,
    Long eventTs,
    String siteId,
    String cellId,
    String gridId,
    String anomalyType,
    String severity,
    String contextJson,
    Double latitude,
    Double longitude,
    String ruleVersion
) {
}

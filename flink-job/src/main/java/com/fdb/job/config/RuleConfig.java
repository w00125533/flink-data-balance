package com.fdb.job.config;

import java.io.Serializable;

public record RuleConfig(
    float rsrpThreshold,
    float sinrThreshold,
    int attachFailBurstThreshold,
    int coverageHoleThreshold,
    int cellConsecutiveMinutes,
    float cellRsrpMin,
    float cellSinrMin,
    float cellAttachSuccessMin,
    float cellHoSuccessMin,
    float cellDropRateMax,
    int userConsecutiveEvents,
    int userWindowMinutes,
    float userRsrpMin,
    float userSinrMin,
    float userLatencyMsMax,
    String ruleVersion
) implements Serializable {
    public static RuleConfig defaults() {
        return new RuleConfig(
            -110f, -3f, 10, 50,
            3, -110f, -3f, 0.95f, 0.90f, 0.05f,
            3, 10, -110f, -3f, 500f, "v1.0");
    }
}

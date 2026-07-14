package com.fdb.job.config;

import java.io.Serializable;

public record RuleConfig(
    float rsrpThreshold,
    float sinrThreshold,
    int attachFailBurstThreshold,
    int coverageHoleThreshold
) implements Serializable {
    public static RuleConfig defaults() {
        return new RuleConfig(-110f, -3f, 10, 50);
    }
}

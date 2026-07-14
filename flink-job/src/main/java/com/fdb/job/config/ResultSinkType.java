package com.fdb.job.config;

import java.util.Locale;

public enum ResultSinkType {
    STARROCKS("starrocks", false),
    ICEBERG("iceberg", true),
    HIVE("hive", true),
    KAFKA("kafka", false),
    NONE("none", false);

    private final String configValue;
    private final boolean fileBased;

    ResultSinkType(String configValue, boolean fileBased) {
        this.configValue = configValue;
        this.fileBased = fileBased;
    }

    public String configValue() {
        return configValue;
    }

    public boolean fileBased() {
        return fileBased;
    }

    public static ResultSinkType fromConfig(String configured) {
        if (configured == null || configured.isBlank()) {
            return STARROCKS;
        }
        String normalized = configured.trim().toLowerCase(Locale.ROOT);
        for (ResultSinkType sinkType : values()) {
            if (sinkType.configValue.equals(normalized)) {
                return sinkType;
            }
        }
        return STARROCKS;
    }
}

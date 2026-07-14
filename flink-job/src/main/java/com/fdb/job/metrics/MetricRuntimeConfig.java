package com.fdb.job.metrics;

import com.fdb.job.config.ResultSinkConfig;

import java.io.Serializable;
import java.util.Map;
import java.util.Properties;

public record MetricRuntimeConfig(
    String runId,
    String resultSink,
    int parallelism,
    boolean metricsEnabled
) implements Serializable {
    private static final int DEFAULT_PARALLELISM = 4;

    public MetricRuntimeConfig {
        runId = blankToDefault(runId, "unknown-run");
        resultSink = blankToDefault(resultSink, "");
        parallelism = parallelism > 0 ? parallelism : DEFAULT_PARALLELISM;
    }

    public static MetricRuntimeConfig from(ResultSinkConfig config, int parallelism) {
        return new MetricRuntimeConfig(
            config.runId(),
            config.resultSink().configValue(),
            parallelism,
            config.metricsEnabled());
    }

    public static MetricRuntimeConfig fromEnvironment() {
        Map<String, String> env = System.getenv();
        Properties properties = System.getProperties();
        return from(ResultSinkConfig.resolve(env, properties), resolveParallelism(env, properties));
    }

    static boolean metricsEnabled(Map<String, String> env, Properties properties) {
        return resolveBoolean(env, properties, "FDB_METRICS_ENABLED", "fdb.metrics.enabled", true);
    }

    private static int resolveParallelism(Map<String, String> env, Properties properties) {
        String configured = env.get("FDB_FLINK_PARALLELISM");
        if (configured == null || configured.isBlank()) {
            configured = properties.getProperty("fdb.flink.parallelism");
        }
        if (configured == null || configured.isBlank()) {
            return DEFAULT_PARALLELISM;
        }
        try {
            int parsed = Integer.parseInt(configured.trim());
            return parsed > 0 ? parsed : DEFAULT_PARALLELISM;
        } catch (NumberFormatException e) {
            return DEFAULT_PARALLELISM;
        }
    }

    private static boolean resolveBoolean(Map<String, String> env, Properties properties, String envName,
                                          String propertyName, boolean defaultValue) {
        String value = env.get(envName);
        if (value == null || value.isBlank()) {
            value = properties.getProperty(propertyName);
        }
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        String normalized = value.trim();
        if ("true".equalsIgnoreCase(normalized) || "1".equals(normalized)
            || "yes".equalsIgnoreCase(normalized) || "on".equalsIgnoreCase(normalized)) {
            return true;
        }
        if ("false".equalsIgnoreCase(normalized) || "0".equals(normalized)
            || "no".equalsIgnoreCase(normalized) || "off".equalsIgnoreCase(normalized)) {
            return false;
        }
        return defaultValue;
    }

    private static String blankToDefault(String value, String defaultValue) {
        return value == null || value.isBlank() ? defaultValue : value;
    }
}

package com.fdb.job.config;

import java.time.Clock;
import java.util.Map;
import java.util.Properties;

public record ResultSinkConfig(
    ResultSinkType resultSink,
    boolean dlqEnabled,
    boolean metricsEnabled,
    boolean metricsHistoryEnabled,
    long metricsEmitIntervalMs,
    boolean reportOnStop,
    String runId,
    String runLabel
) {
    private static final long DEFAULT_METRICS_EMIT_INTERVAL_MS = 5_000L;
    private static final long DEFAULT_CHECKPOINT_INTERVAL_MS = 30_000L;
    private static final long FILE_SINK_MAX_CHECKPOINT_INTERVAL_MS = 180_000L;

    public static ResultSinkConfig resolve(Map<String, String> env, Properties properties) {
        return new ResultSinkConfig(
            ResultSinkType.fromConfig(resolveString(env, properties, "FDB_RESULT_SINK", "fdb.result.sink",
                ResultSinkType.STARROCKS.configValue())),
            resolveBoolean(env, properties, "FDB_DLQ_ENABLED", "fdb.dlq.enabled", true),
            resolveBoolean(env, properties, "FDB_METRICS_ENABLED", "fdb.metrics.enabled", true),
            resolveBoolean(env, properties, "FDB_METRICS_HISTORY_ENABLED", "fdb.metrics.history.enabled", true),
            resolvePositiveLong(env, properties, "FDB_METRICS_EMIT_INTERVAL_MS", "fdb.metrics.emit.interval.ms",
                DEFAULT_METRICS_EMIT_INTERVAL_MS),
            resolveBoolean(env, properties, "FDB_REPORT_ON_STOP", "fdb.report.on.stop", false),
            resolveString(env, properties, "FDB_RUN_ID", "fdb.run.id", defaultRunId()),
            resolveString(env, properties, "FDB_RUN_LABEL", "fdb.run.label", "")
        );
    }

    public static long effectiveCheckpointIntervalMs(ResultSinkType sinkType, long configuredIntervalMs) {
        if (configuredIntervalMs <= 0L) {
            return DEFAULT_CHECKPOINT_INTERVAL_MS;
        }
        if (sinkType != null && sinkType.fileBased()
            && configuredIntervalMs > FILE_SINK_MAX_CHECKPOINT_INTERVAL_MS) {
            return FILE_SINK_MAX_CHECKPOINT_INTERVAL_MS;
        }
        return configuredIntervalMs;
    }

    private static String defaultRunId() {
        return "run-" + Clock.systemUTC().millis();
    }

    private static String resolveString(Map<String, String> env, Properties properties, String envName,
                                        String propertyName, String defaultValue) {
        String value = env.get(envName);
        if (value == null || value.isBlank()) {
            value = properties.getProperty(propertyName);
        }
        return value == null || value.isBlank() ? defaultValue : value.trim();
    }

    private static boolean resolveBoolean(Map<String, String> env, Properties properties, String envName,
                                          String propertyName, boolean defaultValue) {
        String value = resolveString(env, properties, envName, propertyName, Boolean.toString(defaultValue));
        if ("true".equalsIgnoreCase(value) || "1".equals(value) || "yes".equalsIgnoreCase(value)
            || "on".equalsIgnoreCase(value)) {
            return true;
        }
        if ("false".equalsIgnoreCase(value) || "0".equals(value) || "no".equalsIgnoreCase(value)
            || "off".equalsIgnoreCase(value)) {
            return false;
        }
        return defaultValue;
    }

    private static long resolvePositiveLong(Map<String, String> env, Properties properties, String envName,
                                            String propertyName, long defaultValue) {
        String value = resolveString(env, properties, envName, propertyName, Long.toString(defaultValue));
        try {
            long parsed = Long.parseLong(value);
            return parsed > 0L ? parsed : defaultValue;
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }
}

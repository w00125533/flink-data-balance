package com.fdb.job.config;

import com.fdb.common.config.ConfigLoader;

import java.nio.file.Path;
import java.util.Map;
import java.util.Properties;

public final class JobConfig {
    private final ConfigLoader.Config config;

    private JobConfig(ConfigLoader.Config config) { this.config = config; }

    public static JobConfig load() throws Exception {
        String path = System.getProperty("config", "job-default.yaml");
        return new JobConfig(ConfigLoader.builder().defaultResource("job-default.yaml")
            .overlayFile(Path.of(path)).envPrefix("FDB_").build().load());
    }

    public RuleConfig rules() {
        return rulesFrom(config, System.getenv(), System.getProperties());
    }

    static RuleConfig rulesFrom(ConfigLoader.Config config, Map<String, String> env, Properties properties) {
        return new RuleConfig(
            (float) config.getDouble("rules.lowSignal.rsrpThreshold", -110),
            (float) config.getDouble("rules.lowSignal.sinrThreshold", -3),
            config.getInt("rules.attachFailureBurst.threshold", 10),
            config.getInt("rules.coverageHole.threshold", 50),
            intSetting(config, env, properties, "rules.cell.consecutiveMinutes",
                "FDB_ANOMALY_CELL_CONSECUTIVE_MINUTES", "fdb.anomaly.cell.consecutive.minutes", 3),
            floatSetting(config, env, properties, "rules.cell.rsrpMin",
                "FDB_ANOMALY_CELL_RSRP_MIN", "fdb.anomaly.cell.rsrp.min", -110f),
            floatSetting(config, env, properties, "rules.cell.sinrMin",
                "FDB_ANOMALY_CELL_SINR_MIN", "fdb.anomaly.cell.sinr.min", -3f),
            floatSetting(config, env, properties, "rules.cell.attachSuccessMin",
                "FDB_ANOMALY_CELL_ATTACH_SUCCESS_MIN", "fdb.anomaly.cell.attach.success.min", 0.95f),
            floatSetting(config, env, properties, "rules.cell.hoSuccessMin",
                "FDB_ANOMALY_CELL_HO_SUCCESS_MIN", "fdb.anomaly.cell.ho.success.min", 0.90f),
            floatSetting(config, env, properties, "rules.cell.dropRateMax",
                "FDB_ANOMALY_CELL_DROP_RATE_MAX", "fdb.anomaly.cell.drop.rate.max", 0.05f),
            intSetting(config, env, properties, "rules.user.consecutiveEvents",
                "FDB_ANOMALY_USER_CONSECUTIVE_EVENTS", "fdb.anomaly.user.consecutive.events", 3),
            intSetting(config, env, properties, "rules.user.windowMinutes",
                "FDB_ANOMALY_USER_WINDOW_MINUTES", "fdb.anomaly.user.window.minutes", 10),
            floatSetting(config, env, properties, "rules.user.rsrpMin",
                "FDB_ANOMALY_USER_RSRP_MIN", "fdb.anomaly.user.rsrp.min", -110f),
            floatSetting(config, env, properties, "rules.user.sinrMin",
                "FDB_ANOMALY_USER_SINR_MIN", "fdb.anomaly.user.sinr.min", -3f),
            floatSetting(config, env, properties, "rules.user.latencyMsMax",
                "FDB_ANOMALY_USER_LATENCY_MS_MAX", "fdb.anomaly.user.latency.ms.max", 500f),
            stringSetting(config, env, properties, "rules.version",
                "FDB_ANOMALY_RULE_VERSION", "fdb.anomaly.rule.version", "v1.0"));
    }

    private static int intSetting(ConfigLoader.Config config, Map<String, String> env, Properties properties,
                                  String yamlKey, String envKey, String propertyKey, int defaultValue) {
        String value = env.get(envKey);
        if (value == null || value.isBlank()) {
            value = properties.getProperty(propertyKey);
        }
        return value == null || value.isBlank()
            ? config.getInt(yamlKey, defaultValue)
            : Integer.parseInt(value.trim());
    }

    private static float floatSetting(ConfigLoader.Config config, Map<String, String> env, Properties properties,
                                      String yamlKey, String envKey, String propertyKey, float defaultValue) {
        String value = env.get(envKey);
        if (value == null || value.isBlank()) {
            value = properties.getProperty(propertyKey);
        }
        return value == null || value.isBlank()
            ? (float) config.getDouble(yamlKey, defaultValue)
            : Float.parseFloat(value.trim());
    }

    private static String stringSetting(ConfigLoader.Config config, Map<String, String> env, Properties properties,
                                        String yamlKey, String envKey, String propertyKey, String defaultValue) {
        String value = env.get(envKey);
        if (value == null || value.isBlank()) {
            value = properties.getProperty(propertyKey);
        }
        if (value != null && !value.isBlank()) {
            return value.trim();
        }
        String configured = config.getStringOrNull(yamlKey);
        return configured == null || configured.isBlank() ? defaultValue : configured.trim();
    }
}

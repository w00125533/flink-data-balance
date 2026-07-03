package com.fdb.job;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

public record IcebergConfig(
    boolean enabled,
    String warehouse,
    String catalogName,
    String database,
    String table) {

    private static final Logger log = LoggerFactory.getLogger(IcebergConfig.class);

    static IcebergConfig resolve(Map<String, String> env, Properties properties) {
        return new IcebergConfig(
            resolveBoolean(env, properties, "FDB_ICEBERG_ENABLED", "fdb.iceberg.enabled", true),
            resolveString(env, properties, "FDB_ICEBERG_WAREHOUSE", "fdb.iceberg.warehouse", "hdfs://namenode:8020/warehouse/iceberg"),
            resolveString(env, properties, "FDB_ICEBERG_CATALOG", "fdb.iceberg.catalog", "fdb_iceberg"),
            resolveString(env, properties, "FDB_ICEBERG_DATABASE", "fdb.iceberg.database", "fdb"),
            resolveString(env, properties, "FDB_ICEBERG_TABLE", "fdb.iceberg.table", "cell_kpi"));
    }

    private static String resolveString(
        Map<String, String> env,
        Properties properties,
        String envKey,
        String propertyKey,
        String defaultValue) {
        String value = env.get(envKey);
        if (value == null || value.isBlank()) {
            value = properties.getProperty(propertyKey);
        }
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        return value.trim();
    }

    private static boolean resolveBoolean(
        Map<String, String> env,
        Properties properties,
        String envKey,
        String propertyKey,
        boolean defaultValue) {
        String value = resolveString(env, properties, envKey, propertyKey, Boolean.toString(defaultValue));
        if ("true".equalsIgnoreCase(value) || "1".equals(value) || "yes".equalsIgnoreCase(value) || "on".equalsIgnoreCase(value)) {
            return true;
        }
        if ("false".equalsIgnoreCase(value) || "0".equals(value) || "no".equalsIgnoreCase(value) || "off".equalsIgnoreCase(value)) {
            return false;
        }
        log.warn("Invalid Iceberg boolean config {}='{}', falling back to {}", envKey, value, defaultValue);
        return defaultValue;
    }
}

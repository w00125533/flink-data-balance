package com.fdb.job.sink;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

public record IcebergConfig(
    boolean enabled,
    String warehouse,
    String catalogName,
    String database,
    String table,
    String metastoreUri,
    String cellAnomalyTable,
    String userAnomalyTable,
    String gridAnomalyTable) {

    private static final Logger log = LoggerFactory.getLogger(IcebergConfig.class);

    public IcebergConfig(
        boolean enabled,
        String warehouse,
        String catalogName,
        String database,
        String table,
        String metastoreUri) {
        this(enabled, warehouse, catalogName, database, table, metastoreUri,
            "cell_anomaly_events", "user_anomaly_events", "grid_anomaly_events");
    }

    public static IcebergConfig resolve(Map<String, String> env, Properties properties) {
        return new IcebergConfig(
            resolveBoolean(env, properties, "FDB_ICEBERG_ENABLED", "fdb.iceberg.enabled", true),
            resolveString(env, properties, "FDB_ICEBERG_WAREHOUSE", "fdb.iceberg.warehouse", "hdfs://namenode:8020/warehouse/iceberg"),
            resolveString(env, properties, "FDB_ICEBERG_CATALOG", "fdb.iceberg.catalog", "fdb_iceberg"),
            resolveString(env, properties, "FDB_ICEBERG_DATABASE", "fdb.iceberg.database", "iceberg_db"),
            resolveKpiTable(env, properties),
            resolveString(env, properties, "FDB_ICEBERG_METASTORE_URI", "fdb.iceberg.metastore.uri",
                "thrift://hive-metastore:9083"),
            resolveString(env, properties, "FDB_ICEBERG_CELL_ANOMALY_TABLE",
                "fdb.iceberg.cell.anomaly.table", "cell_anomaly_events"),
            resolveString(env, properties, "FDB_ICEBERG_USER_ANOMALY_TABLE",
                "fdb.iceberg.user.anomaly.table", "user_anomaly_events"),
            resolveString(env, properties, "FDB_ICEBERG_GRID_ANOMALY_TABLE",
                "fdb.iceberg.grid.anomaly.table", "grid_anomaly_events"));
    }

    private static String resolveKpiTable(Map<String, String> env, Properties properties) {
        String value = env.get("FDB_ICEBERG_KPI_TABLE");
        if (value == null || value.isBlank()) {
            value = env.get("FDB_ICEBERG_TABLE");
        }
        if (value == null || value.isBlank()) {
            value = properties.getProperty("fdb.iceberg.kpi.table");
        }
        if (value == null || value.isBlank()) {
            value = properties.getProperty("fdb.iceberg.table");
        }
        if (value == null || value.isBlank()) {
            return "cell_kpi";
        }
        return value.trim();
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

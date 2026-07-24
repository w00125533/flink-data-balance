CREATE DATABASE IF NOT EXISTS fdb;

CREATE EXTERNAL TABLE IF NOT EXISTS fdb.cell_kpi (
    window_start_ts BIGINT,
    window_end_ts BIGINT,
    source_event_ts_avg BIGINT,
    source_event_ts_min BIGINT,
    source_event_ts_max BIGINT,
    source_event_count BIGINT,
    site_id STRING,
    cell_id STRING,
    grid_id STRING,
    num_chr_events BIGINT,
    num_users BIGINT,
    rsrp_sample_count BIGINT,
    sinr_sample_count BIGINT,
    attach_attempts BIGINT,
    avg_rsrp FLOAT,
    avg_sinr FLOAT,
    avg_prb_usage_dl FLOAT,
    throughput_dl_mbps_avg FLOAT,
    drop_rate FLOAT,
    ho_success_rate FLOAT,
    attach_success_rate FLOAT,
    join_quality STRING
)
PARTITIONED BY (window_kind STRING, dt STRING, hour STRING)
STORED AS PARQUET
LOCATION 'hdfs://namenode:8020/warehouse/fdb/cell_kpi';

ALTER TABLE fdb.cell_kpi SET LOCATION 'hdfs://namenode:8020/warehouse/fdb/cell_kpi';

MSCK REPAIR TABLE fdb.cell_kpi;

CREATE EXTERNAL TABLE IF NOT EXISTS fdb.cell_anomaly_events (
    detection_ts BIGINT,
    event_ts BIGINT,
    source_event_ts_avg BIGINT,
    source_event_ts_min BIGINT,
    source_event_ts_max BIGINT,
    source_event_count BIGINT,
    entity_type STRING,
    entity_id STRING,
    window_start_ts BIGINT,
    window_end_ts BIGINT,
    imsi STRING,
    site_id STRING,
    cell_id STRING,
    grid_id STRING,
    latitude DOUBLE,
    longitude DOUBLE,
    anomaly_type STRING,
    severity STRING,
    rule_version STRING,
    context_json STRING
)
PARTITIONED BY (dt STRING, hour STRING)
STORED AS PARQUET
LOCATION 'hdfs://namenode:8020/warehouse/fdb/cell_anomaly_events';

CREATE EXTERNAL TABLE IF NOT EXISTS fdb.user_anomaly_events (
    detection_ts BIGINT,
    event_ts BIGINT,
    source_event_ts_avg BIGINT,
    source_event_ts_min BIGINT,
    source_event_ts_max BIGINT,
    source_event_count BIGINT,
    entity_type STRING,
    entity_id STRING,
    window_start_ts BIGINT,
    window_end_ts BIGINT,
    imsi STRING,
    site_id STRING,
    cell_id STRING,
    grid_id STRING,
    latitude DOUBLE,
    longitude DOUBLE,
    anomaly_type STRING,
    severity STRING,
    rule_version STRING,
    context_json STRING
)
PARTITIONED BY (dt STRING, hour STRING)
STORED AS PARQUET
LOCATION 'hdfs://namenode:8020/warehouse/fdb/user_anomaly_events';

CREATE EXTERNAL TABLE IF NOT EXISTS fdb.grid_anomaly_events (
    detection_ts BIGINT,
    event_ts BIGINT,
    source_event_ts_avg BIGINT,
    source_event_ts_min BIGINT,
    source_event_ts_max BIGINT,
    source_event_count BIGINT,
    entity_type STRING,
    entity_id STRING,
    window_start_ts BIGINT,
    window_end_ts BIGINT,
    imsi STRING,
    site_id STRING,
    cell_id STRING,
    grid_id STRING,
    latitude DOUBLE,
    longitude DOUBLE,
    anomaly_type STRING,
    severity STRING,
    rule_version STRING,
    context_json STRING
)
PARTITIONED BY (dt STRING, hour STRING)
STORED AS PARQUET
LOCATION 'hdfs://namenode:8020/warehouse/fdb/grid_anomaly_events';

MSCK REPAIR TABLE fdb.cell_anomaly_events;
MSCK REPAIR TABLE fdb.user_anomaly_events;
MSCK REPAIR TABLE fdb.grid_anomaly_events;

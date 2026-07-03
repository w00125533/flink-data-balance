CREATE DATABASE IF NOT EXISTS fdb;

CREATE EXTERNAL TABLE IF NOT EXISTS fdb.cell_kpi (
    window_start_ts BIGINT,
    window_end_ts BIGINT,
    site_id STRING,
    cell_id STRING,
    grid_id STRING,
    num_chr_events BIGINT,
    num_users BIGINT,
    avg_rsrp FLOAT,
    avg_sinr FLOAT,
    avg_prb_usage_dl FLOAT,
    throughput_dl_mbps_avg FLOAT,
    drop_rate FLOAT,
    ho_success_rate FLOAT,
    attach_success_rate FLOAT
)
PARTITIONED BY (window_kind STRING, dt STRING, hour STRING)
STORED AS PARQUET
LOCATION 'hdfs://namenode:8020/warehouse/fdb/cell_kpi';

MSCK REPAIR TABLE fdb.cell_kpi;

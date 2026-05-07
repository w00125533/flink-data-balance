-- Hive external table for Flink Data Balance cell_kpi_1m
-- Run in Hive CLI / Beeline:
--   ADD JAR /path/to/flink-job-0.1.0-SNAPSHOT.jar;
--   SOURCE docs/hive-schema.q;

-- The Parquet files are written by FileSink to:
--   ${FDB_HIVE_WAREHOUSE:-/user/hive/warehouse/fdb.db}/cell_kpi_1m/
-- Partition layout: dt=yyyy-MM-dd/hour=HH/*.parquet

ADD JAR parquet-hadoop-bundle.jar;

CREATE DATABASE IF NOT EXISTS fdb;

CREATE EXTERNAL TABLE IF NOT EXISTS fdb.cell_kpi_1m (
    window_start_ts       BIGINT,
    window_end_ts         BIGINT,
    window_kind           STRING,
    site_id               STRING,
    cell_id               STRING,
    grid_id               STRING,
    num_chr_events        BIGINT,
    num_users             BIGINT,
    avg_rsrp              FLOAT,
    avg_sinr              FLOAT,
    avg_prb_usage_dl      FLOAT,
    throughput_dl_mbps_avg FLOAT,
    drop_rate             FLOAT,
    ho_success_rate       FLOAT,
    attach_success_rate   FLOAT
)
PARTITIONED BY (dt STRING, hour STRING)
STORED AS PARQUET
LOCATION '/user/hive/warehouse/fdb.db/cell_kpi_1m'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- Discover partitions written by Flink
MSCK REPAIR TABLE fdb.cell_kpi_1m;

-- ── Example queries ──

-- Top cells by drop rate in the last hour
SELECT cell_id, site_id, drop_rate, num_chr_events
FROM fdb.cell_kpi_1m
WHERE dt = '2026-05-06'
  AND hour = '14'
ORDER BY drop_rate DESC
LIMIT 20;

-- Hourly average PRB usage for a site
SELECT hour, AVG(avg_prb_usage_dl) AS avg_prb
FROM fdb.cell_kpi_1m
WHERE site_id = 'SITE-001'
  AND dt >= '2026-05-01'
GROUP BY hour
ORDER BY hour;

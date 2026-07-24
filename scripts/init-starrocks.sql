CREATE DATABASE IF NOT EXISTS fdb;

USE fdb;

CREATE TABLE IF NOT EXISTS cell_anomaly_events (
  anomaly_id VARCHAR(128) NOT NULL,
  detection_ts BIGINT NOT NULL,
  event_ts BIGINT NOT NULL,
  source_event_ts_avg BIGINT NOT NULL DEFAULT "0",
  source_event_ts_min BIGINT NOT NULL DEFAULT "0",
  source_event_ts_max BIGINT NOT NULL DEFAULT "0",
  source_event_count BIGINT NOT NULL DEFAULT "0",
  entity_type VARCHAR(16) NOT NULL,
  entity_id VARCHAR(128) NOT NULL,
  window_start_ts BIGINT NOT NULL,
  window_end_ts BIGINT NOT NULL,
  imsi VARCHAR(32),
  site_id VARCHAR(64),
  cell_id VARCHAR(64),
  grid_id VARCHAR(32),
  latitude DOUBLE,
  longitude DOUBLE,
  anomaly_type VARCHAR(64) NOT NULL,
  severity VARCHAR(16) NOT NULL,
  rule_version VARCHAR(32) NOT NULL,
  context_json STRING
)
PRIMARY KEY(anomaly_id)
DISTRIBUTED BY HASH(anomaly_id) BUCKETS 8
PROPERTIES (
  "replication_num" = "1"
);

CREATE TABLE IF NOT EXISTS user_anomaly_events (
  anomaly_id VARCHAR(128) NOT NULL,
  detection_ts BIGINT NOT NULL,
  event_ts BIGINT NOT NULL,
  source_event_ts_avg BIGINT NOT NULL DEFAULT "0",
  source_event_ts_min BIGINT NOT NULL DEFAULT "0",
  source_event_ts_max BIGINT NOT NULL DEFAULT "0",
  source_event_count BIGINT NOT NULL DEFAULT "0",
  entity_type VARCHAR(16) NOT NULL,
  entity_id VARCHAR(128) NOT NULL,
  window_start_ts BIGINT NOT NULL,
  window_end_ts BIGINT NOT NULL,
  imsi VARCHAR(32),
  site_id VARCHAR(64),
  cell_id VARCHAR(64),
  grid_id VARCHAR(32),
  latitude DOUBLE,
  longitude DOUBLE,
  anomaly_type VARCHAR(64) NOT NULL,
  severity VARCHAR(16) NOT NULL,
  rule_version VARCHAR(32) NOT NULL,
  context_json STRING
)
PRIMARY KEY(anomaly_id)
DISTRIBUTED BY HASH(anomaly_id) BUCKETS 8
PROPERTIES (
  "replication_num" = "1"
);

CREATE TABLE IF NOT EXISTS grid_anomaly_events (
  anomaly_id VARCHAR(128) NOT NULL,
  detection_ts BIGINT NOT NULL,
  event_ts BIGINT NOT NULL,
  source_event_ts_avg BIGINT NOT NULL DEFAULT "0",
  source_event_ts_min BIGINT NOT NULL DEFAULT "0",
  source_event_ts_max BIGINT NOT NULL DEFAULT "0",
  source_event_count BIGINT NOT NULL DEFAULT "0",
  entity_type VARCHAR(16) NOT NULL,
  entity_id VARCHAR(128) NOT NULL,
  window_start_ts BIGINT NOT NULL,
  window_end_ts BIGINT NOT NULL,
  imsi VARCHAR(32),
  site_id VARCHAR(64),
  cell_id VARCHAR(64),
  grid_id VARCHAR(32),
  latitude DOUBLE,
  longitude DOUBLE,
  anomaly_type VARCHAR(64) NOT NULL,
  severity VARCHAR(16) NOT NULL,
  rule_version VARCHAR(32) NOT NULL,
  context_json STRING
)
PRIMARY KEY(anomaly_id)
DISTRIBUTED BY HASH(anomaly_id) BUCKETS 8
PROPERTIES (
  "replication_num" = "1"
);

CREATE TABLE IF NOT EXISTS cell_kpi (
  window_start_ts          BIGINT NOT NULL,
  window_kind              VARCHAR(8) NOT NULL,
  cell_id                  VARCHAR(64) NOT NULL,
  window_end_ts            BIGINT NOT NULL,
  source_event_ts_avg      BIGINT NOT NULL DEFAULT "0",
  source_event_ts_min      BIGINT NOT NULL DEFAULT "0",
  source_event_ts_max      BIGINT NOT NULL DEFAULT "0",
  source_event_count       BIGINT NOT NULL DEFAULT "0",
  join_quality             VARCHAR(16) NOT NULL DEFAULT 'JOINED',
  site_id                  VARCHAR(64) NOT NULL,
  grid_id                  VARCHAR(16) NOT NULL DEFAULT '',
  num_chr_events           BIGINT NOT NULL DEFAULT "0",
  num_users                BIGINT NOT NULL DEFAULT "0",
  rsrp_sample_count        BIGINT NOT NULL DEFAULT "0",
  sinr_sample_count        BIGINT NOT NULL DEFAULT "0",
  attach_attempts          BIGINT NOT NULL DEFAULT "0",
  avg_rsrp                 FLOAT NOT NULL DEFAULT "0",
  avg_sinr                 FLOAT NOT NULL DEFAULT "0",
  avg_prb_usage_dl         FLOAT NOT NULL DEFAULT "0",
  throughput_dl_mbps_avg   FLOAT NOT NULL DEFAULT "0",
  drop_rate                FLOAT NOT NULL DEFAULT "0",
  ho_success_rate          FLOAT NOT NULL DEFAULT "0",
  attach_success_rate      FLOAT NOT NULL DEFAULT "0"
)
PRIMARY KEY (window_start_ts, window_kind, cell_id)
DISTRIBUTED BY HASH(cell_id) BUCKETS 8
PROPERTIES (
  "replication_num" = "1"
);

CREATE VIEW IF NOT EXISTS kpi_1m AS
SELECT * FROM cell_kpi WHERE window_kind = 'MIN_1';

CREATE VIEW IF NOT EXISTS kpi_5m AS
SELECT * FROM cell_kpi WHERE window_kind = 'MIN_5';

CREATE EXTERNAL CATALOG IF NOT EXISTS fdb_iceberg
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hive",
  "hive.metastore.uris" = "thrift://hive-metastore:9083"
);

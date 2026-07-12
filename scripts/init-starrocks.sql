CREATE DATABASE IF NOT EXISTS fdb;

USE fdb;

CREATE TABLE IF NOT EXISTS anomaly_events (
  event_ts      BIGINT NOT NULL,
  imsi          VARCHAR(32) NOT NULL,
  anomaly_type  VARCHAR(32) NOT NULL,
  detection_ts  BIGINT NOT NULL,
  site_id       VARCHAR(64) NOT NULL,
  cell_id       VARCHAR(64) NOT NULL,
  grid_id       VARCHAR(16) NOT NULL DEFAULT '',
  latitude      DOUBLE NOT NULL,
  longitude     DOUBLE NOT NULL,
  severity      VARCHAR(8) NOT NULL,
  rule_version  VARCHAR(32) NOT NULL,
  context_json  STRING
)
PRIMARY KEY (event_ts, imsi, anomaly_type)
DISTRIBUTED BY HASH(imsi) BUCKETS 8
PROPERTIES (
  "replication_num" = "1"
);

CREATE TABLE IF NOT EXISTS cell_kpi (
  window_start_ts          BIGINT NOT NULL,
  window_kind              VARCHAR(8) NOT NULL,
  cell_id                  VARCHAR(64) NOT NULL,
  window_end_ts            BIGINT NOT NULL,
  site_id                  VARCHAR(64) NOT NULL,
  grid_id                  VARCHAR(16) NOT NULL DEFAULT '',
  num_chr_events           BIGINT NOT NULL DEFAULT "0",
  num_users                BIGINT NOT NULL DEFAULT "0",
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

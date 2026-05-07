-- Flink Data Balance — MySQL warehouse sink tables
-- Usage:
--   docker exec -i fdb-mysql mysql -ufdb -pfdbpwd fdb < scripts/init-mysql.sql

USE fdb;

DROP TABLE IF EXISTS anomaly_events;
CREATE TABLE anomaly_events (
  id            BIGINT AUTO_INCREMENT PRIMARY KEY,
  detection_ts  BIGINT       NOT NULL,
  event_ts      BIGINT       NOT NULL,
  imsi          VARCHAR(32)  NOT NULL,
  site_id       VARCHAR(64)  NOT NULL,
  cell_id       VARCHAR(64)  NOT NULL,
  grid_id       VARCHAR(16),
  latitude      DOUBLE,
  longitude     DOUBLE,
  anomaly_type  VARCHAR(32)  NOT NULL,
  severity      VARCHAR(8),
  rule_version  VARCHAR(32),
  context_json  TEXT,
  UNIQUE KEY uk_anomaly (detection_ts, site_id, cell_id, anomaly_type, imsi),
  KEY idx_event_ts (event_ts),
  KEY idx_cell    (cell_id, detection_ts)
) ENGINE=InnoDB CHARSET=utf8mb4;

DROP TABLE IF EXISTS cell_kpi;
CREATE TABLE cell_kpi (
  id                       BIGINT AUTO_INCREMENT PRIMARY KEY,
  window_start_ts          BIGINT       NOT NULL,
  window_end_ts            BIGINT       NOT NULL,
  window_kind              VARCHAR(8)   NOT NULL,
  site_id                  VARCHAR(64)  NOT NULL,
  cell_id                  VARCHAR(64)  NOT NULL,
  grid_id                  VARCHAR(16),
  num_chr_events           BIGINT,
  num_users                BIGINT,
  avg_rsrp                 FLOAT,
  avg_sinr                 FLOAT,
  avg_prb_usage_dl         FLOAT,
  throughput_dl_mbps_avg   FLOAT,
  drop_rate                FLOAT,
  ho_success_rate          FLOAT,
  attach_success_rate      FLOAT,
  UNIQUE KEY uk_kpi       (window_start_ts, window_kind, site_id, cell_id),
  KEY idx_cell_window     (cell_id, window_kind, window_start_ts)
) ENGINE=InnoDB CHARSET=utf8mb4;

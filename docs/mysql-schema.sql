-- Flink Data Balance — MySQL Schema
-- Run once: mysql -u root -p < docs/mysql-schema.sql

CREATE DATABASE IF NOT EXISTS fdb
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;

USE fdb;

-- ── Anomaly Events ──

CREATE TABLE IF NOT EXISTS anomaly_events (
    detection_ts    BIGINT       NOT NULL COMMENT 'Flink detection timestamp (epoch ms)',
    event_ts        BIGINT       NOT NULL COMMENT 'Original event timestamp (epoch ms)',
    imsi            VARCHAR(64)  NOT NULL,
    site_id         VARCHAR(64)  NOT NULL,
    cell_id         VARCHAR(64)  NOT NULL,
    grid_id         VARCHAR(16)  NOT NULL DEFAULT '',
    latitude        DOUBLE       NOT NULL,
    longitude       DOUBLE       NOT NULL,
    anomaly_type    VARCHAR(32)  NOT NULL,
    severity        VARCHAR(8)   NOT NULL,
    rule_version    VARCHAR(16)  NOT NULL DEFAULT '',
    context_json    TEXT,
    window_start    BIGINT       NOT NULL DEFAULT 0 COMMENT '5min boundary for partition pruning',
    PRIMARY KEY (detection_ts, imsi, anomaly_type),
    INDEX idx_site_id (site_id),
    INDEX idx_anomaly_type (anomaly_type),
    INDEX idx_window_start (window_start)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ── Cell KPI 1-minute ──

CREATE TABLE IF NOT EXISTS cell_kpi_1m (
    window_start_ts       BIGINT       NOT NULL,
    window_end_ts         BIGINT       NOT NULL,
    window_kind           VARCHAR(8)   NOT NULL DEFAULT 'MIN_1',
    site_id               VARCHAR(64)  NOT NULL,
    cell_id               VARCHAR(64)  NOT NULL,
    grid_id               VARCHAR(16)  NOT NULL DEFAULT '',
    num_chr_events        BIGINT       NOT NULL DEFAULT 0,
    num_users             BIGINT       NOT NULL DEFAULT 0,
    avg_rsrp              FLOAT        NOT NULL DEFAULT 0,
    avg_sinr              FLOAT        NOT NULL DEFAULT 0,
    avg_prb_usage_dl      FLOAT        NOT NULL DEFAULT 0,
    throughput_dl_mbps_avg FLOAT       NOT NULL DEFAULT 0,
    drop_rate             FLOAT        NOT NULL DEFAULT 0,
    ho_success_rate       FLOAT        NOT NULL DEFAULT 0,
    attach_success_rate   FLOAT        NOT NULL DEFAULT 0,
    PRIMARY KEY (window_start_ts, cell_id),
    INDEX idx_site_id (site_id),
    INDEX idx_window_kind (window_kind)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

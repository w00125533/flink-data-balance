# Flink 数据均衡处理工程 - 设计文档

- **创建日期**: 2026-04-29
- **最近刷新**: 2026-07-09
- **状态**: 待评审
- **关键技术栈**: Java 21 · Flink 1.20 · Maven · Avro · Kafka · StarRocks · Hive HMS · Iceberg · React 18 · TypeScript · Vite · Ant Design · AntV X6 · Prometheus

---

## 1. 目标与非目标

### 1.1 目标

构建一个 Flink 工程，处理基站小区产生的用户级 CHR 数据，并结合 **PM 话统数据** 与 **CFG 配置数据** 做实时分析。核心交付：

1. **三个数据源的模拟器**：CHR、PM、CFG，合并为单 jar 多模式运行。
2. **拓扑发布服务**：发布站点/小区拓扑，模拟器订阅后生成确定性数据。
3. **Flink 主作业**：
   - CHR 与 PM 先分别按 `cellId + minuteTs` 做 1 分钟增量汇聚。
   - 以 `cellId + minuteTs` 做 Full JOIN，最多等待迟到 2 分钟。
   - 产出 1 分钟小区 KPI，5 分钟 KPI 从 1 分钟 KPI rollup。
   - 产出小区级异常与栅格级异常。
4. **动态均衡可选**：默认关闭；关闭时 Flink DAG 不创建动态均衡相关算子。
5. **统一查询入口**：KPI 主持久化在 Iceberg，StarRocks 通过外部目录/视图查询 KPI；异常直接写入 StarRocks 内表。
6. **湖表保留**：Hive Parquet 与 Iceberg 保留为湖表落地和本地对比路径，查询通过 StarRocks 层统一进入。
7. **实时观测控制台**：展示流处理状态、KPI 结果、异常结果、栅格异常、每次 sink 写入耗时与存储健康状态。
8. **数据老化治理**：业务流和结果数据按 1 小时、10GB 上限治理；compact 配置类 topic 保留最新配置。
9. **多目标部署入口**：通过统一 `scripts/deploy.sh <target> <command>` 管理本地 Docker 调试和外部 YARN 部署；本地复用 `../shared-data-infra`，外部环境通过 `.env` 连接已部署的 Kafka、HDFS、Hive、StarRocks、YARN 等基础设施。

### 1.2 非目标

- 不实现完整 5G/6G 协议栈，字段语义贴近真实电信场景但不强制工业标准。
- 不引入 Confluent Schema Registry；schema 通过 jar 包同步。
- 不提供平台级 K8s 部署清单、自动扩缩容策略或生产发布流水线。
- 不负责安装外部 Hadoop/YARN/Hive/Kafka/StarRocks 集群；外部模式只负责连通性检查、项目资源初始化、Flink 提交与停止。
- 不实现 ML 模型类异常检测；当前只做规则集，模型能力作为扩展。
- 不在第一版实现 StarRocks 异步物化视图；先通过 external catalog / view 查询 Iceberg 与 Hive。
- 不提供生产级告警分派、用户权限、审计登录。

### 1.3 关键设计原则

- **统一业务 key**：动态均衡、CHR、PM、CFG 关联均围绕 `cellId`，保证同一小区数据尽量就近处理。
- **PM 语义统一**：话统数据统一命名为 PM，topic、schema、类名、算子、指标、文档和前端都使用 PM。
- **动态均衡按需启用**：默认不启用，减少 DAG 复杂度和本地资源消耗；需要压测倾斜时再打开。
- **结果可解释**：Full JOIN 输出质量标记，明确区分 `JOINED`、`CHR_ONLY`、`PM_ONLY`。
- **查询不复制 KPI 主数据**：KPI 以 Iceberg 为主表，StarRocks 负责统一查询和加速入口。
- **观测面与数据面分离**：业务结果、sink 耗时、Flink 指标分别采集，前端统一呈现。
- **部署目标显式化**：local 与 external-yarn 共享生命周期命令，但不共享执行假设；local 使用 Docker Compose 和 shared-data-infra 容器，external-yarn 使用 Linux CLI 和外部 endpoint。

---

## 2. 整体架构

### 2.1 默认链路：动态均衡关闭

```
topology-service
      |
      v
Kafka topology / chr-events / pm-stats / cfg-config
      |
      v
Flink job
  - CHR source -> keyBy(cellId) -> CHR 1m fact
  - PM  source -> keyBy(cellId) -> PM  1m fact
  - CFG source -> keyBy(cellId) -> latest CFG state
  - CHR 1m fact + PM 1m fact + CFG
      -> Full JOIN by cellId + minuteTs, wait 2 minutes
      -> CellKpi MIN_1
      -> CellKpi MIN_5 rollup
  - Enriched CHR / KPI context
      -> cell anomaly detector
      -> grid anomaly detector

Outputs
  - KPI 1m / 5m -> Kafka result topics -> Iceberg table -> StarRocks external view
  - KPI 1m / 5m -> Hive Parquet path
  - anomalies   -> StarRocks internal tables
  - sink metrics -> fdb-stage-metrics / Prometheus
```

默认链路中不创建动态均衡相关算子。Flink Web UI 和观测控制台都只展示实际存在的 source、aggregation、join、anomaly、sink 和 observability 节点。

### 2.2 可选链路：动态均衡开启

通过环境变量启用：

```text
FDB_DYNAMIC_BALANCING_ENABLED=true
```

开启后增加如下链路：

```
CHR/PM/CFG envelope
  -> routing-assigner(cellId based)
  -> keyBy(vbucketId)
  -> vbucket-load-meter
  -> business pipeline

vbucket-load-meter side output
  -> lb-heartbeat
  -> load-coordinator
  -> lb-routing
  -> routing-assigner broadcast state
```

动态均衡也以 `cellId` 作为路由输入。`slotShift(cellId)` 默认 0；Coordinator 下发路由调整后改变对应小区的虚拟分片落点。关闭动态均衡时，这些算子和 topic 消费链完全不创建：

```text
lb-routing-source
routing-assigner
vbucket-load-meter
lb-heartbeat-source
load-coordinator
lb-routing-sink
lb-heartbeat-sink
```

### 2.3 模块划分

```
flink-data-balance/
├── common/                  # Avro schema、POJO、geo、serde、metric model
├── topology-service/        # 拓扑发布服务
├── simulator/               # CHR/PM/CFG 三模式模拟器
├── flink-job/               # Flink 主作业
├── observability-api/       # 观测与结果查询 API
├── frontend/                # React 观测控制台
├── docker/                  # 项目侧 compose，仅保留本工程服务
├── scripts/                 # 统一部署入口、DDL、维护、冒烟脚本
└── docs/                    # 设计文档与表结构
```

---

## 3. 数据模型

### 3.1 ChrEvent

CHR 是用户级事件主流。关键字段：

| 字段 | 说明 |
|---|---|
| `chrId` | 事件 UUID |
| `eventTs` | 事件时间，epoch ms |
| `imsi` / `imei` | 用户与设备标识 |
| `siteId` / `cellId` | 站点与小区 |
| `eventType` | ATTACH、HANDOVER、DATA_SESSION 等 |
| `pci` / `tac` / `eci` | 无线与核心网标识 |
| `rsrp` / `rsrq` / `sinr` | 无线信号 |
| `latitude` / `longitude` | 用户位置 |

### 3.2 PmStat

PM 是小区级话统/性能测量数据，topic 为 `pm-stats`。默认每小区每 10 秒一条，后续先聚合到分钟级。

| 字段 | 说明 |
|---|---|
| `siteId` / `cellId` | 站点与小区 |
| `windowStartTs` / `windowEndTs` | 原始 PM 统计窗口 |
| `prbUsageDl` / `prbUsageUl` | PRB 使用率 |
| `activeUsers` | 活跃用户数 |
| `avgRsrp` / `avgRsrq` / `avgSinr` | 小区平均无线质量 |
| `throughputDlMbps` / `throughputUlMbps` | 吞吐 |
| `droppedConnections` | 掉线数 |
| `handoverSuccess` / `handoverFailure` | 切换成功/失败 |
| `rrcEstabAttempt` / `rrcEstabSuccess` | RRC 建立 |

### 3.3 CfgConfig

CFG 是小区配置数据，topic 为 `cfg-config`，compact 清理策略。首次加载和后续增量更新都按 `cellId` 分区进入同一 keyed state。

| 字段 | 说明 |
|---|---|
| `siteId` / `cellId` | 站点与小区 |
| `effectiveTs` | 生效时间 |
| `version` | 单调递增版本 |
| `cellType` | LTE / NR_NSA / NR_SA |
| `bandwidthMhz` | 带宽 |
| `arfcn` / `frequencyBand` | 频点 |
| `centerLat` / `centerLon` | 小区中心 |
| `coverageRadiusM` | 覆盖半径 |
| `pci` / `tac` / `eci` | 配置侧标识 |
| `tombstone` | 软删除标记 |

### 3.4 Minute Fact

CHR 和 PM 分别先形成分钟事实。

`ChrMinuteFact`：

| 字段 | 说明 |
|---|---|
| `cellId` | 小区 |
| `minuteTs` | 分钟起始时间 |
| `siteId` | 站点 |
| `chrCount` | CHR 事件数 |
| `uniqueUsers` | 唯一用户估算或精确集合 |
| `avgRsrp` / `avgSinr` | CHR 侧无线质量 |
| `attachAttempts` / `attachSuccess` | 接入统计 |

`PmMinuteFact`：

| 字段 | 说明 |
|---|---|
| `cellId` | 小区 |
| `minuteTs` | 分钟起始时间 |
| `pmWindowCount` | 参与汇聚的 PM 原始窗口数 |
| `avgPrbUsageDl` | 平均下行 PRB |
| `throughputDlMbpsAvg` | 平均下行吞吐 |
| `dropCount` | 掉线数 |
| `handoverSuccess` / `handoverFailure` | 切换统计 |

### 3.5 CellKpi

KPI 由 CHR/PM 分钟事实 Full JOIN 后生成。

| 字段 | 说明 |
|---|---|
| `windowStartTs` / `windowEndTs` | KPI 窗口 |
| `windowKind` | `MIN_1` / `MIN_5` |
| `joinQuality` | `JOINED` / `CHR_ONLY` / `PM_ONLY` |
| `siteId` / `cellId` / `gridId` | 空间维度 |
| `numChrEvents` | CHR 事件数 |
| `numUsers` | 用户数 |
| `avgRsrp` / `avgSinr` | 信号质量 |
| `avgPrbUsageDl` | PRB 使用率 |
| `throughputDlMbpsAvg` | 吞吐 |
| `dropRate` | 掉线率 |
| `hoSuccessRate` | 切换成功率 |
| `attachSuccessRate` | 接入成功率 |

### 3.6 AnomalyEvent

异常拆成小区级与栅格级查询模型，Flink 内部可共用 Avro 基础结构。

| 字段 | 说明 |
|---|---|
| `detectionTs` | 检测时间 |
| `eventTs` | 触发事件时间 |
| `siteId` / `cellId` | 小区维度 |
| `gridId` | geohash |
| `latitude` / `longitude` | 展示坐标 |
| `anomalyType` | LOW_SIGNAL、CONFIG_MISMATCH、COVERAGE_HOLE 等 |
| `severity` | LOW / MEDIUM / HIGH |
| `ruleVersion` | 规则版本 |
| `contextJson` | CHR、PM、CFG 快照 |

---

## 4. Kafka Topic

| Topic | 分区 | 清理 | Key | 用途 |
|---|---:|---|---|---|
| `chr-events` | 64 | delete 1h | cellId | CHR 主流 |
| `pm-stats` | 16 | delete 1h | cellId | PM 原始话统 |
| `cfg-config` | 8 | compact | cellId | CFG 最新配置 |
| `topology` | 4 | compact | siteId | 拓扑发布 |
| `cell-kpi-1m` | 8 | delete 1h | cellId | 1 分钟 KPI 输出 |
| `cell-kpi-5m` | 8 | delete 1h | cellId | 5 分钟 KPI 输出 |
| `cell-anomaly-events` | 16 | delete 1h | cellId | 小区异常 |
| `grid-anomaly-events` | 16 | delete 1h | gridId | 栅格异常 |
| `chr-dlq` / `pm-dlq` / `cfg-dlq` / `enrichment-late` | 各 4 | delete 1h | 原业务 key | 死信 / 迟到 |
| `fdb-stage-metrics` | 4 | delete 1h | stageId | 阶段与 sink 指标 |
| `lb-heartbeat` | 1 | delete 1h | subtaskId | 动态均衡心跳，仅启用动态均衡时需要 |
| `lb-routing` | 1 | compact | cellId | 动态均衡路由，仅启用动态均衡时需要 |

对 delete topic 设置：

```text
retention.ms=3600000
retention.bytes=10737418240
```

compact 配置类 topic 不做 1 小时删除，保留最新配置语义。

---

## 5. Flink 作业设计

### 5.1 时间语义

- CHR 使用 event time，watermark 允许乱序 20 秒。
- PM 使用 `windowEndTs` 作为 event time，允许乱序 2 分钟。
- CFG 使用 processing time 更新 keyed state，同时保留 `effectiveTs/version` 做版本判断。
- KPI Full JOIN 等待窗口为 2 分钟：窗口结束后最多等待迟到 CHR/PM 2 分钟，到期输出最终结果。

### 5.2 CFG 加载

CFG 首次加载直接从 `cfg-config` compact topic earliest 读取，按 `cellId` keyBy 到对应 task。

```
cfg-config
  -> cfg-source
  -> keyBy(cellId)
  -> latestCfgState[cellId]
```

后续增量配置仍走同一条链路。更新规则：

1. `version` 更大时更新。
2. `version` 相同但 `effectiveTs` 更新时更新。
3. `tombstone=true` 时删除该小区 CFG state。
4. CFG 缺失不阻塞 KPI 输出，但 `joinQuality` 和 `contextJson` 需体现配置缺失。

### 5.3 PM/CHR 分钟级汇聚

CHR 分钟事实：

```
chr-events
  -> chr-source
  -> keyBy(cellId)
  -> event-time 1m aggregate
  -> ChrMinuteFact
```

PM 分钟事实：

```
pm-stats
  -> pm-source
  -> keyBy(cellId)
  -> event-time 1m aggregate
  -> PmMinuteFact
```

增量聚合使用 accumulator，避免在窗口触发时扫描完整原始事件集合。唯一用户数第一版可以使用 `HashSet`，后续可替换为 HLL。

### 5.4 Full JOIN

```
ChrMinuteFact
  + PmMinuteFact
  + latest CFG
  -> keyBy(cellId + minuteTs)
  -> wait 2 minutes
  -> CellKpi MIN_1
```

输出规则：

| 到达情况 | 输出 |
|---|---|
| CHR 与 PM 都到达 | `JOINED` |
| 只有 CHR | `CHR_ONLY`，PM 指标填 0 或空值 |
| 只有 PM | `PM_ONLY`，CHR 指标填 0 或空值 |

Full JOIN 不因为单侧缺失丢弃整分钟数据。控制台可按 `joinQuality` 过滤，帮助定位数据源迟到或缺口。

### 5.5 5 分钟 KPI rollup

5 分钟 KPI 从 1 分钟 KPI rollup：

```
CellKpi MIN_1
  -> keyBy(cellId)
  -> event-time 5m rollup
  -> CellKpi MIN_5
```

这样避免原始 CHR/PM 被重复扫描，且 1 分钟与 5 分钟口径一致。

### 5.6 异常检测

小区级异常：

```
CellKpi MIN_1 / enriched CHR context
  -> keyBy(cellId)
  -> CellAnomalyDetector
  -> cell-anomaly-events
  -> StarRocks internal table
```

栅格级异常：

```
Enriched location / CellKpi
  -> keyBy(gridId)
  -> GridAnomalyDetector
  -> grid-anomaly-events
  -> StarRocks internal table
```

v1 规则：

| 规则 | 维度 | 说明 |
|---|---|---|
| LOW_SIGNAL | cellId | RSRP/SINR 连续低于阈值 |
| ATTACH_FAILURE_BURST | cellId | 接入失败突增 |
| HANDOVER_FAIL_PATTERN | cellId | 切换失败率异常 |
| CONFIG_MISMATCH | cellId | CHR 上报配置与 CFG 不一致 |
| COVERAGE_HOLE | gridId | 栅格内低信号事件聚集 |

### 5.7 动态均衡实现

默认关闭：

```text
FDB_DYNAMIC_BALANCING_ENABLED=false
```

关闭时直接按 `cellId` 分区，不创建动态均衡算子。开启时使用 `cellId` 计算虚拟分片：

```text
vbucketId = hash(cellId, slotShift(cellId)) mod FDB_VBUCKET_COUNT
```

Coordinator 只在动态均衡开启时运行。控制台根据运行时 DAG 和配置决定是否显示负载均衡页面。

### 5.8 并行度与算子链

默认本地配置：

```text
FDB_FLINK_PARALLELISM=4
taskmanager.numberOfTaskSlots=4
```

KPI window 与重 sink 拆链。观测阶段使用 SinkLatencyProbe stage ID，实际 Flink sink name 仍保留在 sinkTo 节点上：

- `kpi-1m`
- `kpi-5m-rollup`
- `kafka-kpi-1m` -> actual sink `cell-kpi-kafka-sink`
- `starrocks-kpi-1m` -> actual sink `cell-kpi-jdbc-sink`
- `hive-kpi-1m` -> actual sink `cell-kpi-hive-sink`
- `iceberg-kpi-1m` -> actual sink `cell-kpi-iceberg-sink`
- `kafka-kpi-5m` -> actual sink `cell-kpi-5m-kafka-sink`
- `starrocks-kpi-5m` -> actual sink `cell-kpi-5m-jdbc-sink`
- `hive-kpi-5m` -> actual sink `cell-kpi-5m-hive-sink`
- `iceberg-kpi-5m` -> actual sink `cell-kpi-5m-iceberg-sink`
- `kafka-cell-anomaly` -> actual sink `cell-anomaly-kafka-sink`
- `kafka-grid-anomaly` -> actual sink `grid-anomaly-kafka-sink`
- `starrocks-cell-anomaly` -> actual sink `cell-anomaly-starrocks-sink`
- `starrocks-grid-anomaly` -> actual sink `grid-anomaly-starrocks-sink`

通过 `startNewChain` / `disableChaining` 或显式算子边界避免 UI 上把窗口和多个 sink 合并为一个 vertex，方便定位 busy、backpressure 和 sink 延迟。

---

## 6. Sink 与存储

### 6.1 KPI 主持久化：Iceberg

1 分钟和 5 分钟 KPI 继续写入 Iceberg `fdb.cell_kpi`。表中通过 `window_kind` 区分 `MIN_1` 与 `MIN_5`。

默认配置：

```text
FDB_ICEBERG_ENABLED=true
FDB_ICEBERG_WAREHOUSE=hdfs://namenode:8020/warehouse/iceberg
FDB_ICEBERG_CATALOG=fdb_iceberg
FDB_ICEBERG_DATABASE=iceberg_db
FDB_ICEBERG_TABLE=cell_kpi
```

Iceberg 分区：

```text
window_kind, dt, hour
```

其中 `dt/hour` 由 `window_start_ts` 派生。

### 6.2 Hive Parquet

Hive Parquet 保留为湖表输出路径：

```text
hdfs://namenode:8020/warehouse/fdb/cell_kpi/window_kind=<kind>/dt=<yyyy-MM-dd>/hour=<HH>/*.parquet
```

Hive 主要用于本地验证和湖表对比。控制台查询不直接连 Hive，而是通过 StarRocks 外部目录或外表统一查询。

### 6.3 StarRocks 查询层

StarRocks 由 `../shared-data-infra` 的 `starrocks` profile 提供，本工程不重复定义 FE/BE 容器。

StarRocks 职责：

1. 提供控制台统一查询入口。
2. 通过 Iceberg external catalog / view 查询 KPI。
3. 通过 Hive external catalog / view 查询 Parquet 湖表对比结果。
4. 存储异常内表，支撑在线查询。

第一版不创建异步物化视图。若 KPI 查询慢，再新增 StarRocks async materialized view 或 API 缓存。

### 6.4 StarRocks 内表

小区异常内表：

```sql
CREATE TABLE cell_anomaly_events (
  detection_ts BIGINT NOT NULL,
  event_ts BIGINT NOT NULL,
  site_id VARCHAR(64),
  cell_id VARCHAR(64) NOT NULL,
  grid_id VARCHAR(16),
  anomaly_type VARCHAR(64) NOT NULL,
  severity VARCHAR(16) NOT NULL,
  rule_version VARCHAR(32),
  context_json STRING
)
DUPLICATE KEY(detection_ts, cell_id, anomaly_type)
PARTITION BY RANGE(detection_ts) ()
DISTRIBUTED BY HASH(cell_id) BUCKETS 16;
```

栅格异常内表：

```sql
CREATE TABLE grid_anomaly_events (
  detection_ts BIGINT NOT NULL,
  event_ts BIGINT NOT NULL,
  grid_id VARCHAR(16) NOT NULL,
  latitude DOUBLE,
  longitude DOUBLE,
  anomaly_type VARCHAR(64) NOT NULL,
  severity VARCHAR(16) NOT NULL,
  rule_version VARCHAR(32),
  context_json STRING
)
DUPLICATE KEY(detection_ts, grid_id, anomaly_type)
PARTITION BY RANGE(detection_ts) ()
DISTRIBUTED BY HASH(grid_id) BUCKETS 16;
```

分区由维护脚本按小时创建与删除。

### 6.5 Sink 耗时指标

每个 sink 分支都记录每次写入耗时。

字段：

| 字段 | 说明 |
|---|---|
| `sinkName` | 具体 sink 名称 |
| `sinkType` | kafka / starrocks / hive / iceberg |
| `dataset` | kpi_1m / kpi_5m / cell_anomaly / grid_anomaly |
| `records` | 本次写入记录数 |
| `bytes` | 估算写入字节数 |
| `startTs` / `endTs` | 写入开始与结束 |
| `durationMs` | 写入耗时 |
| `success` | 是否成功 |
| `errorMessage` | 失败原因 |
| `checkpointId` | 可选，Iceberg/Hive 可关联 checkpoint |

采集路径：

```
SinkLatencyProbe
  -> fdb-stage-metrics
  -> Observability API
  -> 控制台 Sink 耗时页

Flink metrics
  -> Prometheus
  -> 指标面板
```

Iceberg 额外展示 checkpoint commit 和 snapshot commit 耗时，因为数据可见性依赖 commit 完成。

---

## 7. 查询 API 与控制台

### 7.1 API

| API | 说明 |
|---|---|
| `GET /api/results/kpi/1m` | 查询 1 分钟 KPI |
| `GET /api/results/kpi/5m` | 查询 5 分钟 KPI |
| `GET /api/results/anomalies/cell` | 查询小区级异常 |
| `GET /api/results/anomalies/grid` | 查询栅格级异常 |
| `GET /api/results/sink-latency` | 查询 sink 耗时统计 |
| `GET /api/flow/stages` | 查询 Flink 阶段状态 |
| `GET /api/events/stream` | SSE 推送阶段、sink、异常摘要 |

KPI API 通过 StarRocks 查询 Iceberg view。异常 API 查询 StarRocks 内表。

### 7.2 前端页面

采用“业务结果优先”的布局：

```text
流处理总览
KPI 1m
KPI 5m
小区异常
栅格异常
Sink 耗时
执行历史
指标面板
```

`KPI 1m` / `KPI 5m`：

- 时间范围筛选。
- `siteId` / `cellId` / `joinQuality` 筛选。
- 结果表格。
- KPI 趋势小图。
- 侧栏展示相关 sink 最近耗时、p95、失败数。

`小区异常`：

- 时间、`siteId`、`cellId`、`severity`、`anomalyType` 筛选。
- 异常明细表。
- 最近异常趋势。

`栅格异常`：

- 表格为必交付能力。
- GIS 简单呈现为增强能力：根据 geohash 还原中心点或边界，展示散点或栅格色块。
- 如果 GIS 依赖或性能复杂，第一版可降级为表格 + 经纬度/geohash 列，不阻塞主功能。

`Sink 耗时`：

- 按 `sinkType + dataset + windowKind` 展示最近一次耗时。
- 展示 p50 / p95 / p99、失败数、记录数、字节数。
- Iceberg 展示 checkpoint/snapshot commit 耗时。

### 7.3 流程图显示规则

控制台流程图只显示实际创建的 DAG 节点。

动态均衡关闭时，不显示：

```text
lb-routing-source
routing-assigner
vbucket-load-meter
lb-heartbeat-source
load-coordinator
负载迁移页
```

动态均衡开启时，显示动态均衡闭环和迁移统计。

---

## 8. 数据老化与容量上限

统一目标：

```text
retention.time=1h
retention.bytes=10GB
```

覆盖范围：

- CHR、PM、KPI、异常、DLQ、指标等 delete topic。
- StarRocks 异常内表。
- Hive Parquet 数据。
- Iceberg KPI 数据与快照。
- 本地运行历史与 sink summary 文件。

排除范围：

- `topology`
- `cfg-config`
- 其他 compact 配置类 topic

### 8.1 Kafka

delete topic 设置：

```text
retention.ms=3600000
retention.bytes=10737418240
cleanup.policy=delete
```

compact topic 保持：

```text
cleanup.policy=compact
```

### 8.2 StarRocks

异常内表按小时分区。维护脚本定时：

1. 创建未来 2 小时分区。
2. 删除早于 1 小时的分区。
3. 检查表数据量，超过 10GB 时优先删除最老分区。

### 8.3 Hive Parquet

按 `window_kind/dt/hour` 删除过期目录。需要在删除后刷新元数据。

### 8.4 Iceberg

维护任务：

1. 删除早于 1 小时的 KPI 数据文件。
2. 过期旧 snapshot。
3. 删除 orphan files。
4. 统计表目录大小，超过 10GB 时触发更激进的过期策略。

第一版可以由脚本手动或定时执行；后续接入调度器。

---

## 9. 配置

### 9.1 Flink

| 配置 | 默认值 | 说明 |
|---|---|---|
| `FDB_FLINK_PARALLELISM` | `4` | Flink 作业默认并发 |
| `taskmanager.numberOfTaskSlots` | `4` | 本地 TaskManager slots |
| `FDB_FLINK_CHECKPOINT_INTERVAL_MS` | `60000` | checkpoint 间隔 |
| `FDB_DYNAMIC_BALANCING_ENABLED` | `false` | 是否启用动态均衡 |
| `FDB_VBUCKET_COUNT` | `1024` | 动态均衡启用时的虚拟分片数 |
| `FDB_JOIN_ALLOWED_LATENESS_MS` | `120000` | CHR/PM Full JOIN 等待时间 |

### 9.2 Kafka topic

| 配置 | 默认值 |
|---|---|
| `FDB_CHR_TOPIC` | `chr-events` |
| `FDB_PM_TOPIC` | `pm-stats` |
| `FDB_CFG_TOPIC` | `cfg-config` |
| `FDB_KPI_1M_TOPIC` | `cell-kpi-1m` |
| `FDB_KPI_5M_TOPIC` | `cell-kpi-5m` |
| `FDB_CELL_ANOMALY_TOPIC` | `cell-anomaly-events` |
| `FDB_GRID_ANOMALY_TOPIC` | `grid-anomaly-events` |

### 9.3 StarRocks

| 配置 | 默认值 |
|---|---|
| `FDB_STARROCKS_FE_ENDPOINT` | `starrocks-fe:9030` |
| `FDB_STARROCKS_USER` | `root` |
| `FDB_STARROCKS_PASSWORD` | 空 |
| `FDB_STARROCKS_DATABASE` | `fdb` |

StarRocks FE/BE 来自 `../shared-data-infra`，本工程只接入 external network。

### 9.4 Iceberg

| 配置 | 默认值 |
|---|---|
| `FDB_ICEBERG_ENABLED` | `true` |
| `FDB_ICEBERG_WAREHOUSE` | `hdfs://namenode:8020/warehouse/iceberg` |
| `FDB_ICEBERG_CATALOG` | `fdb_iceberg` |
| `FDB_ICEBERG_DATABASE` | `iceberg_db` |
| `FDB_ICEBERG_TABLE` | `cell_kpi` |

### 9.5 部署目标

部署入口统一为：

```bash
FDB_ENV_FILE=.env.local bash scripts/deploy.sh local check
FDB_ENV_FILE=.env.local bash scripts/deploy.sh local up
FDB_ENV_FILE=.env.local bash scripts/deploy.sh local init
FDB_ENV_FILE=.env.local bash scripts/deploy.sh local submit
FDB_ENV_FILE=.env.local bash scripts/deploy.sh local stop
FDB_ENV_FILE=.env.local bash scripts/deploy.sh local smoke
FDB_ENV_FILE=.env.local bash scripts/deploy.sh local down

FDB_ENV_FILE=.env.external bash scripts/deploy.sh external-yarn check
FDB_ENV_FILE=.env.external bash scripts/deploy.sh external-yarn check --strict
FDB_ENV_FILE=.env.external bash scripts/deploy.sh external-yarn init
FDB_ENV_FILE=.env.external bash scripts/deploy.sh external-yarn submit
FDB_ENV_FILE=.env.external bash scripts/deploy.sh external-yarn stop
FDB_ENV_FILE=.env.external bash scripts/deploy.sh external-yarn smoke
```

`local` 目标用于开发机 Docker 调试；`external-yarn` 目标用于无 Docker 的 Linux 部署机，向外部 YARN 提交 Flink 作业。

| 配置 | local 示例 | external-yarn 示例 | 说明 |
|---|---|---|---|
| `FDB_DEPLOY_TARGET` | `local` | `external-yarn` | 防误用的目标声明 |
| `SHARED_INFRA_DIR` | `../shared-data-infra` | 空 | local 复用共享 Docker 基础设施 |
| `FDB_KAFKA_BOOTSTRAP` | `kafka:9092` | `kafka01:9092,kafka02:9092` | Flink 与服务端连接 Kafka |
| `FDB_KAFKA_HOST_BOOTSTRAP` | `localhost:9092` | 同外部 broker | 宿主机或 CLI 连接 Kafka |
| `FDB_HDFS_URI` | `hdfs://namenode:8020` | `hdfs://nameservice1` | HDFS 入口 |
| `FDB_HIVE_JDBC_URL` | `jdbc:hive2://localhost:10000/default` | `jdbc:hive2://hive01:10000/default` | HiveServer2 JDBC |
| `FDB_HIVE_METASTORE_URI` | `thrift://hive-metastore:9083` | `thrift://hms01:9083` | Hive Metastore |
| `FLINK_HOME` | 空或 Docker 内置 | `/opt/flink-1.20.3` | external-yarn 必填 |
| `HADOOP_CONF_DIR` | 空或容器内置 | `/etc/hadoop/conf` | external-yarn 必填 |
| `YARN_CONF_DIR` | 空或同 Hadoop | `/etc/hadoop/conf` | external-yarn 必填 |
| `FDB_FLINK_YARN_QUEUE` | 空 | `default` | 外部 YARN 队列 |
| `FDB_FLINK_CHECKPOINT_DIR` | `file:///tmp/fdb-checkpoints` | `hdfs://nameservice1/flink-data-balance/checkpoints` | checkpoint 目录 |
| `FDB_STARROCKS_FE_ENDPOINT` | `starrocks-fe:9030` | `starrocks-fe01:9030` | StarRocks FE |

配置文件建议分为：

```text
.env.example.local
.env.example.external-yarn
```

二者差异集中在 runtime、endpoint、客户端 CLI、初始化执行位置、停止和验证方式。业务 topic 名、Hive/Iceberg 表名、数据库名尽量共用默认值。

当前 local `.env` 示例暴露脚本和作业实际支持的本地开关；StarRocks、Prometheus 与 shared Hive 等基础设施端点来自 `../shared-data-infra`，项目侧 compose 只管理 Flink runtime、observability-api 与 frontend。external-yarn 则通过 `.env.external` 显式配置外部 endpoint 与 CLI 路径。

---

## 10. 部署模式与共享基础设施

新增或修改 Docker Compose 基础设施前必须先检查 `../shared-data-infra`。

### 10.1 local

local 目标继续复用共享能力：

| 能力 | 来源 |
|---|---|
| Kafka / ZooKeeper | `../shared-data-infra` streaming profile |
| HDFS / Hive HMS / HiveServer2 | `../shared-data-infra` lakehouse profile |
| StarRocks FE/BE | `../shared-data-infra` starrocks profile |
| Kafka UI | `../shared-data-infra` observability / streaming tooling |

local 目标中本工程保留：

| 服务 | 说明 |
|---|---|
| Flink JobManager / TaskManager | 项目作业运行时 |
| observability-api | 结果查询与指标聚合 |
| frontend | 控制台 |
| prometheus | shared-data-infra 提供 scrape 与指标查询 |

local 生命周期：

| 命令 | 说明 |
|---|---|
| `deploy.sh local check` | 检查 Docker、shared-data-infra network、compose 配置 |
| `deploy.sh local up` | 启动项目侧 Flink runtime、observability-api、frontend |
| `deploy.sh local init` | 创建 Kafka topics、StarRocks 表、HDFS 目录、Hive 表，准备 Flink Hadoop runtime jar |
| `deploy.sh local submit` | 向本地 Docker Flink JobManager 提交作业 |
| `deploy.sh local stop` | 取消本地 Flink 作业 |
| `deploy.sh local smoke` | 执行本地端到端冒烟验证 |
| `deploy.sh local down` | 停止项目侧本地容器 |

修改 compose 后至少运行：

```bash
docker compose -f docker/docker-compose.yml --profile e2e config
```

### 10.2 external-yarn

external-yarn 目标用于没有 Docker 的 Linux 部署机。外部 Kafka、HDFS、Hive、StarRocks、YARN 由平台或外部环境提供，本工程不负责启动或销毁这些基础设施。

部署机要求：

| 依赖 | 说明 |
|---|---|
| JDK 17 | 构建与运行 Java 组件 |
| Maven 3.9+ | 构建项目 jar |
| Flink 客户端 | `FLINK_HOME` 指向 Flink 1.20.x 安装目录 |
| Hadoop/YARN 客户端 | 提供 `hdfs`、`yarn`，并配置 `HADOOP_CONF_DIR` / `YARN_CONF_DIR` |
| Hive beeline | 执行 Hive DDL 与连通性检查 |
| Kafka CLI | 执行 topic 创建与 Kafka 连通性检查 |
| StarRocks/MySQL client | 初始化 StarRocks 数据库、表或 external catalog |

external-yarn 生命周期：

| 命令 | 说明 |
|---|---|
| `deploy.sh external-yarn check` | 诊断 CLI 与 Kafka/HDFS/Hive/YARN/StarRocks 连通性；默认不作为硬门禁 |
| `deploy.sh external-yarn check --strict` | 同上，但任一失败返回非零 |
| `deploy.sh external-yarn init` | 幂等创建 Kafka topics、HDFS warehouse/checkpoint 目录、Hive/StarRocks DDL |
| `deploy.sh external-yarn submit` | 构建 jar，并通过 `$FLINK_HOME/bin/flink` 提交到 YARN |
| `deploy.sh external-yarn stop` | 根据记录的 Flink job id 或 YARN application id 停止作业 |
| `deploy.sh external-yarn smoke` | 外部小流量验证；首版可渐进增强，当前不作为开发机硬门禁 |

外部提交由部署人员显式执行 `deploy.sh external-yarn submit`。`init` 不自动提交长跑作业，避免在外部资源准备阶段误启动 Flink。

提交和停止状态记录到：

```text
logs/external-yarn-current.env
```

其中保存 Flink job id、YARN application id、提交时间和使用的 env 文件路径，供 `stop` 和排障使用。

---

## 11. 容错与一致性

| 组件 | 语义 | 说明 |
|---|---|---|
| Kafka Source | at-least-once | checkpoint 记录 offset |
| CHR/PM minute aggregate | exactly-once state | 依赖 Flink checkpoint |
| Full JOIN | exactly-once state | 到期输出最终结果 |
| Kafka result sink | at-least-once | Avro topic，可重放 |
| StarRocks anomaly sink | at-least-once + 幂等 key | 内表按业务 key 去重或容忍重复 |
| Hive sink | checkpoint rolling | 文件可见性受 checkpoint 影响 |
| Iceberg sink | exactly-once append | checkpoint 触发 commit |
| Sink metrics | at-least-once | 指标允许重复，API 聚合时按时间窗口处理 |

DLQ：

- `chr-dlq`
- `pm-dlq`
- `cfg-dlq`
- `enrichment-late`

---

## 12. 测试策略

| 类型 | 覆盖 |
|---|---|
| Unit | PM schema、CFG state 更新、分钟 accumulator、Full JOIN 到期逻辑 |
| Unit | `JOINED/CHR_ONLY/PM_ONLY` 输出质量 |
| Unit | 5 分钟 rollup 从 1 分钟 KPI 聚合 |
| Unit | 动态均衡开关关闭时不构建相关 DAG 分支 |
| Unit | SinkLatencyProbe 统计 duration、records、bytes、failure |
| Integration | Embedded Kafka + Flink MiniCluster 小流量端到端 |
| Integration | StarRocks DDL 与异常写入 |
| Integration | Iceberg data files 与 snapshot commit 可见 |
| E2E | 共享 Kafka/Hive/HDFS/StarRocks + 项目 Flink + 控制台 API |
| Deploy | `deploy.sh local` 生命周期命令覆盖本地 Docker 调试 |
| Deploy | `deploy.sh external-yarn` 生命周期命令覆盖外部 Linux/YARN 部署 |

提交前约束：

- 仅改 compose、脚本、文档时不需要 Java 符号 impact analysis。
- 修改 Java 函数、类或方法前必须运行 GitNexus impact analysis。
- 提交前运行 GitNexus detect_changes。
- 修改部署脚本后至少运行 `bash -n scripts/deploy.sh`。
- 修改 Docker Compose 后必须运行 `docker compose -f docker/docker-compose.yml --profile e2e config`。
- 当前没有外部集群时，不要求 `deploy.sh external-yarn check` 在开发机通过；外部环境就绪后用 `deploy.sh external-yarn check --strict` 作为部署门禁。

---

## 13. 验收标准

- [ ] 全文和代码命名使用 PM，不出现旧话统缩写。
- [ ] `pm-stats` topic 正常创建并消费。
- [ ] `cfg-config` 首次加载和增量更新都按 `cellId` 进入 keyed state。
- [ ] 默认 `FDB_DYNAMIC_BALANCING_ENABLED=false` 时，Flink DAG 不出现动态均衡相关算子。
- [ ] 开启动态均衡时，按 `cellId` 计算 vbucket，并显示动态均衡页面。
- [ ] CHR 与 PM 先生成 1 分钟事实，再按 `cellId + minuteTs` Full JOIN。
- [ ] PM 或 CHR 单侧缺失时仍输出 `CHR_ONLY` 或 `PM_ONLY`。
- [ ] 5 分钟 KPI 从 1 分钟 KPI rollup。
- [ ] 1 分钟和 5 分钟 KPI 写入 Iceberg，并可通过 StarRocks 查询。
- [ ] 小区级异常和栅格级异常写入 StarRocks 内表。
- [ ] 控制台提供 KPI 1m、KPI 5m、小区异常、栅格异常、Sink 耗时页面。
- [ ] 栅格异常至少有表格展示；GIS 展示可用时显示地图或栅格。
- [ ] 每个 sink 分支记录最近一次耗时、p50/p95/p99、记录数、字节数、失败数。
- [ ] Iceberg 展示 checkpoint/snapshot commit 耗时。
- [ ] Kafka、StarRocks、Hive、Iceberg 数据按 1 小时和 10GB 上限治理。
- [ ] Flink 默认并发与 TaskManager slots 为 4。
- [ ] KPI window 与重 sink 在 Flink UI 中拆成可辨识 vertex。
- [ ] `../shared-data-infra` 能提供 Kafka/ZooKeeper/Hive/HDFS/StarRocks，本工程不重复定义这些基础设施。
- [x] 本地入口统一为 `scripts/deploy.sh local <command>`，覆盖 `check/up/init/submit/stop/smoke/down`。
- [x] 外部入口统一为 `scripts/deploy.sh external-yarn <command>`，覆盖 `check/init/submit/stop/smoke`。
- [x] 旧的 `dev-up.sh`、`dev-down.sh`、`e2e-smoke-test.sh` 入口不再作为目标态入口；README 示例统一迁移到 `deploy.sh`。
- [x] `local up/down` 只管理本工程本地 Docker runtime，不重复启动 shared-data-infra 中已有的 Kafka/HDFS/Hive/StarRocks 服务。
- [x] `external-yarn init` 幂等创建项目所需 Kafka topics、HDFS 目录、Hive/StarRocks DDL，但不自动提交 Flink 作业。
- [x] `external-yarn submit` 由部署人员显式执行，使用 `FLINK_HOME`、`HADOOP_CONF_DIR`、`YARN_CONF_DIR` 和 `.env` 中的外部 endpoint 提交到 YARN。
- [x] `external-yarn check` 默认诊断模式不作为开发机硬门禁；`--strict` 模式在外部部署机上任一失败返回非零。

---

## 14. 后续扩展

1. StarRocks async materialized view：当 Iceberg 外部查询慢时再引入。
2. KPI 查询缓存：观测 API 对热门时间范围做短 TTL 缓存。
3. 高精度 GIS：栅格边界、多级 geohash drilldown、热力图。
4. ML 异常检测：与规则集并行输出。
5. 生产调度：将老化脚本接入调度器。
6. 更多部署目标：K8s、独立 Flink 集群和对象存储。

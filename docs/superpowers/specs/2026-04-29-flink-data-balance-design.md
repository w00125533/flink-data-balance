# Flink 数据均衡处理工程 - 设计文档

- **创建日期**: 2026-04-29
- **最近刷新**: 2026-07-15
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
   - 小区级异常在 1 分钟 KPI 后做规则状态检测，用户级异常在 enrich 后做规则状态检测，栅格级覆盖空洞保留 geohash 检测。
4. **动态均衡可选**：默认关闭；关闭时 Flink DAG 不创建动态均衡相关算子。
5. **单类型业务结果 Sink**：通过 `FDB_RESULT_SINK=starrocks|iceberg|hive|kafka|none` 控制本次运行全部业务结果只写一种 sink，便于压测不同存储的上限。
6. **统一业务结果口径**：KPI 1m、KPI 5m、小区异常、用户异常、栅格异常均纳入同一个 result sink 开关；Hive/Iceberg 模式下异常结果也落成对应表。
7. **实时观测控制台**：展示当前 run、实际启用的 DAG、KPI 结果、三类实体异常结果、每次 sink 写入耗时、瓶颈候选与压测报告入口。
8. **数据老化治理**：业务流和结果数据按 1 小时、10GB 上限治理；compact 配置类 topic 保留最新配置。
9. **多目标部署入口**：通过统一 `scripts/deploy.sh <target> <command>` 管理本地 Docker 调试和外部 YARN 部署；本地复用 `../shared-data-infra`，外部环境通过 `.env` 连接已部署的 Kafka、HDFS、Hive、StarRocks、YARN 等基础设施。
10. **压测报告**：每次压测以 `benchmarkId/runId` 归档运行配置、Flink 快照、业务 metrics、存储探测和 HTML 报告，用于对比不同 sink 压力档位下的稳定上限与瓶颈。

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
- **业务结果 Sink 单选**：业务结果落地不再并行写 Iceberg、StarRocks、Hive、Kafka；每次运行只构建一种业务 sink 分支。
- **观测面与数据面分离**：业务结果 sink 与观测 metrics 分离；metrics 走 Kafka 轻量采样和 API 本地历史文件，不污染被测 result sink。
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
  - CHR source -> enrich with CFG/PM context
      -> user event anomaly detect-dedup
      -> coverage-hole detector by geohash
      -> CHR 1m fact
  - PM  source -> keyBy(cellId) -> PM  1m fact
  - CFG source -> keyBy(cellId) -> latest CFG state
  - CHR 1m fact + PM 1m fact + CFG
      -> Full JOIN by cellId + minuteTs, wait 2 minutes
      -> CellKpi MIN_1
      -> cell KPI anomaly detect-dedup
      -> CellKpi MIN_5 rollup

Outputs
  - KPI 1m / 5m -> selected result sink only
  - cell/user/grid anomalies -> selected result sink only
  - sink metrics -> fdb-stage-metrics -> Observability API memory + local JSONL history
  - benchmark report -> benchmark-runner/output/benchmark-runs/<benchmarkId>/index.html
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
│   └── src/main/java/com/fdb/job/
│       ├── config/          # 作业配置、规则配置、result sink 开关
│       ├── source/          # Kafka source / Avro serde
│       ├── model/           # Flink 作业内 envelope 与 minute fact
│       ├── enrich/          # CHR/PM/CFG 富化
│       ├── kpi/             # CHR/PM 预聚合、分钟拼接、5 分钟 rollup
│       ├── anomaly/         # 小区 KPI、用户事件、栅格覆盖空洞规则检测
│       ├── balance/         # vbucket 路由与负载均衡
│       ├── sink/            # ResultSinks、StarRocks/Hive/Iceberg/Kafka sink
│       ├── metrics/         # StageMetricsProbe、SinkLatencyProbe、metrics publisher
│       └── maintenance/     # Iceberg/存储老化维护工具
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
| `sourceEventTsAvg` / `sourceEventTsMin` / `sourceEventTsMax` | 参与该 KPI 的原始 CHR/PM 事件时间统计；用于累计延迟统计，不使用窗口结束时间替代 |
| `sourceEventCount` | 参与 `sourceEventTs*` 统计的原始事件数量 |
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

异常拆成 `CELL`、`USER`、`GRID` 三类实体查询模型，Flink 内部共用实体化 Avro 基础结构。字段不再强制把用户专属字段或坐标字段压到小区异常上。

| 字段 | 说明 |
|---|---|
| `detectionTs` | 检测时间 |
| `eventTs` | 触发事件时间；聚合类异常使用窗口结束时间 |
| `sourceEventTsAvg` / `sourceEventTsMin` / `sourceEventTsMax` | 触发异常的原始事件时间统计；用户/栅格事件来自 CHR，小区聚合异常来自 `CellKpi` |
| `sourceEventCount` | 参与 `sourceEventTs*` 统计的原始事件数量 |
| `entityType` | `CELL` / `USER` / `GRID` |
| `entityId` | 小区异常为 `cellId`，用户异常为 `imsi`，栅格异常为 `gridId` 或 geohash |
| `windowStartTs` / `windowEndTs` | 检测窗口起止时间 |
| `siteId` / `cellId` | 已知时填充；用户和栅格异常可作为上下文 |
| `imsi` | 仅用户异常必填，非用户异常为空 |
| `gridId` | 栅格异常必填，非栅格异常仅在已有上下文时填充 |
| `latitude` / `longitude` | 仅在有坐标上下文时填充；小区异常不依赖经纬度 |
| `anomalyType` | `CELL_RADIO_BAD`、`CELL_SERVICE_BAD`、`USER_FAILURE`、`USER_QOE_BAD`、`COVERAGE_HOLE` |
| `severity` | LOW / MEDIUM / HIGH |
| `ruleVersion` | 规则版本 |
| `contextJson` | 规则维度、门限、观测值、窗口时间和必要上下文 |

活动异常类型：

| 类型 | 实体 | 来源 | 含义 |
|---|---|---|---|
| `CELL_RADIO_BAD` | `CELL` | `CellKpi MIN_1` detect-dedup | 小区连续 1 分钟无线质量劣化，如 RSRP/SINR 低于门限 |
| `CELL_SERVICE_BAD` | `CELL` | `CellKpi MIN_1` detect-dedup | 小区连续 1 分钟业务质量劣化，如接入、切换、掉话率不满足门限 |
| `USER_FAILURE` | `USER` | enriched CHR detect-dedup | 同一用户在 10 分钟内同一规则维度连续失败 |
| `USER_QOE_BAD` | `USER` | enriched CHR detect-dedup | 同一用户在 10 分钟内连续体验劣化 |
| `COVERAGE_HOLE` | `GRID` | geohash/grid detector | 栅格内低信号事件聚集 |

旧事件级小区异常类型 `LOW_SIGNAL`、`ATTACH_FAILURE_BURST`、`HANDOVER_FAIL_PATTERN`、`CONFIG_MISMATCH` 可以保留在 schema/枚举中用于兼容，但新 DAG 不再主动产出这些类型。

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
| `user-anomaly-events` | 16 | delete 1h | imsi | 用户异常 |
| `grid-anomaly-events` | 16 | delete 1h | gridId | 栅格异常 |
| `chr-dlq` / `pm-dlq` / `cfg-dlq` | 各 4 | delete 1h | 原业务 key | 无法继续处理的死信 |
| `enrichment-late` | 4 | delete 1h | 原业务 key | 富化上下文迟到或缺失，主流继续处理 |
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
4. CFG 缺失不阻塞 CHR 主流、用户异常检测或 KPI 输出；富化输出 `EnrichedChr(chr, null, latestPm)`，并把 CFG 缺失上下文写入 `enrichment-late` 侧通道。
5. CFG 缺失需在 `joinQuality`、`contextJson` 或轻量指标中体现，但不能当作业务 DLQ 记录处理。
6. Enrichment 里的 PM 上下文只保留 `latestPmState[cellId]`，不再维护 6 条 PM ring。
   该状态仅用于 CHR 事件携带最近 PM 上下文；分钟 KPI 仍由独立的 PM 1m fact 链路产生。
   CFG state 默认 24h TTL，latest PM state 默认 10min TTL，用于减少长期闲置 key 对 checkpoint
   体积的影响。

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

小区级异常改到 1 分钟 KPI 后检测：

```
CellKpi MIN_1
  -> cell-kpi-anomaly-evaluations
  -> keyBy(cellId + ruleDimension)
  -> cell-kpi-anomaly-detect-dedup
  -> cell-anomaly-events
  -> selected result sink
```

小区规则按 `cellId + ruleDimension` 分组，连续 3 个 1 分钟 KPI 周期不满足同一门限时触发。进入异常 streak 时输出一次，直到该规则维度恢复正常前不重复输出。

| 规则维度 | 异常类型 | 默认门限 |
|---|---|---|
| `avgRsrp` | `CELL_RADIO_BAD` | `< -110` |
| `avgSinr` | `CELL_RADIO_BAD` | `< -3` |
| `attachSuccessRate` | `CELL_SERVICE_BAD` | `< 0.95` |
| `hoSuccessRate` | 暂不触发 | 保留为 KPI 字段；`CellKpi` 尚无 handover attempt 分母，当前不作为 `CELL_SERVICE_BAD` 规则输入 |
| `dropRate` | `CELL_SERVICE_BAD` | `> 0.05` |

用户级异常在 enrich 后与事件级链路并列：

```
EnrichedChr
  -> user-event-anomaly-evaluations
  -> keyBy(imsi + ruleDimension)
  -> user-event-anomaly-detect-dedup
  -> user-anomaly-events
  -> selected result sink
```

用户规则按 `imsi + ruleDimension` 分组。同一用户在 10 分钟内同一规则维度连续 3 个异常事件触发；同一维度出现正常或成功事件即打断序列。进入异常 streak 时输出一次，恢复前不重复输出。缺失 `imsi` 的记录跳过用户级异常检测，并计入轻量 invalid-input 指标，不写业务 DLQ。

| 规则维度 | 异常类型 | 默认门限 |
|---|---|---|
| attach/session/access failure | `USER_FAILURE` | 10 分钟内连续 3 次失败 |
| handover failure | `USER_FAILURE` | 10 分钟内连续 3 次失败 |
| poor RSRP | `USER_QOE_BAD` | `rsrp < -110` 连续 3 个事件 |
| poor SINR | `USER_QOE_BAD` | `sinr < -3` 连续 3 个事件 |
| high latency | `USER_QOE_BAD` | `latencyMs > 500` 连续 3 个事件 |

栅格级覆盖空洞保持 geohash/grid 检测：

```
Enriched location / CellKpi context
  -> keyBy(gridId or geohash)
  -> GridAnomalyDetector
  -> grid-anomaly-events
  -> selected result sink
```

`COVERAGE_HOLE` 仍表示栅格内低信号事件聚集。该规则可以使用经纬度生成 geohash；小区级异常不依赖 latitude/longitude。

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

KPI window 与重 sink 拆链。观测阶段使用 SinkLatencyProbe stage ID，实际 Flink sink name 仍保留在 sinkTo 节点上。业务结果 sink 按 `FDB_RESULT_SINK` 单选构建：

| `FDB_RESULT_SINK` | 创建的业务 sink 分支 |
|---|---|
| `starrocks` | `starrocks-kpi-1m`、`starrocks-kpi-5m`、`starrocks-cell-anomaly`、`starrocks-user-anomaly`、`starrocks-grid-anomaly` |
| `iceberg` | `iceberg-kpi-1m`、`iceberg-kpi-5m`、`iceberg-cell-anomaly`、`iceberg-user-anomaly`、`iceberg-grid-anomaly` |
| `hive` | `hive-kpi-1m`、`hive-kpi-5m`、`hive-cell-anomaly`、`hive-user-anomaly`、`hive-grid-anomaly` |
| `kafka` | `kafka-kpi-1m`、`kafka-kpi-5m`、`kafka-cell-anomaly`、`kafka-user-anomaly`、`kafka-grid-anomaly` |
| `none` | 不创建业务结果 sink，仅保留计算链路和可选 metrics |

通过 `startNewChain` / `disableChaining` 或显式算子边界避免 UI 上把窗口和多个 sink 合并为一个 vertex，方便定位 busy、backpressure 和 sink 延迟。
普通 Flink 作业默认不拆 enrichment 及异常检测链路，避免额外 runtime 开销；设置
`FDB_FLINK_DIAGNOSTIC_CHAINING=true` 时，会对 direct routing、enrichment、
enrichment metrics、用户事件异常评估/去重、栅格异常检测和 enrichment late sink
等关键诊断节点调用 `disableChaining`，Flink UI、前端流程图和 benchmark 报告会按
Flink REST 返回的 vertex 展示更细粒度节点。该模式用于定位反压和 busy 热点，不应和默认
operator chaining 模式直接比较性能上限。

### 5.9 Result Sink 开关

统一结果 sink 配置：

```text
FDB_RESULT_SINK=starrocks | iceberg | hive | kafka | none
FDB_DLQ_ENABLED=true | false
```

`FDB_RESULT_SINK` 控制 KPI 1m、KPI 5m、小区异常、用户异常、栅格异常五类业务结果。未选中的业务 sink 分支不创建，Flink Web UI、前端流程图和压测报告中也不展示这些分支。

DLQ 不属于业务结果 sink。`FDB_DLQ_ENABLED=true` 时，配置缺失、迟到或无法富化的数据可以写入 Kafka 兜底 topic；压测时可关闭以减少额外 Kafka 写入干扰。

Kafka metrics、动态均衡 heartbeat/routing、checkpoint/savepoint 不受 `FDB_RESULT_SINK` 控制。

---

## 6. Sink 与存储

### 6.1 Result Sink 类型

业务结果由 `FDB_RESULT_SINK` 单选落地：

| 类型 | 落地范围 | 说明 |
|---|---|---|
| `starrocks` | `cell_kpi`、`cell_anomaly_events`、`user_anomaly_events`、`grid_anomaly_events` | 面向在线查询和 StarRocks connector/Stream Load 写入压测。 |
| `iceberg` | `cell_kpi`、`cell_anomaly_events`、`user_anomaly_events`、`grid_anomaly_events` | 面向湖表写入、checkpoint commit 和小文件成本压测。 |
| `hive` | `kpi`、`cell_anomaly_events`、`user_anomaly_events`、`grid_anomaly_events` Parquet 路径 | 面向 HDFS/Hive FileSink 写入压测。 |
| `kafka` | `cell-kpi-1m`、`cell-kpi-5m`、`cell-anomaly-events`、`user-anomaly-events`、`grid-anomaly-events` | 面向 Kafka 输出链路压测。 |
| `none` | 无业务结果落地 | 面向纯计算链路压测。 |

### 6.2 Iceberg 表

Iceberg 模式写入 4 张表：

```text
iceberg_db.cell_kpi
iceberg_db.cell_anomaly_events
iceberg_db.user_anomaly_events
iceberg_db.grid_anomaly_events
```

`cell_kpi` 通过 `window_kind` 区分 `MIN_1` 与 `MIN_5`，分区为：

```text
window_kind, dt, hour
```

异常表分区为：

```text
dt, hour
```

`dt/hour` 均由业务事件时间或窗口起始时间派生。Iceberg sink 的数据可见性依赖 checkpoint commit，压测报告需要展示 checkpoint duration、snapshot commit 延迟、文件数和平均文件大小。

### 6.3 Hive Parquet 路径

Hive 模式按 dataset 分开写入：

```text
hdfs://namenode:8020/warehouse/fdb/kpi/window_kind=MIN_1/dt=<yyyy-MM-dd>/hour=<HH>/*.parquet
hdfs://namenode:8020/warehouse/fdb/kpi/window_kind=MIN_5/dt=<yyyy-MM-dd>/hour=<HH>/*.parquet
hdfs://namenode:8020/warehouse/fdb/cell_anomaly_events/dt=<yyyy-MM-dd>/hour=<HH>/*.parquet
hdfs://namenode:8020/warehouse/fdb/user_anomaly_events/dt=<yyyy-MM-dd>/hour=<HH>/*.parquet
hdfs://namenode:8020/warehouse/fdb/grid_anomaly_events/dt=<yyyy-MM-dd>/hour=<HH>/*.parquet
```

Hive FileSink 同样受 checkpoint 提交影响。Hive/Iceberg 模式默认 checkpoint interval 为 30 秒；用户可以调大，但 `FDB_FLINK_CHECKPOINT_INTERVAL_MS` 对 file-based result sink 不应超过 180 秒，避免实时可见性过差。小文件风险通过 rolling policy、checkpoint 配置和压测报告显式暴露，不通过隐藏异常结果来规避。

### 6.4 StarRocks 表

StarRocks 由 `../shared-data-infra` 的 `starrocks` profile 提供，本工程不重复定义 FE/BE 容器。StarRocks 模式写入：

```text
fdb.cell_kpi
fdb.cell_anomaly_events
fdb.user_anomaly_events
fdb.grid_anomaly_events
```

`cell_kpi` 中通过 `window_kind` 区分 `MIN_1` 与 `MIN_5`。业务结果通过 StarRocks Flink connector 以 Stream Load 写入，默认 `exactly-once` 语义随 checkpoint 事务提交；KPI 表使用 `(window_start_ts, window_kind, cell_id)` 主键，三类异常表使用稳定 `anomaly_id` 主键，便于重放和失败恢复时去重。StarRocks connector 的 `sink.buffer-flush.max-bytes`、`sink.buffer-flush.max-rows`、`sink.buffer-flush.interval-ms` 可分别通过 `FDB_STARROCKS_SINK_BUFFER_FLUSH_MAX_BYTES`、`FDB_STARROCKS_SINK_BUFFER_FLUSH_MAX_ROWS`、`FDB_STARROCKS_SINK_BUFFER_FLUSH_INTERVAL_MS` 配置；本地高强度压测默认下调这些值，避免多个 StarRocks sink 并行 subtask 的默认缓冲叠加撑高 TaskManager 内存峰值。

### 6.5 Kafka Result Topics

Kafka 模式写入：

```text
cell-kpi-1m
cell-kpi-5m
cell-anomaly-events
user-anomaly-events
grid-anomaly-events
```

这些 topic 是业务结果 topic，受 `FDB_RESULT_SINK=kafka` 控制。DLQ topic 和 metrics topic 不属于业务结果，不由 `FDB_RESULT_SINK` 控制。

开发环境初始化时直接重建 `cell_anomaly_events`、`user_anomaly_events`、`grid_anomaly_events` 三张异常表，不提供存量升级脚本。

### 6.6 Sink 耗时指标

每个 sink 分支都记录每次写入耗时。

字段：

| 字段 | 说明 |
|---|---|
| `sinkName` | 具体 sink 名称 |
| `sinkType` | kafka / starrocks / hive / iceberg |
| `dataset` | kpi_1m / kpi_5m / cell_anomaly / user_anomaly / grid_anomaly |
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
  -> 内存最新态
  -> FDB_RUN_HISTORY_DIR/<runId>/metrics.jsonl
  -> 控制台 Sink 耗时页 / 压测报告

Flink metrics
  -> Prometheus
  -> 指标面板
```

时延指标口径：

- Source delay：CHR/PM/CFG 入口按 `processing_time - source_event_time` 统计 p50/p95/p99。
- KPI availability delay：`kpi-1m` 与 `kpi-5m` 按 `processing_time - CellKpi.sourceEventTsAvg` 统计累计可用延迟；窗口等待、Join 等待和算子排队都会体现在该值里。
- Operator latency：单轮报告的 Operator 表展示 `累计延迟` 和 `算子延迟`。`累计延迟` 表示当前算子输出相对原始 `eventTs/sourceEventTsAvg` 的 P95；`算子延迟` 用当前算子累计延迟减去直接上游最大累计延迟近似得到。传输延迟来自 Flink latency marker，仅显示在流程图边上。
- Sink commit/write delay：业务结果增加 connector 写入与提交打点。`sink-front` 是进入 connector 前的累计延迟；`connector-write` 是 writer/invoke 耗时；`connector-commit` 是 flush/prepare/commit/checkpoint 耗时，不包含 `connector-write`。

Iceberg/Hive 额外展示 checkpoint commit、文件数量、平均文件大小、小于 1MB 文件数量，因为 file-based sink 的数据可见性和小文件成本会直接影响性能规格。

---

## 7. 查询 API 与控制台

### 7.1 API

| API | 说明 |
|---|---|
| `GET /api/results/kpi/1m` | 查询 1 分钟 KPI |
| `GET /api/results/kpi/5m` | 查询 5 分钟 KPI |
| `GET /api/results/anomalies/cell` | 查询小区级异常 |
| `GET /api/results/anomalies/user` | 查询用户级异常 |
| `GET /api/results/anomalies/grid` | 查询栅格级异常 |
| `GET /api/results/sink-latency` | 查询 sink 耗时统计 |
| `GET /api/flow/runtime` | 查询当前 run 配置、result sink、DLQ、metrics、Flink job 状态、并行度和 checkpoint 配置 |
| `GET /api/flow/status` | 查询 Flink 阶段状态；benchmark-runner 优先使用该实际接口，并兼容历史 `/api/flow/stages` |
| `GET /api/runs` | 查询历史压测 run |
| `GET /api/runs/{runId}` | 查询单次 run 明细、metrics 历史摘要和报告状态 |
| `GET /api/events/stream` | SSE 推送阶段、sink、异常摘要 |

结果 API 按当前 result sink 的查询能力读取结果。StarRocks 模式直接查询 StarRocks；Iceberg/Hive 模式优先通过 StarRocks external catalog 或可配置查询入口读取；Kafka/none 模式下结果页可降级为 topic/运行摘要，不假定具备完整历史查询能力。

### 7.2 前端页面

采用“当前运行态优先”的布局：

```text
流处理总览
KPI 1m
KPI 5m
小区异常
用户异常
栅格异常
Sink 耗时
执行历史
指标面板
```

`流处理总览` 顶部显示当前 run 条：

```text
Run ID | Result Sink | Metrics | DLQ | Parallelism | Checkpoint | Job Status | Report
```

中间流程图只显示当前实际创建的 DAG 节点：

- `starrocks` 模式只显示 StarRocks 业务结果 sink。
- `iceberg` 模式只显示 Iceberg 业务结果 sink。
- `hive` 模式只显示 Hive 业务结果 sink。
- `kafka` 模式只显示 Kafka 业务结果 sink。
- `none` 模式只显示计算链路，sink 区显示 disabled。

右侧显示瓶颈候选：

```text
反压
Checkpoint 耗时
Sink P95
输入积压
小文件
失败 / 重启
```

`Report` 显示 `collecting / ready / failed`；ready 时可打开 API 渲染后的临时摘要。benchmark-runner 压测交付以 `index.html` 和单轮 `report.html` 为准。

`KPI 1m` / `KPI 5m`：

- 时间范围筛选。
- `siteId` / `cellId` / `joinQuality` 筛选。
- 结果表格。
- KPI 趋势小图。
- 侧栏展示相关 sink 最近耗时、p95、失败数。

`小区异常`：

- 时间、`siteId`、`cellId`、`severity`、`anomalyType`、`entityId` 筛选。
- 异常明细表。
- 最近异常趋势。

`用户异常`：

- 时间、`imsi`、`cellId`、`severity`、`anomalyType`、`entityId` 筛选。
- 展示 `windowStartTs`、`windowEndTs`、规则维度、门限和观测值摘要。
- 最近用户异常趋势。

`栅格异常`：

- 表格为必交付能力。
- GIS 简单呈现为增强能力：根据 geohash 还原中心点或边界，展示散点或栅格色块。
- 如果 GIS 依赖或性能复杂，第一版可降级为表格 + 经纬度/geohash 列，不阻塞主功能。

`Sink 耗时`：

- 按 `sinkType + dataset + windowKind` 展示当前启用 sink 的最近一次耗时。
- 展示 p50 / p95 / p99、失败数、记录数、字节数。
- Iceberg/Hive 展示 checkpoint/snapshot commit 耗时和小文件摘要。

### 7.3 流程图显示规则

控制台流程图只显示实际创建的 DAG 节点。

异常节点显示规则：

- `cell-kpi-anomaly-detect-dedup` 显示在 `CellKpi MIN_1` 之后。
- `user-event-anomaly-detect-dedup` 显示在 `enrich` 之后，并与后续 CHR 1m fact 链路并列。
- `grid coverage-hole anomaly` 显示为独立栅格/geohash 分支。
- 小区异常页面不展示为依赖 `imsi`、`latitude` 或 `longitude` 的流程。

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

### 7.4 压测报告

每次 sink 上限压测使用 `benchmarkId` 归档批次配置、结构化结果、
Flink/业务/存储快照和 HTML 报告。压测交付件固定输出到
`benchmark-runner/output/benchmark-runs/<benchmarkId>/`，不使用 Docker volume
路径，也不提供输出目录配置项：

```text
benchmark-runner/output/benchmark-runs/<benchmarkId>/
  benchmark-config.json
  benchmark-results.json
  benchmark-summary.csv
  index.html
  runs/<runId>/
    run.json
    flink-snapshot.json
    fdb-metrics-snapshot.json
    storage-snapshot.json
    source-metrics.json
    report.html
```

Observability API 仍保留按 run 生成临时报告的能力，用于本地调试或
前端实时查看；正式压测交付以 `benchmark-runner` 生成的 HTML 为准。
Observability API 报告生成入口：

```bash
bash scripts/deploy.sh local report
bash scripts/deploy.sh external-yarn report
```

Sink benchmark orchestration entry:

```bash
bash scripts/benchmark.sh local
bash scripts/benchmark.sh external-yarn
bash scripts/benchmark.sh local --env benchmark-runner/conf/.env.benchmark-starrocks-30eps-scale-20260719-1148.tmp
bash scripts/benchmark.sh local --env benchmark-runner/conf/.env.benchmark-starrocks-30eps-scale-20260719-1148.tmp --set FDB_BENCHMARK_CELL_LEVELS="2000 5000"
```

`scripts/benchmark.sh` is a thin launcher for the Java `benchmark-runner`
module. It loads `.env`, locates the runner jar, and passes the target plus CLI
arguments through. It does not contain benchmark state-machine logic.

压测配置文件目标态统一放在 `benchmark-runner/conf/` 下。历史复跑或临时压测
使用 `--env <file>` 显式注入 `.env.benchmark-*.tmp`；`FDB_ENV_FILE` 仍作为
兼容入口，但命令行 `--env` 优先。少量参数覆写使用重复的
`--set KEY=VALUE`，runner 在读取 env 文件后再应用这些覆写值。

The Java `benchmark-runner` owns sink upper-bound benchmarking:

- run a `sink x cellLevel` matrix;
- use generated cell count as the primary pressure multiplier;
- treat `cellLevel` as the target generated cell count, not site count;
- calculate `targetChrEpsPerCell = FDB_BENCHMARK_CHR_EPS_PER_CELL`;
- calculate `targetChrTotalEps = cellLevel * targetChrEpsPerCell`;
- calculate `targetPmEpsPerCell = FDB_BENCHMARK_PM_EPS_PER_CELL`;
- calculate `targetPmTotalEps = cellLevel * targetPmEpsPerCell`;
- `Target CHR EPS` 表示每小区每秒 CHR 目标输出记录数；全局总压力量使用
  `Global CHR EPS = cellLevel * Target CHR EPS` 表示；
- call `deploy.sh <target> prepare` before each run to reset benchmark data;
- start topology-service plus CFG/PM/CHR simulators on the runner host;
- submit and stop Flink through the existing `deploy.sh <target> submit/stop`
  commands so local and external-yarn deployment behavior stays centralized;
- poll Flink REST API, Observability API, and storage probes during warmup and
  measurement;
- stop higher cell levels for a sink after the first unstable or failed level;
- generate HTML reports and machine-readable JSON/CSV output.

First-version benchmark matrix:

```text
FDB_BENCHMARK_SINKS=none starrocks kafka hive iceberg
FDB_BENCHMARK_CELL_LEVELS=10000 20000 40000
FDB_BENCHMARK_CHR_EPS_PER_CELL=30
FDB_BENCHMARK_PM_EPS_PER_CELL=1
FDB_CHR_PRODUCER_THREADS=6
FDB_BENCHMARK_ANOMALY_INJECTION_RATIO=0.05
FDB_BENCHMARK_WARMUP_SEC=60
FDB_BENCHMARK_DURATION_SEC=300
```

Pressure model:

- `cellLevel` controls the target generated cell count and is the primary
  benchmark multiplier. The topology generator should translate this into a
  site count or stop after the target cell count is reached.
- CFG baseline records grow with cell count.
- PM and CHR total EPS grow linearly with generated cell count.
- `Target CHR EPS` is the per-cell per-second CHR output target. For example,
  `FDB_BENCHMARK_CHR_EPS_PER_CELL=30` means each generated cell targets 30 CHR
  records/s; with `cellLevel=50000`, `Global CHR EPS` is `1500000`.
- `Target PM EPS` follows the same per-cell model. For example,
  `FDB_BENCHMARK_PM_EPS_PER_CELL=1` means each generated cell targets 1 PM
  record/s; with `cellLevel=50000`, `Global PM EPS` is `50000`.
- KPI and sink output volume grow with active cells.

Run id format:

```text
<benchmarkId>-<sink>-cells<cellLevel>-chr-eps<targetChrEpsPerCell>
```

Observation sources:

- Flink REST API: job status, vertices, subtasks, records/bytes in/out, busy,
  idle, backpressure, checkpoint summary, latest checkpoint duration/failures,
  TaskManager and slot status.
- Observability API / fdb metrics: CHR/PM/CFG source delay, KPI 1m/5m
  availability delay, sink probe p50/p95/p99, sink records/s, watermark lag,
  dataset and window dimensions.
- Storage probes: Kafka offsets, StarRocks row ranges, Hive file/in-progress
  files, Iceberg files/snapshots/metadata and recent partitions.

Stable/unstable/failed decisions:

- `failed`: submit failed, Flink job failed/canceled, or the run cannot complete.
- `unstable`: sustained backpressure, checkpoint failures or slow checkpoint,
  KPI availability delay over threshold, sink latency over threshold, growing
  sink failures, producer under-delivery, Kafka/source backlog growth, or
  storage probe anomalies.
- `stable`: job stays RUNNING and all thresholds remain healthy through the
  measurement window.

Default thresholds:

```text
FDB_BENCHMARK_POLL_INTERVAL_SEC=10
FDB_BENCHMARK_MAX_BACKPRESSURE_RATIO=0.2
FDB_BENCHMARK_MAX_CHECKPOINT_DURATION_MS=120000
FDB_BENCHMARK_MAX_CONSECUTIVE_CHECKPOINT_FAILURES=2
FDB_BENCHMARK_MAX_KPI_AVAILABILITY_P95_MS=180000
FDB_BENCHMARK_MAX_SINK_P95_MS=180000
FDB_BENCHMARK_MAX_WATERMARK_LAG_MS=180000
FDB_BENCHMARK_MIN_PRODUCER_DELIVERY_RATIO=0.98
FDB_BENCHMARK_MAX_SOURCE_BACKLOG_RECORDS=0
```

关键压测变量语义：

- `FDB_BENCHMARK_CHR_EPS_PER_CELL=30`：Target CHR EPS，每小区每秒 CHR 目标输出记录数；全局总压力量为 `cellLevel * Target CHR EPS`。
- `FDB_BENCHMARK_PM_EPS_PER_CELL=1`：Target PM EPS，每小区每秒 PM 目标输出记录数；全局总压力量为 `cellLevel * Target PM EPS`。
- `FDB_BENCHMARK_ANOMALY_INJECTION_RATIO=0.05`：5% cell 和 5% user 进入确定性异常 cohort。
- `FDB_BENCHMARK_MIN_PRODUCER_DELIVERY_RATIO=0.98`：source 实际交付率低于目标 98% 时判定 unstable。
- `FDB_BENCHMARK_MAX_SOURCE_BACKLOG_RECORDS=0`：source backlog 超过 0 条即判定 unstable。

Benchmark artifacts are fixed under the runner module and are not Docker-volume
paths:

```text
benchmark-runner/output/benchmark-runs/<benchmarkId>/
  benchmark-config.json
  benchmark-results.json
  benchmark-summary.csv
  index.html
  runs/<runId>/
    run.json
    flink-snapshot.json
    fdb-metrics-snapshot.json
    storage-snapshot.json
    source-metrics.json
    report.html
```

运行期日志统一写入仓库根目录 `logs/`。`deploy.sh` 的 Flink submit 输出使用
`logs/local-flink-submit.out` 或 `logs/external-yarn-submit.out`；
local smoke 的 topology/simulator 日志使用 `logs/topology.log`、
`logs/cfg.log`、`logs/pm.log`、`logs/chr.log`；benchmark runner 拉起的
Java 子进程 stdout/stderr 使用 `logs/benchmark-*.out|err`。

本地 `submit` 在提交前要求 Flink 可用 slot 数不少于 `FDB_FLINK_PARALLELISM`。
如果 TaskManager 因手动重建或默认 compose 配置回落导致 slot 不足，脚本会在
source 数据开始发布前失败，避免把资源错配误判为 sink 压测结果。

`index.html` is the primary delivery artifact. It includes benchmark
configuration, stable upper-bound cards per sink, the `sink x cellLevel` matrix,
failure points, cross-sink metric comparisons, global recommendations, per-sink
recommendations, and links to every single-run `report.html`.

报告交付为 HTML-only；单轮 `report.html` 必须直观展示 Run Summary、Source Density、
Latency、Source Backlog 和 Checkpoint 信息。无 latency 样本时显示 `N/A`，不能裸露
`-1` 或误显示 `0 ms`。

Per-run `prepare` resets the Kafka benchmark topics, StarRocks result tables,
and Hive/Iceberg HDFS output paths before topology generation and simulator
startup. This makes sink comparisons less sensitive to old Kafka offsets,
historic compact-topic replays, or stale storage rows/files.

The simulator writes CHR/PM/CFG source metrics into per-run source metrics
files. Benchmark-runner folds those into `source-metrics.json`, and the
single-run report shows source totals, observed rates, per-cell density,
backlog, and producer delivery attainment.

Flink operator `Records In/s`, `Records Out/s`, `Bytes In/s`, and `Bytes Out/s`
must come from per-vertex Flink metrics API values such as
`numRecordsInPerSecond`. Cumulative `/jobs/{jobId}` counters are shown only as
`Records In Total` and `Records Out Total`, not as `/s` rates.

Benchmark source pacing and latency rules:

- CHR simulator must pace publishing by the configured global target EPS. If it
  cannot deliver at least `FDB_BENCHMARK_MIN_PRODUCER_DELIVERY_RATIO` of target
  EPS after warmup, the run is `unstable` because the pressure source itself is
  saturated.
- CHR simulator can use `FDB_CHR_PRODUCER_THREADS` producer worker threads
  inside one process. `FDB_RATE_EPS` remains the global target EPS; workers split
  that target evenly and each worker owns an independent Kafka producer.
- PM and CFG simulators publish according to their business cadence; the report
  must show total records, records/s, records/cell total, and records/s/cell for
  CHR, PM, and CFG.
- Topology `Published Topology Records` counts topology-service cell records and
  usually equals `cellLevel`; it is not CHR/PM event volume.
- Kafka/Flink source backlog is part of stability judgment. A run is
  `unstable` when source lag/backlog grows beyond the configured threshold even
  if downstream sink latency has not crossed its threshold yet.
- 时延拆成 source delay、KPI availability delay、算子累计/增量延迟、connector write/commit delay。KPI 与 sink 的累计延迟使用原始 `eventTs/sourceEventTsAvg`，不使用窗口结束时间。窗口等待和 watermark lag 与 producer/source backlog 分开展示。
- p50/p95/p99 must be calculated from observed samples. When there are no
  samples, reports display `N/A`, not `0 ms`.
- Result sink checkpoint cadence is shown in the report. Default checkpoint
  interval remains `30s`; file-based Hive/Iceberg sinks may be raised but must
  not exceed `180s`.

Benchmark anomaly injection:

- `FDB_BENCHMARK_ANOMALY_INJECTION_RATIO=0.05` means 5% of generated cells and
  5% of generated users are assigned to deterministic anomaly cohorts for the
  run.
- Cell anomaly injection is cohort-based, not random single-record noise. The
  selected cells produce consecutive low radio quality or poor service KPI
  signals so the existing 3-minute cell anomaly rule can activate.
- User anomaly injection is cohort-based. The selected users produce consecutive
  access failure or QoE-bad CHR events within the configured user anomaly window
  so user anomaly rules can activate.
- Grid anomaly injection is also data-source driven. Anomalous CHR records keep
  the same low RSRP/SINR and failure fields, and their latitude/longitude
  converge to a small set of stable geohash6 hotspots so the existing
  `CoverageHoleDetector` can reach its per-grid low-signal threshold and emit
  `COVERAGE_HOLE` without synthetic downstream events.
- Injected anomalies are marked only through ordinary CHR/PM field values; the
  Flink detection logic still relies on the same business rules and dedup state
  used in non-benchmark runs.

单轮 `report.html` 采用“顶部总览 + 分区钻取”布局。首屏需要回答本轮是否稳定、
施加了多大压力、发现了什么瓶颈或告警、下一步建议调优什么；下方再按结构化分区
展示明细，不再把 Java `toString()` 输出作为主要阅读内容：

- 运行摘要：sink、cell level、Target CHR/PM EPS、Global CHR/PM EPS、实际 in/out EPS、target、warmup、
  duration、最终状态和瓶颈原因；
- Flink 资源：job status、TaskManager 数、slots、job 并行度、checkpoint
  duration/failure、restart/fail/cancel 信号和 backpressure；
- 算子吞吐：每个 Flink vertex/operator 一行，展示名称、并行度、records/bytes
  in/out per second、busy/idle/backpressure（可采集时）和紧凑健康状态；
- 时延：source delay、KPI 1m 可见性、KPI 5m 可见性、sink latency 和
  watermark lag，优先展示 p50/p95/p99；第一版如果只有 p95，则先展示 p95；
- sink 与存储：sink records/s、bytes/s、p50/p95/p99、失败数，以及所选 sink
  的 storage probe 明细；包括 StarRocks 健康检查、Kafka offsets、Hive
  file/in-progress files、Iceberg files/snapshots 等适用信息；
- 原始产物：链接到 `run.json`、`flink-snapshot.json`、
  `fdb-metrics-snapshot.json` 和 `storage-snapshot.json`，用于机器级复盘。

视觉呈现保持紧凑、偏运维诊断：优先使用状态 badge、小型指标卡和表格，少用长段文字。
大段原始 JSON 或 preformatted 内容不放在主阅读区域，只通过链接或页面末尾的原始
快照区暴露。

The first version does not generate Markdown reports. JSON and CSV files are
kept for repeatable analysis and post-processing.

`benchmark-runner` unit tests cover configuration parsing, matrix expansion,
Flink REST parsing, decision rules, fake-client orchestration, JSON/CSV output,
HTML generation, and output path safety. `scripts/test-benchmark-dispatch.sh`
only covers the thin shell launcher: help output, jar checks, env loading, and
argument pass-through. Shell tests are not attached to the Maven lifecycle.

`FDB_REPORT_ON_STOP=true` 时，`stop` 后自动尝试生成报告。报告内容包括：

- run 基本信息：时间、git commit、jar SHA、target、result sink、parallelism、checkpoint interval。
- Flink 资源：TaskManager、slots、job 状态、restart/fail/cancel 次数。
- 输入吞吐：CHR/PM/CFG records/s、bytes/s。
- 处理阶段：enrichment、kpi join、5m rollup 的吞吐、busy/idle/backpressure。
- sink 指标：dataset、records、bytes、records/s、p50/p95/p99、失败数。
- checkpoint 指标：成功次数、失败次数、平均/最大 duration、alignment time。
- 存储状态：Kafka/StarRocks/Hive/Iceberg 数据量、文件数、小文件数量和最近老化结果。
- 自动结论：标记高 backpressure、checkpoint 变慢、sink p95 异常、失败重启、小文件过多等瓶颈候选。

---

## 8. 数据老化与容量上限

统一目标：

```text
retention.time=1h
retention.bytes=10GB
```

覆盖范围：

- CHR、PM、KPI、异常、DLQ、指标等 delete topic。
- StarRocks 业务结果表。
- Hive Parquet 业务结果数据。
- Iceberg 业务结果数据与快照。
- 本地运行历史、metrics JSONL 与压测报告。

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

业务结果表按小时分区或按事件时间治理。维护脚本定时：

1. 创建未来 2 小时分区。
2. 删除早于 1 小时的分区。
3. 检查表数据量，超过 10GB 时优先删除最老分区。

### 8.3 Hive Parquet

按 dataset 与 `dt/hour` 删除过期目录。KPI 额外按 `window_kind` 分区。删除后需要刷新元数据。

### 8.4 Iceberg

维护任务：

1. 删除早于 1 小时的业务结果数据文件。
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
| `FDB_FLINK_ENRICHMENT_PARALLELISM` | `FDB_FLINK_PARALLELISM` | Enrichment 阶段独立并行度 |
| `FDB_FLINK_USER_ANOMALY_PARALLELISM` | `FDB_FLINK_PARALLELISM` | 用户级异常检测阶段独立并行度 |
| `FDB_FLINK_GRID_ANOMALY_PARALLELISM` | `FDB_FLINK_PARALLELISM` | 网格覆盖异常检测阶段独立并行度 |
| `FDB_FLINK_KPI_PARALLELISM` | `FDB_FLINK_PARALLELISM` | CHR/PM 1m 聚合、KPI join、5m rollup 等 KPI 阶段独立并行度 |
| `FDB_FLINK_CHECKPOINT_INTERVAL_MS` | `30000` | checkpoint 间隔；Hive/Iceberg result sink 不应超过 180000ms |
| `FDB_DYNAMIC_BALANCING_ENABLED` | `false` | 是否启用动态均衡 |
| `FDB_VBUCKET_COUNT` | `1024` | 动态均衡启用时的虚拟分片数 |
| `FDB_CHR_WATERMARK_OUT_OF_ORDER_MS` | `2000` | CHR source watermark 最大乱序等待 |
| `FDB_PM_WATERMARK_OUT_OF_ORDER_MS` | `2000` | PM source watermark 最大乱序等待 |
| `FDB_KPI_JOIN_WAIT_MS` | `10000` | CHR/PM/CFG 1 分钟 KPI 拼接在窗口结束后的等待时间 |
| `FDB_RESULT_SINK` | `starrocks` | 业务结果 sink：`starrocks` / `iceberg` / `hive` / `kafka` / `none` |
| `FDB_DLQ_ENABLED` | `true` | 是否启用 DLQ 兜底 topic |
| `FDB_METRICS_ENABLED` | `true` | 是否启用 Flink metrics probe 上报 |
| `FDB_METRICS_EMIT_INTERVAL_MS` | `5000` | metrics 采样输出间隔 |
| `FDB_METRICS_SAMPLE_EVERY_RECORDS` | `1` | Stage probe 每 N 条记录计算一次业务延迟；Flink records counter 仍统计全量 |
| `FDB_SINK_METRICS_SAMPLE_EVERY_RECORDS` | `1` | Sink probe 每 N 条记录计算一次业务延迟和估算 bytes；sink records 仍统计全量 |
| `FDB_METRICS_HISTORY_ENABLED` | `true` | Observability API 是否写本地 JSONL 历史 |
| `FDB_REPORT_ON_STOP` | `false` | stop 后是否自动生成压测报告 |
| `FDB_BENCHMARK_SINKS` | `none starrocks kafka hive iceberg` | `benchmark-runner` 顺序执行的 sink 列表，支持空格或逗号分隔 |
| `FDB_BENCHMARK_CELL_LEVELS` | `10000 20000 40000` | 小区数升压档位；第一版用小区数作为主压力倍率 |
| `FDB_BENCHMARK_CHR_EPS_PER_CELL` | `30` | Target CHR EPS，每小区每秒 CHR 目标输出记录数；Global CHR EPS = cellLevel * epsPerCell |
| `FDB_BENCHMARK_PM_EPS_PER_CELL` | `1` | Target PM EPS，每小区每秒 PM 目标输出记录数；Global PM EPS = cellLevel * epsPerCell |
| `FDB_CHR_PRODUCER_THREADS` | `6` | CHR simulator 单进程内 producer worker 线程数；`FDB_RATE_EPS` 是全局目标，各线程均分 |
| `FDB_BENCHMARK_DIAGNOSTIC_CHAINING` | `true` | benchmark-runner 默认开启诊断拆链，并向 Flink 作业注入 `FDB_FLINK_DIAGNOSTIC_CHAINING`；普通 Flink 作业不设置时默认关闭 |
| `FDB_BENCHMARK_ANOMALY_INJECTION_RATIO` | `0.05` | 压测异常注入比例；5% cell 和 5% user 进入确定性异常 cohort |
| `FDB_BENCHMARK_MIN_PRODUCER_DELIVERY_RATIO` | `0.98` | producer 实际交付率低于目标 EPS 的比例阈值时，本轮 unstable |
| `FDB_BENCHMARK_MAX_SOURCE_BACKLOG_RECORDS` | `0` | source backlog 容忍阈值；持续增长或超过阈值时，本轮 unstable |
| `FDB_BENCHMARK_WARMUP_SEC` | `60` | 单轮 submit 后、启动 simulator 前的 Flink 作业预热秒数 |
| `FDB_BENCHMARK_DURATION_SEC` | `300` | 兼容旧配置；未设置 `FDB_BENCHMARK_SIMULATION_DURATION_SEC` 时作为模拟器输出时长 |
| `FDB_BENCHMARK_SIMULATION_DURATION_SEC` | `FDB_BENCHMARK_DURATION_SEC` | 单轮 simulator 在源端真实发数循环内固定输出数据的秒数，例如 `300` 表示输出 5 分钟 Simulation 数据量后自行退出 |
| `FDB_BENCHMARK_DRAIN_TIMEOUT_SEC` | `300` | simulator 自行退出后继续等待窗口物化、sink/checkpoint 追平的最长秒数 |
| `FDB_BENCHMARK_POLL_INTERVAL_SEC` | `10` | runner 在 drain 阶段采样 Flink REST、Observability API 和存储探测的周期 |
| `FDB_BENCHMARK_PROBE_COMMAND_TIMEOUT_SEC` | `FDB_HDFS_PROBE_TIMEOUT_SEC` 或 `20` | storage probe 外部命令的 Java 侧总超时；只影响观测路径，不改变 deploy/submit 超时 |
| `FDB_LOCAL_FLINK_SUBMIT_TIMEOUT_SEC` | `120` | 本地 `flink run` 命令超时 |
| `FDB_LOCAL_FLINK_SUBMIT_LATE_WAIT_SEC` | `30` | 本地 submit 命令超时后，继续等待迟到 JobID 的秒数 |
| `FDB_LOCAL_FLINK_SUBMIT_RETRY_ON_UNKNOWN` | `0` | 默认不重试“未拿到 JobID 的未知提交结果”，避免同一轮压测重复提交多个 Flink 作业；确认首次未到达 JobManager 时才设为 `1` |
| `FDB_LOCAL_FLINK_REST_TIMEOUT_SEC` | `10` | 本地 submit 阶段通过 Flink REST 发现迟到 JobID 的单次请求超时 |
| `FDB_TOPOLOGY_METADATA_TIMEOUT_MS` | `120000` | topology publisher 在发送前等待 Kafka topic metadata 可见的最长时间 |
| `FDB_TOPOLOGY_PRODUCER_MAX_BLOCK_MS` | `15000` | topology 发布使用的 Kafka producer `max.block.ms`，避免 metadata 卡顿拖住整轮压测 |
| `FDB_TOPOLOGY_PRODUCER_REQUEST_TIMEOUT_MS` | `15000` | topology 发布使用的 Kafka producer request timeout |
| `FDB_TOPOLOGY_PRODUCER_DELIVERY_TIMEOUT_MS` | `30000` | topology 发布使用的 Kafka producer delivery timeout |
| `FDB_BENCHMARK_MAX_BACKPRESSURE_RATIO` | `0.2` | 持续反压阈值，超过则当前档位 unstable |
| `FDB_BENCHMARK_MAX_CHECKPOINT_DURATION_MS` | `120000` | checkpoint duration 阈值 |
| `FDB_BENCHMARK_MAX_CONSECUTIVE_CHECKPOINT_FAILURES` | `2` | 连续 checkpoint 失败阈值 |
| `FDB_BENCHMARK_MAX_KPI_AVAILABILITY_P95_MS` | `180000` | KPI 1m/5m 出数 p95 延迟阈值 |
| `FDB_BENCHMARK_MAX_SINK_P95_MS` | `180000` | 当前 sink p95 延迟阈值 |
| `FDB_BENCHMARK_MAX_WATERMARK_LAG_MS` | `180000` | watermark lag 阈值 |
| `FDB_RATE_MAX_OUT_OF_ORDER_LAG_MS` | `2000` | CHR simulator 最大乱序上限；超过 2s 的配置会被截断 |
| `FDB_PM_MAX_OUT_OF_ORDER_LAG_MS` | `2000` | PM simulator windowEnd 最大落后上限；超过 2s 的配置会被截断 |
| `FDB_RUN_ID` | 自动生成 | 当前压测 run 标识 |
| `FDB_RUN_LABEL` | 空 | 当前压测 run 可读标签 |
| `FDB_ANOMALY_CELL_CONSECUTIVE_MINUTES` | `3` | 小区 KPI 连续异常分钟数 |
| `FDB_ANOMALY_CELL_RSRP_MIN` | `-110` | 小区无线质量 RSRP 下限 |
| `FDB_ANOMALY_CELL_SINR_MIN` | `-3` | 小区无线质量 SINR 下限 |
| `FDB_ANOMALY_CELL_ATTACH_SUCCESS_MIN` | `0.95` | 小区接入成功率下限 |
| `FDB_ANOMALY_CELL_HO_SUCCESS_MIN` | `0.90` | 预留配置；`CellKpi` 尚无 handover attempt 分母，当前不触发小区服务异常 |
| `FDB_ANOMALY_CELL_DROP_RATE_MAX` | `0.05` | 小区掉话率上限 |
| `FDB_ANOMALY_USER_CONSECUTIVE_EVENTS` | `3` | 用户级连续异常事件数 |
| `FDB_ANOMALY_USER_WINDOW_MINUTES` | `10` | 用户级异常检测窗口 |
| `FDB_ANOMALY_USER_RSRP_MIN` | `-110` | 用户体验 RSRP 下限 |
| `FDB_ANOMALY_USER_SINR_MIN` | `-3` | 用户体验 SINR 下限 |
| `FDB_ANOMALY_USER_LATENCY_MS_MAX` | `500` | 用户体验时延上限 |

### 9.2 Kafka topic

| 配置 | 默认值 |
|---|---|
| `FDB_CHR_TOPIC` | `chr-events` |
| `FDB_PM_TOPIC` | `pm-stats` |
| `FDB_CFG_TOPIC` | `cfg-config` |
| `FDB_KPI_1M_TOPIC` | `cell-kpi-1m` |
| `FDB_KPI_5M_TOPIC` | `cell-kpi-5m` |
| `FDB_CELL_ANOMALY_TOPIC` | `cell-anomaly-events` |
| `FDB_USER_ANOMALY_TOPIC` | `user-anomaly-events` |
| `FDB_GRID_ANOMALY_TOPIC` | `grid-anomaly-events` |

### 9.3 StarRocks

| 配置 | 默认值 |
|---|---|
| `FDB_STARROCKS_FE_ENDPOINT` | `starrocks-fe:9030` |
| `FDB_STARROCKS_CONNECTOR_JDBC_URL` | `jdbc:mysql://starrocks-fe:9030` |
| `FDB_STARROCKS_LOAD_URL` | `starrocks-fe:8030` |
| `FDB_STARROCKS_USER` | `root` |
| `FDB_STARROCKS_PASSWORD` | 空 |
| `FDB_STARROCKS_DATABASE` | `fdb` |
| `FDB_STARROCKS_SINK_SEMANTIC` | `exactly-once` |
| `FDB_STARROCKS_SINK_LABEL_PREFIX` | 运行时按 `FDB_RUN_ID` 派生 |

StarRocks FE/BE 来自 `../shared-data-infra`，本工程只接入 external network。`FDB_STARROCKS_JDBC_URL` 仍可用于 API 和维护脚本的 SQL 查询；Flink 作业写入使用 connector 专用的 JDBC 查询端点与 Stream Load 端点。

### 9.4 Iceberg

| 配置 | 默认值 |
|---|---|
| `FDB_ICEBERG_WAREHOUSE` | `hdfs://namenode:8020/warehouse/iceberg` |
| `FDB_ICEBERG_CATALOG` | `fdb_iceberg` |
| `FDB_ICEBERG_DATABASE` | `iceberg_db` |
| `FDB_ICEBERG_KPI_TABLE` | `cell_kpi` |
| `FDB_ICEBERG_CELL_ANOMALY_TABLE` | `cell_anomaly_events` |
| `FDB_ICEBERG_USER_ANOMALY_TABLE` | `user_anomaly_events` |
| `FDB_ICEBERG_GRID_ANOMALY_TABLE` | `grid_anomaly_events` |

### 9.5 Hive / File Sink

| 配置 | 默认值 |
|---|---|
| `FDB_HIVE_WAREHOUSE` | `hdfs://namenode:8020/warehouse/fdb` |
| `FDB_FILE_SINK_ROLLOVER_INTERVAL_MS` | `600000` |
| `FDB_FILE_SINK_INACTIVITY_INTERVAL_MS` | `300000` |
| `FDB_FILE_SINK_MAX_PART_SIZE_BYTES` | `134217728` |

### 9.6 部署目标

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
| `FDB_STARROCKS_FE_ENDPOINT` | `starrocks-fe:9030` | `starrocks-fe01:9030` | StarRocks FE 查询端口，脚本可据此派生 connector JDBC URL |
| `FDB_STARROCKS_CONNECTOR_JDBC_URL` | `jdbc:mysql://starrocks-fe:9030` | `jdbc:mysql://starrocks-fe01:9030` | StarRocks connector JDBC 查询端点，可覆盖自动派生值 |
| `FDB_STARROCKS_LOAD_URL` | `starrocks-fe:8030` | `starrocks-fe01:8030` | StarRocks Stream Load endpoint，可覆盖自动派生值 |
| `FDB_STARROCKS_SINK_SEMANTIC` | `exactly-once` | `exactly-once` | StarRocks connector 写入语义 |

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
| StarRocks result sink | connector exactly-once + 幂等主键 | 默认随 checkpoint 事务提交；KPI 和异常表按主键去重 |
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
| Unit | 小区 KPI 单输入 detect-dedup 算子在连续 3 个 1 分钟异常周期后只触发一次，恢复后才允许再次触发 |
| Unit | 用户事件单输入 detect-dedup 算子在 10 分钟内连续 3 个同维度异常事件后触发，正常/成功事件会打断序列 |
| Unit | 用户事件异常检测跳过缺失 `imsi` 的记录并计入轻量 invalid-input 指标 |
| Unit | CFG 缺失时 enrich 主流继续输出，并写入 `enrichment-late` 侧通道 |
| Unit | 动态均衡开关关闭时不构建相关 DAG 分支 |
| Unit | `FDB_RESULT_SINK` 只构建一种业务结果 sink，未选 sink 不出现在 StreamGraph/plan |
| Unit | `FDB_DLQ_ENABLED=false` 时不创建 DLQ sink |
| Unit | SinkLatencyProbe 统计 duration、records、bytes、failure |
| Unit | metrics disabled 时 probe 不向 Kafka 发布样本 |
| Unit | Observability API 将 metrics sample 追加到 `metrics.jsonl` 并生成报告摘要 |
| Integration | Embedded Kafka + Flink MiniCluster 小流量端到端 |
| Integration | StarRocks DDL 与 KPI、cell/user/grid 三类异常写入 |
| Integration | Hive/Iceberg KPI 与 cell/user/grid 三类异常 data files、checkpoint commit、snapshot 可见 |
| Integration | Hive/Iceberg checkpoint interval 默认 30s，配置值不超过 180s |
| E2E | 共享 Kafka/Hive/HDFS/StarRocks + 项目 Flink + 控制台 API |
| Frontend | 总览页按当前 `resultSink` 只渲染真实 sink 节点，并展示报告入口 |
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

### 12.1 2026-07-14 Sink Benchmarking Implementation Status

本轮实现已经落地以下范围：

- Flink job 包结构拆分为 `config/source/model/enrich/kpi/anomaly/balance/sink/metrics` 等子包，`FlinkJobMain` 保持为拓扑入口。
- `FDB_RESULT_SINK=starrocks|iceberg|hive|kafka|none` 已控制 KPI 1m、KPI 5m、小区异常和栅格异常四类业务结果，每次运行只构建一种业务 sink 分支；2026-07-15 目标态在此基础上扩展用户异常，见 12.2。
- `FDB_DLQ_ENABLED`、`FDB_METRICS_ENABLED`、`FDB_METRICS_HISTORY_ENABLED`、`FDB_RUN_ID`、`FDB_RUN_LABEL`、`FDB_REPORT_ON_STOP` 等运行时开关已贯通 Flink、observability-api、compose 和 deploy 脚本。
- `deploy.sh local|external-yarn benchmark` 曾作为第一版脚本级闭环复用 submit/stop/report，按 `FDB_BENCHMARK_SINKS` 顺序执行 sink 压测矩阵，并生成早期批次汇总文件。2026-07-16 目标态调整为 `scripts/benchmark.sh` 启动 Java `benchmark-runner`，见 12.3。
- metrics 仍走 `fdb-stage-metrics` Kafka topic；observability-api 保留内存最新值，并按 run 写入本地 `metrics.jsonl`。正式压测报告由 benchmark-runner 生成 HTML。
- 前端流处理总览页读取 `/api/flow/runtime`，展示当前 run/result sink/metrics/DLQ/parallelism/checkpoint/job/report，并按已知 active result sink 过滤流程图、阶段面板和 sink 面板。
- 本地验证在 shared-data-infra 运行时完成了 `starrocks` run：Flink job RUNNING 时 60/60 tasks running，plan 只包含 StarRocks business sink，不包含 Hive/Iceberg/Kafka business sink；`deploy.sh local report` 返回 `status=ready` 并生成过 Observability API 临时报告。

仍作为后续增强的项：

- Hive/Iceberg 文件数、平均文件大小、小文件数量和 snapshot/checkpoint commit 明细在报告中的深度统计。
- 前端 `Report` ready 后渲染 Observability API 临时报告正文；benchmark-runner 交付报告通过静态 HTML 打开。

### 12.2 2026-07-15 Entity Anomaly Design Refresh

目标态刷新为：

- 小区异常从 enriched CHR 事件级检测迁移到 `CellKpi MIN_1` 后的单输入规则状态检测。
- 用户异常在 enrich 后新增单输入规则状态检测分支，与事件级处理链路并列。
- 栅格覆盖空洞检测保持 geohash/grid 检测，不并入小区异常。
- 异常结果拆为 `cell_anomaly_events`、`user_anomaly_events`、`grid_anomaly_events` 三张表和三类 topic。
- dev 初始化直接重建三张异常表；当前没有存量部署，不编写升级说明。

### 12.3 2026-07-16 Sink 上限压测 Runner 设计

目标态刷新为 Java `benchmark-runner` 承载压测编排，`scripts/benchmark.sh`
仅作为启动入口。`deploy.sh` 继续负责部署动作，不再承载复杂压测状态机。

- 压测主线为 Sink 上限压测，第一版只做 `sink x cellLevel`，不展开
  parallelism/checkpoint 矩阵。
- 升压方式为逐级升压。每个 sink 从低 cell level 开始，遇到
  `unstable` 或 `failed` 后停止该 sink 后续更高档位。
- 压力倍率以小区数为主。`cellLevel` 控制 topology size、CFG baseline、
  PM 窗口记录数、KPI key 空间和 sink 写入规模。
- Target CHR EPS 由 `FDB_BENCHMARK_CHR_EPS_PER_CELL` 配置，表示每小区每秒
  CHR 目标输出记录数；Global CHR EPS 由 `cellLevel * Target CHR EPS` 计算。
- CHR 模拟器必须按目标全局 EPS 做每秒节奏控制；producer 实际交付率低于阈值，
  或 Kafka/Flink source backlog 持续增长时，本轮判定为 `unstable`。
- 单轮压测按固定 Simulation 数据量运行。runner 在 submit 后先按
  `FDB_BENCHMARK_WARMUP_SEC` 预热 Flink 作业，再启动 topology/simulator；
  simulator 以源端真实发数循环为起点，运行 `FDB_BENCHMARK_SIMULATION_DURATION_SEC`
  后自行退出；runner 等待 simulator 退出后进入 drain 阶段，继续观测直到
  CHR 1m、PM 1m、KPI 1m 的窗口物化 metrics 达到预期
  closed minutes，或超过 `FDB_BENCHMARK_DRAIN_TIMEOUT_SEC`。
- CHR/PM 模拟器最大乱序默认并封顶为 2 秒；Flink CHR/PM watermark 默认 2 秒；
  KPI 1m Full Join 默认在分钟结束后等待 10 秒，减少非业务乱序带来的等待噪声。
- 压测异常注入默认 `FDB_BENCHMARK_ANOMALY_INJECTION_RATIO=0.05`，5% cell
  和 5% user 进入确定性异常 cohort，连续产生可被现有规则检测到的异常信号。
- 单轮报告必须展示 CHR/PM/CFG 的 total、records/s、total/cell、records/s/cell。
- Latency 表拆分为 source delay、KPI availability delay、业务 sink 分支 latency，
  并单独展示 checkpoint duration、watermark lag/backlog。Iceberg/Hive 的真实
  connector commit 与 checkpoint commit 绑定，报告不再使用 Storage Visibility P95
  作为稳定性指标。
- 运行态判断优先使用 Flink REST API，结合 Observability API 的业务语义
  metrics 和 Kafka/StarRocks/Hive/Iceberg storage probes。
- benchmark-runner 默认 `FDB_BENCHMARK_DIAGNOSTIC_CHAINING=true`，提交前注入
  `FDB_FLINK_DIAGNOSTIC_CHAINING=true`，便于 Flink UI 和单轮报告把 enrichment
  及异常检测链路拆成更细 vertex；单轮报告的 Run Summary 展示“诊断拆链 ON/OFF”。
- 反压优化遵循“先定位、再扩容、再降观测开销”的顺序。诊断拆链打开时，
  enrichment、用户异常、网格异常和 KPI 关键阶段会尽量拆成独立 vertex；用户级异常
  从 `evaluation -> keyBy -> dedup` 合并为 `keyBy(imsi) -> user-event-anomaly-detect`，
  用维度级 MapState 保持连续异常语义，减少中间流膨胀。Coverage 空洞检测只在自己的
  分支先过滤低 RSRP CHR，不影响主 enriched 流继续进入用户异常和 KPI 链路。
- 压测时可以通过 `FDB_METRICS_SAMPLE_EVERY_RECORDS` 和
  `FDB_SINK_METRICS_SAMPLE_EVERY_RECORDS` 将业务延迟 probe 改为 N 条采样；采样不影响
  Flink REST 的 records、busy、反压和 checkpoint 原生指标，也不改变 sink records
  的总量统计。
- 稳定性判定输出 `stable`、`unstable`、`failed` 三类状态；上一档
  `stable` 作为该 sink 的稳定上限，首个非稳定档作为失败点。
- 报告固定写入 `benchmark-runner/output/benchmark-runs/<benchmarkId>/`，
  不使用 Docker volume 路径，也不提供输出目录配置项。
- 报告交付为 HTML-only：批次入口 `index.html`，单轮详情
  `runs/<runId>/report.html`。JSON/CSV 仅用于机器分析和复盘。
- 单轮报告中的 Flink `Records In/s` / `Records Out/s` 采用测量窗口内多次
  Flink REST 采样的非零均值；`Records In/Out Total` 明确标注为算子累计
  counter 聚合值，不作为原始业务输入量解释。
- Observability metrics 按 `stage/sink/window + subtaskIndex` 保留每个
  subtask 最新样本后再聚合，records/bytes/failures 求和，p50/p95/p99 取可用
  最大值；缺失时延显示 `N/A`，默认 `unknown` 占位指标不进入 benchmark
  单轮 latency/sink 明细。`critical` 状态优先于 `healthy` 聚合输出。
- CHR 1m、PM 1m、KPI 1m 在窗口输出后额外发布 `window-materialization`
  metrics，dataset 分别为 `chr-1m`、`pm-1m`、`kpi-1m`，`windowKind` 携带
  `MIN_1@<windowEndTs>`。探针看到下一个窗口时发布上一个窗口的最终计数，
  并通过每 2 秒低频调度刷新有变化的当前窗口计数，关闭时再 flush 最新计数，
  避免按每条记录高频发布观测指标。benchmark-runner
  使用这些样本统计真实 closed minute 数，不再用 Flink operator 累计
  `recordsOutTotal / cellLevel` 粗略推断。
- StarRocks storage probe 必须查询业务结果表行数并在报告中显示为
  `Storage Rows`；Hive/Iceberg/Kafka 的 storage probe 保持文件或 topic 口径。
- CFG 为一次性初始化源，`Source Density` 的 `records/s/cell` 显示 `N/A`，
  避免把初始化平均速率误读为持续事件流密度。
- `index.html` 集成全局结论、每个 sink 的稳定上限、失败点、横向对比、
  优化建议和所有单轮详情链接，不单独生成 recommendations 文件。
- 单轮 `report.html` 按 C 方案实现：顶部为运行状态、瓶颈原因、调优建议和
  核心指标卡；下方按运行摘要、Flink 资源、算子吞吐、时延、Sink 与存储、
  原始产物分区钻取。页面应主要使用状态 badge、指标卡和表格，避免把
  Java record `toString()` 或大段 JSON 作为主阅读内容。
- 第一版单轮详情页必须补齐 Flink REST 的 TaskManager/slot/job vertex
  明细、Observability API 的 KPI/sink/watermark 时延摘要，以及 storage
  probe 的结构化展示；dry-run 也要输出足够丰富的示例数据，避免测试报告
  看起来像空壳。
- Java tests 覆盖配置、矩阵、Flink API 解析、判定引擎、fake clients、
  JSON/CSV 和 HTML 生成。`scripts/test-benchmark-dispatch.sh` 只覆盖 shell
  启动入口，不挂入 Maven lifecycle。

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
- [ ] CFG 缺失时 enrich 主流继续输出，CFG 缺失上下文写入 `enrichment-late`，不作为业务 DLQ。
- [x] 小区异常由 `CellKpi MIN_1` 后的 `cell-kpi-anomaly-detect-dedup` 产出，连续 3 个 1 分钟周期触发，恢复前不重复输出。
- [x] 用户异常由 enrich 后的 `user-event-anomaly-detect-dedup` 产出，10 分钟内同维度连续 3 个异常事件触发，正常/成功事件打断序列。
- [x] 活动异常类型使用 `CELL_RADIO_BAD`、`CELL_SERVICE_BAD`、`USER_FAILURE`、`USER_QOE_BAD`、`COVERAGE_HOLE`。
- [x] `FDB_RESULT_SINK=starrocks|iceberg|hive|kafka|none` 时，业务结果 sink 每次只创建一种分支。
- [x] KPI 1m、KPI 5m、小区异常、用户异常、栅格异常均跟随 `FDB_RESULT_SINK` 写入对应 StarRocks/Iceberg/Hive/Kafka 目标。
- [x] StarRocks/Iceberg/Hive/Kafka 均具备 `cell_anomaly_events`、`user_anomaly_events`、`grid_anomaly_events` 三类异常输出。
- [x] `FDB_RESULT_SINK=none` 时不创建业务结果 sink，但计算链路可运行。
- [x] `FDB_DLQ_ENABLED=false` 时不创建 DLQ sink；开启时 DLQ 仅作为 Kafka 兜底链路。
- [x] 控制台提供 KPI 1m、KPI 5m、小区异常、用户异常、栅格异常、Sink 耗时页面。
- [x] 流处理总览页展示当前 run、result sink、metrics、DLQ、parallelism、checkpoint、job status 和 report 状态。
- [x] 流处理总览页只显示当前真实启用的 sink 节点，并展示瓶颈候选摘要。
- [x] 栅格异常至少有表格展示；GIS 展示可用时显示地图或栅格。
- [x] 每个 sink 分支记录最近一次耗时、p50/p95/p99、记录数、字节数、失败数。
- [ ] Hive/Iceberg 展示 checkpoint/snapshot commit 耗时、文件数、平均文件大小和小文件数量。
- [x] Hive/Iceberg result sink 默认 checkpoint interval 为 30s，配置值不超过 180s。
- [x] Observability API 将 metrics 写入本地 JSONL 历史，并能按 run 生成压测报告。
- [ ] Kafka、StarRocks、Hive、Iceberg 数据按 1 小时和 10GB 上限治理。
- [x] Flink 默认并发与 TaskManager slots 为 4。
- [x] KPI window 与重 sink 在 Flink UI 中拆成可辨识 vertex。
- [x] `../shared-data-infra` 能提供 Kafka/ZooKeeper/Hive/HDFS/StarRocks，本工程不重复定义这些基础设施。
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

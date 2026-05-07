# Flink 数据均衡处理工程 — 设计文档

- **创建日期**: 2026-04-29
- **状态**: 待评审
- **关键技术栈**: Java 21 · Flink 1.20 · Maven · Avro · Kafka · MySQL · Hive (HMS) · Grafana

---

## 1. 目标与非目标

### 1.1 目标

构建一个 Flink 工程，处理基站小区产生的用户级 CHR 数据，并结合**话统数据 (MR)** 与**配置数据 (CM)** 做实时分析。核心交付：

1. **三个数据源的模拟器**（合并为单 jar 多模式）：CHR、MR、CM
2. **主拓扑发布服务**：发布站点/小区拓扑，所有模拟器订阅
3. **Flink 作业**：完成 CHR + MR + CM 的实时关联，输出
   - 用户级**异常事件**（通过规则集检测）
   - 小区级 **KPI 聚合**（参数化窗口，默认 1 分钟 + 5 分钟）
4. **负载均衡机制**：在保留"同站点亲和"前提下，应对静态、时段漂移、突发热点叠加的负载倾斜
5. **状态管理**：周期性导出/加载，支撑重平衡迁移与长周期持久化
6. **下游 Sink**：MySQL（主路径）+ Hive（flink-connector-hive）；StarRocks 作为预留扩展
7. **本地开发环境**：Windows + Git Bash + Docker Desktop，端到端可跑

### 1.2 非目标

- 不实现完整的 5G/6G 协议栈，CHR/MR/CM 字段语义贴近真实电信场景但不强制工业标准
- 不引入 Confluent Schema Registry（schema 通过 jar 包同步）
- 不实现真实的 K8s/YARN 部署清单（项目结构预留，但本期只覆盖本地开发）
- 不实现 ML 模型类异常检测（只做规则集，留作扩展）

### 1.3 关键设计原则

- **YAGNI**：先实现可证明价值的 v1，复杂能力（如显式状态搬迁、ML 异常检测）作为扩展点列出
- **小模块、清晰边界**：模拟器、拓扑服务、Flink 作业职责互不交叉
- **倾斜可观察、可调试**：路由表用 CSV 存储和导出；Grafana dashboard 开箱呈现倾斜度
- **本地优先，生产可演进**：所有外部依赖（Kafka/MySQL/HMS）在 Windows 本地通过 docker-compose 可起；代码侧不写死本地路径

---

## 2. 整体架构

### 2.1 数据流总览

```
┌──────────────────┐                                    
│ topology-service │ ──┐ topology (compact)              
└──────────────────┘   │                                 
                       ▼                                 
┌──────────────────────────────────────────────────────┐
│          Kafka topology / 三流业务 topic              │
└──────────────────────────────────────────────────────┘
        ▲           ▲            ▲                       
        │           │            │                       
   ┌────────┐  ┌────────┐  ┌────────┐                   
   │simChr  │  │simMr   │  │simCm   │ (单 jar 多模式)    
   └────────┘  └────────┘  └────────┘                   
                                                         
        chr-events     mr-stats     cm-config            
            ▼              ▼            ▼                
   ┌──────────────────────────────────────────┐         
   │          Flink Job (主作业)               │         
   │                                           │         
   │  Source → VBucketAssigner (broadcast 监听)│         
   │       → keyBy(VBucketId)                  │         
   │       → EnrichmentProcessFunction (合一)  │         
   │            ├─ Anomaly (rekey cellId)      │         
   │            ├─ KPI 1m / 5m (rekey cellId)  │         
   │            └─ Heartbeat (side output)     │         
   │  Coordinator (parallelism=1)              │         
   │       reads lb-heartbeat                  │         
   │       writes lb-routing (CSV) + snapshot │         
   │  状态导出器 (5min 周期, file:// hdfs://)   │         
   └──────────────────────────────────────────┘         
            │              │            │                
            ▼              ▼            ▼                
       anomaly-events  cell-kpi-1m  cell-kpi-5m          
            │              │            │                
            └──────┬───────┴────────────┘                
                   ▼                                     
        ┌──────────┴──────────┐                          
        ▼                     ▼                          
    MySQL Sink            Hive Sink (HMS, Parquet)       
    (jdbc, idempotent)    (按 dt/window_kind/hour 分区)   
                                                         
     [扩展] StarRocks Routine Load 直接消费 Kafka 输出 topic
```

### 2.2 模块划分

```
flink-data-balance/
├── pom.xml                            # parent BOM
├── common/                            # Avro schemas, POJOs, geo, kafka serde, config
├── topology-service/                  # 主拓扑发布服务 (独立 JVM)
├── simulator/                         # CHR/MR/CM 三模式合一的模拟器
├── flink-job/                         # Flink 主作业
├── docker/                            # docker-compose: Kafka + MySQL + HMS + Postgres
├── scripts/                           # 启动 / DDL / 工具脚本
└── docs/                              # 设计文档 + Grafana dashboard JSON
```

---

## 3. 数据模型 (Avro)

所有 schema 位于 `common/src/main/avro/`，用 `avro-maven-plugin` 生成 SpecificRecord。

### 3.1 ChrEvent — 用户级 CHR 事件

| 字段 | 类型 | 说明 |
|---|---|---|
| `chrId` | string | UUID |
| `eventTs` | long (timestamp-millis) | 事件时间 |
| `imsi` | string | 用户标识 |
| `imei` | string? | 设备标识 |
| `siteId` / `cellId` | string | 站点 / 小区 |
| `eventType` | enum | ATTACH/DETACH/HANDOVER/DATA_SESSION/VOICE_CALL/RRC_SETUP_FAIL/SERVICE_REQUEST/PAGING |
| `ratType` | enum | LTE / NR_NSA / NR_SA / ENDC |
| `pci` | int | 物理小区 ID |
| `qci` | int? | LTE QoS 类（4G）/ 5QI（5G） |
| `tac` / `eci` | int / long | TAC / ECI |
| `mcc` / `mnc` | string | MCC/MNC |
| `arfcn` | int? | 频点号 |
| `nssaiSst` / `nssaiSd` | int? / string? | 5G 切片 |
| `bearerType` | enum? | DEFAULT/DEDICATED |
| `durationMs` | long? | 会话持续时长 |
| `bytesUp` / `bytesDown` | long? | 上下行字节 |
| `latencyMs` | float? | 端到端时延 |
| `rsrp` / `rsrq` / `sinr` | float? | 无线信号 |
| `cqi` / `mcs` | int? | 信道质量 / 调制编码 |
| `bler` | float? | 块错误率 |
| `timingAdvance` | int? | TA |
| `resultCode` | int | 0=成功，非零=失败原因 |
| `latitude` / `longitude` | double | 用户经纬度 |
| `gridId` | string? | Geohash（由 Flink 派生写入下游 schema，源端可空） |

### 3.2 MrStat — 话统记录（每小区每 10 秒一条）

| 字段 | 类型 | 说明 |
|---|---|---|
| `siteId` / `cellId` | string | |
| `windowStartTs` / `windowEndTs` | long | 10 秒窗口边界 |
| `prbUsageDl` / `prbUsageUl` | float (0–1) | 资源块使用率 |
| `activeUsers` | int | 活跃用户数 |
| `avgRsrp` / `avgRsrq` / `avgSinr` | float | |
| `avgCqi` / `avgMcs` / `avgBler` | float | |
| `throughputDlMbps` / `throughputUlMbps` | float | |
| `droppedConnections` | int | |
| `handoverSuccess` / `handoverFailure` | int | |
| `prachAttempt` / `prachFailure` | int | 随机接入计数 |
| `rrcEstabAttempt` / `rrcEstabSuccess` | int | RRC 建立 |
| `avgLatencyMs` | float | |
| `packetLossRate` | float | |
| `numerology` | int? | 5G 子载波间隔参数 |

### 3.3 CmConfig — 配置数据（按需更新）

| 字段 | 类型 | 说明 |
|---|---|---|
| `siteId` / `cellId` | string | |
| `effectiveTs` | long | 生效时间 |
| `version` | long | 单调递增 |
| `cellType` | enum | LTE / NR_NSA / NR_SA |
| `bandwidthMhz` | int | |
| `frequencyBand` / `arfcn` | string / int | |
| `maxPowerDbm` | float | |
| `azimuth` | int (0–359) | |
| `centerLat` / `centerLon` | double | 小区中心 |
| `coverageRadiusM` | int | |
| `pci` | int | |
| `tac` / `eci` | int / long | |
| `mcc` / `mnc` | string | |
| `numerology` | int? | |
| `mimoMode` | enum? | SISO / MIMO_2x2 / MIMO_4x4 / MIMO_8x8 |
| `antennaPorts` | int | |
| `nssai` | array<record{sst, sd}> | 5G 切片 |
| `tddSubFrameAssignment` | int? | TDD 子帧配置 |
| `referenceSignalPower` | float? | |
| `neighborCells` | array<string> | 邻区 |
| `tombstone` | boolean (default false) | 软删除 |

### 3.4 AnomalyEvent — 异常事件（输出）

| 字段 | 类型 | 说明 |
|---|---|---|
| `detectionTs` | long | 检测时刻 |
| `eventTs` | long | 触发的 CHR 事件时间 |
| `imsi` | string | |
| `siteId` / `cellId` | string | |
| `gridId` | string | Geohash level 7 |
| `latitude` / `longitude` | double | |
| `anomalyType` | enum | LOW_SIGNAL / ATTACH_FAILURE_BURST / HANDOVER_FAIL_PATTERN / CONFIG_MISMATCH / COVERAGE_HOLE / (扩展) |
| `severity` | enum | LOW / MEDIUM / HIGH |
| `ruleVersion` | string | 规则版本号 |
| `contextJson` | string | 触发时刻 MR/CM 关键字段快照 |

### 3.5 CellKpi — 小区级 KPI（输出）

| 字段 | 类型 | 说明 |
|---|---|---|
| `windowStartTs` / `windowEndTs` | long | |
| `windowKind` | enum | MIN_1 / MIN_5 / MIN_15 / HOUR_1（参数化） |
| `siteId` / `cellId` | string | |
| `gridId` | string | 小区中心点 Geohash |
| `numChrEvents` | long | |
| `numUsers` | long | 唯一用户数（HLL 近似） |
| `avgRsrp` / `avgSinr` | float | |
| `avgPrbUsageDl` | float | |
| `throughputDlMbpsAvg` | float | |
| `dropRate` | float | droppedConnections / totalConnections |
| `hoSuccessRate` | float | |
| `attachSuccessRate` | float | |

### 3.6 Avro <-> Hive/MySQL 兼容性约束

- 时间戳统一用 `long` (epoch ms)，避免 Avro logical type 在 Hive 旧版本兼容问题
- 不使用 Avro `map` 类型（Hive 表达不直观），改用 `array<record{key, value}>`
- 可空字段用 `union { null, T }` 且 default 为 null
- 嵌套层级 ≤ 2 层

---

## 4. Kafka Topic 全景

| Topic | 分区 | 清理 | Key | 用途 |
|---|---:|---|---|---|
| `chr-events` | 64 | delete 7d | siteId | CHR 事件主流 |
| `mr-stats` | 16 | delete 3d | siteId | 话统 10 秒打点 |
| `cm-config` | 8 | **compact** | cellId | 配置数据 |
| `topology` | 4 | **compact** | siteId | 主拓扑 |
| `lb-heartbeat` | 1 | delete 1h | subtaskId | 子任务负载心跳 |
| `lb-routing` | 1 | **compact** | siteId | 路由表（CSV value） |
| `anomaly-events` | 16 | delete 7d | cellId | 异常输出 |
| `cell-kpi-1m` | 8 | delete 3d | cellId | KPI 1 分钟 |
| `cell-kpi-5m` | 8 | delete 7d | cellId | KPI 5 分钟 |
| `chr-dlq` / `mr-dlq` / `cm-dlq` / `enrichment-late` | 各 4 | delete 7d | — | 死信 / 迟到 |

本地副本因子 = 1，分区数偏小（用于演示）；生产环境按吞吐重新规划。Topic 创建脚本 `scripts/create-kafka-topics.sh` 提供。

---

## 5. 拓扑服务 (`topology-service/`)

### 5.1 职责

- 启动时根据 `topology.yaml` 生成确定性的站点-小区拓扑
- 全量发布到 Kafka `topology` topic（compact）
- 后续可发布增量（新增/下线站点）
- 可选 HTTP `/topology` 端点供调试

### 5.2 `topology.yaml` 关键结构

```yaml
seed: 42
sites:
  count: 3000
  cellsPerSite: { min: 3, max: 9 }
  region:
    latRange: [39.7, 40.2]
    lonRange: [116.0, 116.8]
  hotZones:
    - name: zone-cbd-1
      center: [39.91, 116.40]
      radiusKm: 3
      siteWeightMultiplier: 5.0
    - name: zone-res-1
      center: [40.05, 116.30]
      radiusKm: 4
      siteWeightMultiplier: 2.5

cellDefaults:
  cellType: NR_SA
  bandwidthMhzCandidates: [20, 40, 100]
  frequencyBands: ["n78", "n41", "n28"]
  maxPowerDbm: 49.0
  numerology: 1
  mimoMode: MIMO_4x4
```

### 5.3 关键实现

- `TopologyGenerator`：基于 `seed` 派生站点位置（按热点权重做拒绝采样）、PCI/TAC/ECI/邻区
- `KafkaTopologyPublisher`：消息体 = Avro Topology record；Kafka key = siteId
- `TopologyHttpServer`（可选）：基于 jdk.httpserver，单端口暴露 GET `/topology`

---

## 6. 模拟器 (`simulator/`)

### 6.1 模式分发

```bash
java -jar simulator.jar chr --config sim-chr.yaml
java -jar simulator.jar mr  --config sim-mr.yaml
java -jar simulator.jar cm  --config sim-cm.yaml
```

`SimulatorMain` 解析子命令分发到 `ChrSimulator` / `MrSimulator` / `CmSimulator`。

### 6.2 公共能力（共享代码 `simulator/common/`）

- `TopologyClient`：消费 `topology` topic，建立本地缓存（站点/小区/经纬度/PCI/...）
- `KafkaPublisher`：Avro 序列化 + idempotent producer
- `RateController`：按目标 EPS 用 token bucket 控制发送速率

### 6.3 ChrSimulator

#### 倾斜模型（四个开关独立）

```yaml
skewProfile:
  static:    { enabled: true }                        # 来自 topology hotZones
  diurnal:                                            # 时段漂移
    enabled: true
    rushHourMultipliers:
      "07:00-09:30": 2.5
      "17:00-20:00": 3.0
      "00:00-05:00": 0.3
    geographicShift:
      cbdHotZoneNames: ["zone-cbd-1"]
      residentialHotZoneNames: ["zone-res-1"]
      cbdPeak: "10:00-18:00"
      residentialPeak: "19:00-23:00"
  burst:
    enabled: true
    events:
      - triggerRate: 0.001       # 每秒触发概率
        durationMin: [10, 30]
        radiusKm: 1.0
        siteMultiplier: [10, 50]
  noise:     { enabled: true, stdRatio: 0.15 }
```

#### 速率与生成

- 每秒为每个小区计算瞬时强度 `λ(cellId, t) = baseλ × static × diurnal × burst × noise`
- Poisson 过程生成事件
- `eventDistribution` 决定事件类型分布（DATA_SESSION 占多数）
- `userPool` 维护 IMSI 池 + 简单 random-waypoint 漂移
- `signalModel` 根据用户与小区中心的距离派生 rsrp/sinr/cqi（边缘用户信号差，更易触发异常）
- `outOfOrderMaxLagMs` 制造乱序，测试 watermark

#### 模式

- `mode: generate`：默认，按上述模型生成
- `mode: replay`：从 Avro 文件回放（参数：`replay.path`、`replay.timeScale`、`replay.loop`）
- `mode: mixed`：先回放固定一段，再切到 generate

#### Kafka 写入

- topic = `chr-events`
- **key = siteId**（让 Kafka 端就具备初步亲和）
- value = Avro binary

### 6.4 MrSimulator

- 每 10 秒为所有 cell 生成一条 `MrStat`，墙钟对齐到 10 秒整数倍
- 与 CHR 共享 `skewProfile` 计算 `λ`，把 `prbUsage / activeUsers / throughput` 设为 `λ` 的函数 + 噪声
- 滞后 1 个窗口（模拟统计延迟）
- topic = `mr-stats`，key = siteId

### 6.5 CmSimulator

- 启动时按 `topology` 全量发布 baseline `CmConfig`（`version=1`）
- 运行中按 `updates.intervalMin` 间隔随机挑 `changeRate` 比例的 cell 改一两个字段，`version++`
- 偶发 tombstone 测试软删除
- topic = `cm-config`（compact），key = cellId

---

## 7. Flink 作业 (`flink-job/`)

### 7.1 时间语义

- **Event time**，watermark 基于 CHR `eventTs`
- 策略：`forBoundedOutOfOrderness(Duration.ofSeconds(20))`（容忍 20 秒乱序）
- 空闲 source 检测：`withIdleness(Duration.ofMinutes(1))`

### 7.2 两层分桶 + Coordinator

#### L1（稳定层）：siteId → VBucketId

- `NUM_VBUCKETS = 1024`
- `VBucketId = (consistentHash(siteId) ⊕ slotShift(siteId)) mod 1024`
- `slotShift(siteId)` 默认 0；Coordinator 通过下发 `lb-routing` 修改它
- 大部分 siteId 的 L1 映射保持不变；仅热点站点收到 shift

#### L2（路由层）：VBucketId → Subtask

- 由 Flink 标准 keyBy 哈希决定 (`KeyGroupRangeAssignment`)，不去对抗框架

#### Coordinator 算子

```
┌─────────────────────────────────────────────────────┐
│ LoadCoordinator (parallelism=1)                      │
│                                                       │
│   Input 1: KafkaSource lb-heartbeat                  │
│            (subtaskId, eps, vbucketEps[N], ts)       │
│   Input 2: 自身定时器 (每 10s 评估)                    │
│                                                       │
│   State (operator state):                             │
│     - latestHeartbeats: Map<subtaskId, Heartbeat>    │
│     - currentRouting:   Map<siteId, slotShift>       │
│                                                       │
│   决策逻辑 (RebalancePolicy):                          │
│     当某 subtask EPS > 1.5× 中位数 持续 60s:           │
│       - 找出该 subtask 上 EPS top-3 的 siteId          │
│       - 为它们计算新 slotShift, 目标 = 当前最空闲 subtask│
│       - 仅在 5min 边界生效                              │
│     输出:                                              │
│       - Kafka lb-routing (CSV)                        │
│       - 文件 routing-snapshot.csv (全量, 周期写)        │
└─────────────────────────────────────────────────────┘
```

#### 路由控制流（避免 Flink DAG 内部环）

- Workers → side output → KafkaSink → topic `lb-heartbeat` → KafkaSource → Coordinator
- Coordinator → KafkaSink → topic `lb-routing` → KafkaSource (broadcast) → 所有 worker

#### routing CSV 格式

```
siteId,vbucketId,slotShift,assignedSubtask,routingVersion,decisionTs
SITE-000123,17,0,5,42,1714387200000
SITE-007890,1003,8,12,42,1714387200000
...
```

### 7.3 三流合并（envelope 模式）

```java
sealed interface InputEnvelope {
    long ts();
    int vbucketId();
}
record ChrEnv(long ts, int vb, ChrEvent payload) implements InputEnvelope {}
record MrEnv (long ts, int vb, MrStat payload)   implements InputEnvelope {}
record CmEnv (long ts, int vb, CmConfig payload) implements InputEnvelope {}

DataStream<InputEnvelope> merged = chrTagged
    .union(mrTagged, cmTagged)
    .keyBy(InputEnvelope::vbucketId);

merged.process(new EnrichmentProcessFunction()).name("enrichment");
```

### 7.4 EnrichmentProcessFunction

#### Keyed State（per VBucketId）

| State 名 | Key | 类型 | TTL | 内容 |
|---|---|---|---|---|
| `cmState` | (VBucketId, cellId) | ValueState\<CmConfig\> | 无 | 最新生效配置 |
| `mrRing` | (VBucketId, cellId) | ListState\<MrStat\> | 5min idle | 最近 6 个 10s 窗口 |
| `userCtx` | (VBucketId, imsi) | ValueState\<UserCtx\> | 30min idle | 用户最近会话上下文 |
| `bufferState` | (VBucketId, cellId) | ListState\<ChrEvent\> | 30s（最大 buffer） | CM 未到时短期缓冲；超时丢 dlq |
| `routingTable` | broadcast | MapState\<siteId, slotShift\> | 无 | 当前路由 |

#### 处理逻辑

```
processElement(env):
    switch (env):
        case ChrEnv(_, _, chr):
            cm = cmState.get((vb, chr.cellId))
            mr = mrRing.latest((vb, chr.cellId))
            if (cm == null) {
                bufferState.add(chr)               // 短期缓冲
                if (bufferAge > 30s) -> dlq
                return
            }
            enriched = enrich(chr, cm, mr)
            enriched.gridId = Geohash.encode(chr.lat, chr.lon, level=7)
            out.collect(enriched)
            
        case MrEnv(_, _, mr):
            mrRing.add((vb, mr.cellId), mr)
            mrRing.evictOldest((vb, mr.cellId))
            
        case CmEnv(_, _, cm):
            if (cm.tombstone) cmState.clear((vb, cm.cellId))
            else if (cm.version > existing.version) cmState.update((vb, cm.cellId), cm)
            // 唤醒等待 cm 的 buffered chr
            flushBuffered((vb, cm.cellId))

onTimer(heartbeatTimer, every 5s):
    payload = HeartbeatPayload(subtaskId, recentEps, vbucketEps, ts)
    sideOutput(HEARTBEAT_TAG, payload)

onTimer(snapshotTimer, every 5min boundary):
    triggerStateSnapshot(this.subtaskId)
```

### 7.5 状态周期性导出 / 加载

#### 导出（每 5 分钟边界）

```
for each VBucketId v owned by this subtask:
    cmDump   = serialize(cmState   filtered by v)
    mrDump   = serialize(mrRing    filtered by v)
    userDump = serialize(userCtx   filtered by v)
    writeAvro(<state-root>/vbucket=v/cm-state/   <ts>.avro,   cmDump)
    writeAvro(<state-root>/vbucket=v/mr-ring/    <ts>.avro,   mrDump)
    writeAvro(<state-root>/vbucket=v/user-ctx/   <ts>.avro,   userDump)
    writeMeta (<state-root>/vbucket=v/_LATEST,    <ts>)
```

- 异步执行，不阻塞主流
- 失败 metric `state.snapshot.failures`，重试 3 次

#### 加载（启动 / 重平衡时）

```
for each newly assigned VBucketId v:
    latestTs = readMeta(<state-root>/vbucket=v/_LATEST)
    if (latestTs exists):
        load cmState, mrRing, userCtx from snapshot files
    else:
        cmState ← 重读 cm-config topic from beginning (compact, 全量)
        mrRing  ← 订阅 mr-stats earliest+30s lookback (预热)
        userCtx ← 留空 (会话级状态)
```

#### 存储布局

```
<state-root>/
├── vbucket=017/
│   ├── _LATEST                    # 文本: "2026-04-29T10-30-00"
│   ├── cm-state/<ts>.avro
│   ├── mr-ring/<ts>.avro
│   └── user-ctx/<ts>.avro
└── _global/routing-snapshot.csv
```

- 保留每个 VBucket 最近 24 个快照（约 2 小时）
- `<state-root>` 可配 `file:///` `hdfs:///` `s3://`
- 周期性持久化的双重价值：(1) 重平衡迁移 (2) 长周期审计 / 离线分析

### 7.6 异常检测算子

```
EnrichedChr ─keyBy(cellId)─▶ AnomalyDetector ─▶ AnomalyEvent
```

#### v1 规则集（5 条）

| 规则 ID | 类型 | 触发条件 | severity |
|---|---|---|---|
| `LOW_SIGNAL` | 单事件 | rsrp < -110 dBm 或 sinr < -3 dB | LOW |
| `ATTACH_FAILURE_BURST` | 滑动窗口 | 同 cellId 在 1min 内 attach 失败 ≥ 10 | HIGH |
| `HANDOVER_FAIL_PATTERN` | 滑动窗口 | 同 cellId 在 5min 内 HO 失败率 > 30% | MEDIUM |
| `CONFIG_MISMATCH` | 跨数据源 | CHR 上报 PCI/TAC 与 CmConfig 不一致 | HIGH |
| `COVERAGE_HOLE` | 空间聚合 | 同 gridId 内 5min 内低信号事件 ≥ 50 | MEDIUM |

- 实现：`KeyedProcessFunction`，规则状态用 `MapState<RuleId, RuleState>`，定时器驱动滑窗
- 阈值通过配置文件 hot-reload（监控配置文件 mtime）
- `Rule` 接口：`Optional<AnomalyEvent> evaluate(EnrichedChr, KeyedState)`

### 7.7 KPI 聚合算子

```
EnrichedChr ─keyBy(cellId)─▶
   ├─ window(Tumbling 1m).aggregate(KpiAccumulator) ─▶ cell-kpi-1m
   └─ window(Tumbling 5m).aggregate(KpiAccumulator) ─▶ cell-kpi-5m
```

- `KpiAccumulator`：事件数、唯一用户数（HLL 近似，约 1.5% 误差）、avg_rsrp、avg_sinr、avg_prb_usage_dl、throughput_avg、drop_rate、ho_success_rate、attach_success_rate
- 窗口可参数化：`--kpi-windows=1m,5m,15m,1h`

### 7.8 Sink 抽象

```java
interface WarehouseSink {
    SinkFunction<AnomalyEvent> anomalySink(SinkContext ctx);
    SinkFunction<CellKpi>      kpiSink(SinkContext ctx);
}

class MysqlWarehouseSink implements WarehouseSink { ... }       // v1 主路径
class StarRocksWarehouseSink implements WarehouseSink { ... }   // 骨架, 留空
```

通过配置 `warehouse.type=mysql|starrocks` 切换。

#### MySQL Sink

- 用 `flink-connector-jdbc` 批量写入
- 唯一约束：
  - `anomaly_events`: `UNIQUE (detection_ts, site_id, cell_id, anomaly_type, imsi)`
  - `cell_kpi`: `UNIQUE (window_start_ts, window_kind, site_id, cell_id)`
- 用 `INSERT ... ON DUPLICATE KEY UPDATE` 保证幂等
- 批量大小：500 行 或 1 秒，取先到

#### Hive Sink

- 用 `flink-connector-hive` + HiveCatalog
- HMS 地址通过配置注入
- Parquet 格式 + Snappy 压缩
- 分区：
  - `anomaly_events`: `dt, hour`
  - `cell_kpi`: `dt, window_kind, hour`
- 滚动策略：每 128MB 或 5min 触发，避免小文件
- 由 Flink checkpoint 触发分区 commit

#### StarRocks（预留扩展）

- DDL 与 Routine Load 任务定义放在 `scripts/starrocks-*.sql`
- 切换 `warehouse-type=starrocks` 时，DDL 已就绪；本期不验证

---

## 8. MySQL Schema

### 8.1 anomaly_events

```sql
CREATE TABLE anomaly_events (
  id            BIGINT AUTO_INCREMENT PRIMARY KEY,
  detection_ts  BIGINT NOT NULL,
  event_ts      BIGINT NOT NULL,
  imsi          VARCHAR(32) NOT NULL,
  site_id       VARCHAR(64) NOT NULL,
  cell_id       VARCHAR(64) NOT NULL,
  grid_id       VARCHAR(16),
  latitude      DOUBLE,
  longitude     DOUBLE,
  anomaly_type  VARCHAR(32) NOT NULL,
  severity      VARCHAR(8),
  rule_version  VARCHAR(32),
  context_json  TEXT,
  UNIQUE KEY uk_anomaly (detection_ts, site_id, cell_id, anomaly_type, imsi),
  KEY idx_event_ts (event_ts),
  KEY idx_cell (cell_id, detection_ts)
) ENGINE=InnoDB CHARSET=utf8mb4;
```

### 8.2 cell_kpi

```sql
CREATE TABLE cell_kpi (
  id              BIGINT AUTO_INCREMENT PRIMARY KEY,
  window_start_ts BIGINT NOT NULL,
  window_end_ts   BIGINT NOT NULL,
  window_kind     VARCHAR(8) NOT NULL,
  site_id         VARCHAR(64) NOT NULL,
  cell_id         VARCHAR(64) NOT NULL,
  grid_id         VARCHAR(16),
  num_chr_events  BIGINT,
  num_users       BIGINT,
  avg_rsrp        FLOAT,
  avg_sinr        FLOAT,
  avg_prb_usage_dl FLOAT,
  throughput_dl_mbps_avg FLOAT,
  drop_rate       FLOAT,
  ho_success_rate FLOAT,
  attach_success_rate FLOAT,
  UNIQUE KEY uk_kpi (window_start_ts, window_kind, site_id, cell_id),
  KEY idx_cell_window (cell_id, window_kind, window_start_ts)
) ENGINE=InnoDB CHARSET=utf8mb4;
```

---

## 9. Hive Schema

```sql
CREATE EXTERNAL TABLE anomaly_events (
  detection_ts  BIGINT,
  event_ts      BIGINT,
  imsi          STRING,
  site_id       STRING,
  cell_id       STRING,
  grid_id       STRING,
  latitude      DOUBLE,
  longitude     DOUBLE,
  anomaly_type  STRING,
  severity      STRING,
  rule_version  STRING,
  context_json  STRING
)
PARTITIONED BY (dt STRING, hour STRING)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY')
LOCATION '<warehouse>/anomaly_events/';

CREATE EXTERNAL TABLE cell_kpi (
  window_start_ts BIGINT,
  window_end_ts   BIGINT,
  site_id         STRING,
  cell_id         STRING,
  grid_id         STRING,
  num_chr_events  BIGINT,
  num_users       BIGINT,
  avg_rsrp        FLOAT,
  avg_sinr        FLOAT,
  avg_prb_usage_dl FLOAT,
  throughput_dl_mbps_avg FLOAT,
  drop_rate       FLOAT,
  ho_success_rate FLOAT,
  attach_success_rate FLOAT
)
PARTITIONED BY (dt STRING, window_kind STRING, hour STRING)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY')
LOCATION '<warehouse>/cell_kpi/';
```

---

## 10. 配置管理

### 10.1 三层合并优先级

```
默认配置 (jar 内 resources/<module>-default.yaml)
   ↑ 被覆盖
yaml 文件 (启动参数 --config <path>)
   ↑ 被覆盖
环境变量 (FDB_*)
```

### 10.2 关键启动参数

| 参数 | 默认 | 说明 |
|---|---|---|
| `--kpi-windows` | `1m,5m` | 窗口列表 |
| `--rebalance-threshold` | `1.5` | 过载阈值（× 中位数） |
| `--rebalance-window` | `60s` | 过载持续判定窗口 |
| `--vbucket-count` | `1024` | VBucket 数 |
| `--state-store` | `file:///tmp/fdb-state` | 状态存储根 |
| `--warehouse-type` | `mysql` | mysql / starrocks |
| `--watermark-lag` | `20s` | 乱序容忍 |
| `--rule-config` | `rules.yaml` | 规则阈值配置 |

### 10.3 关键环境变量

```
FDB_KAFKA_BOOTSTRAP=localhost:9092
FDB_MYSQL_URL=jdbc:mysql://localhost:3306/fdb
FDB_MYSQL_USER=fdb / FDB_MYSQL_PASSWORD=...
FDB_HMS_URI=thrift://localhost:9083
FDB_STATE_ROOT=file:///tmp/fdb-state
```

---

## 11. 容错与一致性

| 组件 | 一致性 | 机制 |
|---|---|---|
| Flink Checkpoint | exactly-once | incremental RocksDB, 间隔 60s, 超时 10min, 保留 3 |
| Kafka Source | exactly-once | offset 由 checkpoint 管理 |
| Kafka Sink | exactly-once | transactional |
| MySQL Sink | at-least-once + 业务幂等 | UNIQUE KEY + ON DUPLICATE KEY UPDATE |
| Hive Sink | exactly-once (per partition) | checkpoint 触发分区 commit |
| 状态导出 | best-effort | 失败 metric + 重试 3 次, 不阻塞主流 |

### DLQ

- `chr-dlq` / `mr-dlq` / `cm-dlq`：序列化错误、字段非法、CM 长缺失（>30s）
- `enrichment-late`：watermark 之后到达
- 死信消息保留原始字节 + 错误原因 JSON

---

## 12. 可观测性

### 12.1 Metrics（Flink → Prometheus）

```
enrichment.events.{in,enriched,dlq}        counter
enrichment.cmstate.cells                    gauge
enrichment.userctx.imsi                     gauge
enrichment.buffered.waiting_cm              gauge
coordinator.subtask.eps[N]                  gauge per subtask
coordinator.subtask.imbalance               gauge (max/median)
coordinator.rebalance.count                 counter
coordinator.routing.entries.dirty           gauge
state.snapshot.{count,duration_ms,bytes}    histogram
state.snapshot.failures                     counter
kpi.window.late                             counter
anomaly.<type>.count                        counter per rule
```

### 12.2 Logging

- SLF4J + logback
- INFO：路由变更、状态快照完成、规则命中样本（采样 1/100）
- WARN：死信、迟到事件、CM 缺失
- ERROR：序列化失败、外部依赖失败

### 12.3 Grafana Dashboard

提供 `docs/grafana-dashboard.json`，开箱包含面板：
- EPS 总览 + 各 subtask EPS 时间序列
- 倾斜度（max EPS / median EPS）
- 重平衡次数 + 路由表 dirty 项
- enrichment 延迟分布（p50/p95/p99）
- 异常事件按类型计数 / 各 severity 分布
- KPI 窗口处理延迟
- 状态快照耗时与体积

---

## 13. 测试策略

| 层级 | 范围 | 工具 |
|---|---|---|
| Unit | 规则、Geohash、SkewModel、Avro 序列化、配置加载、ConsistentHash | JUnit5 + AssertJ |
| Component | 模拟器各模式 dry-run（不写 Kafka）、规则集 fixture 测试 | JUnit5 |
| Integration | Embedded Kafka + MiniCluster Flink，端到端少量事件 | Testcontainers + flink-test-utils |
| Skew injection | 启用 burst skewProfile，断言 Coordinator 在 N 分钟内输出新路由 | Integration |
| Replay | 录制 chr-events 文件，replay 模式回放，校验幂等性 | Integration |
| State migration | 手动触发路由变更，验证新 subtask 加载快照后处理无缝 | Integration |

---

## 14. 本地开发环境

### 14.1 平台说明

- **目标平台**：Windows 11 + Git Bash + Docker Desktop
- 已验证可行：docker-compose 在 Windows + Docker Desktop（WSL2 backend）下完全可用，Git Bash 直接调用 `docker` / `docker compose`
- Windows 路径在 docker volume bind 时使用正斜杠（`./docker/data`），避免 `C:\` 形式

### 14.2 docker-compose 服务（`docker/docker-compose.yml`）

| 服务 | 镜像（指示） | 端口 | 用途 |
|---|---|---|---|
| zookeeper | confluentinc/cp-zookeeper | 2181 | Kafka 协调 |
| kafka | confluentinc/cp-kafka | 9092 | 消息总线 |
| kafka-ui | provectuslabs/kafka-ui | 8080 | 浏览器看 topic |
| mysql | mysql:8 | 3306 | 数仓 sink |
| hms-postgres | postgres:14 | 5432 | HMS 元数据后端 |
| hive-metastore | apache/hive:standalone-metastore | 9083 | Hive 元数据服务 |

### 14.3 Native Fallback（如不便用 Docker）

- **Kafka**：可下载 Apache Kafka tar，用 `bin/windows/*.bat` 启动；或 WSL2 中本机起
- **MySQL**：MySQL 官方 Windows 安装包
- **HMS**：在 Windows 上原生跑非常困难，**强烈建议 Docker**
- **Postgres**：原生安装包亦可

### 14.4 启动流程

```bash
# 0. 起依赖
./scripts/dev-up.sh                          # docker compose up -d

# 1. 建 Kafka topic + DDL
./scripts/create-kafka-topics.sh
mysql -h localhost -ufdb -p < scripts/init-mysql.sql
# Hive 表通过 Flink HiveCatalog 自动注册, 或手动:
# hive -f scripts/init-hive.sql

# 2. 启动应用 (顺序非强制, 但建议)
java -jar topology-service/target/topology-service.jar &
java -jar simulator/target/simulator.jar cm  --config simulator/conf/sim-cm.yaml &
java -jar simulator/target/simulator.jar mr  --config simulator/conf/sim-mr.yaml &
java -jar simulator/target/simulator.jar chr --config simulator/conf/sim-chr.yaml &
flink run flink-job/target/flink-job.jar --config flink-job/conf/job.yaml
```

---

## 15. 已知设计权衡

| 取舍 | v1 选择 | 理由 / 后续演进 |
|---|---|---|
| 状态迁移 | 经 source topic 重读 + 周期性导出快照加载 | 简单可靠；v2 可加点对点 transfer |
| 重平衡时机 | 仅 5min 边界 | 保证 KPI 窗口完整；burst 响应延迟 ≤ 5min |
| L1 调整粒度 | 仅 top-K 热点站点 | 保证 95%+ 站点映射稳定 |
| 唯一用户数 | HLL 近似 | 状态体积 vs 精度（约 1.5% 误差）的取舍 |
| Hive 写入 | flink-connector-hive | 比 FileSystem 重，但元数据天然管理 |

---

## 16. 后续扩展点

1. **StarRocks Sink 启用**：DDL 与 Routine Load 已就绪，切 `warehouse-type=starrocks`
2. **状态显式迁移（v2）**：从快照重读切到点对点 Kafka transfer
3. **热点 Key 拆分**：针对超极端单站点的补充手段（C 方案降级保护）
4. **多租户**：多 region 共享 Flink 集群，每 region 独立路由
5. **ML-based 异常检测**：统计/模型路径，与规则集并行
6. **K8s/YARN 部署清单**：完善 deploy/ 模块

---

## 17. 验收标准

- [ ] 三种 simulator 在本地能各自独立运行并向 Kafka 写入 Avro 数据
- [ ] topology-service 启动后，simulator 能从 `topology` topic 拿到拓扑
- [ ] Flink 作业能持续消费三流并产出 `anomaly-events` / `cell-kpi-{1m,5m}`
- [ ] MySQL 表能看到累积写入的异常事件与 KPI
- [ ] Hive 表能查询到分区数据
- [ ] 启用 burst skew 后，Coordinator 能在 ≤ 5min 内输出新路由（CSV 可见）
- [ ] 手动触发路由变更后，新 subtask 通过状态加载在 ≤ 30s 内进入稳态
- [ ] Grafana dashboard 能展示倾斜度、重平衡次数、规则命中等关键指标
- [ ] 单元 + 集成测试通过

---

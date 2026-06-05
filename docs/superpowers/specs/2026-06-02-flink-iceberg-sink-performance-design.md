# Flink Iceberg Sink 写入与性能对比规格

## 目标

在现有 Flink KPI 写入链路中新增 Apache Iceberg 写入能力，同时保留当前 Hive Parquet 写入逻辑，并在 e2e 冒烟测试中输出 Hive 与 Iceberg 的写入性能对比指标。

本需求不是替换 Hive，而是在同一份 `CellKpi` 输出上并行写入 Hive 可读 Parquet 目录和 Iceberg 表，便于对两种湖格式写入路径做本地 demo 级对比。

## 当前背景

当前 Flink 作业会将 KPI 写入以下目标：

- Kafka：`cell-kpi-1m`、`cell-kpi-5m`
- MySQL：`cell_kpi`
- Hive 外表：通过 Flink `FileSink` 写 Parquet 文件，再由 Hive `MSCK REPAIR TABLE` 发现分区

现有 Hive 路径实际目录结构为：

```text
docker/data/warehouse/cell_kpi/window_kind=<kind>/dt=<yyyy-MM-dd>/hour=<HH>/*.parquet
```

Hive 外表定义位于 `docs/hive-schema.q`，数据位置为：

```text
file:///warehouse/cell_kpi
```

## Iceberg 写入需求

Iceberg 使用 Hadoop Catalog，默认写入本地 warehouse：

```text
file:///warehouse/iceberg
```

默认表标识：

```text
fdb.cell_kpi
```

默认配置：

```text
FDB_ICEBERG_ENABLED=true
FDB_ICEBERG_WAREHOUSE=file:///warehouse/iceberg
FDB_ICEBERG_CATALOG=fdb_iceberg
FDB_ICEBERG_DATABASE=fdb
FDB_ICEBERG_TABLE=cell_kpi
```

Iceberg 表必须是 append-only 写入，本阶段不实现 upsert、overwrite、compaction、snapshot cleanup 或表维护任务。

Iceberg 表 schema 与当前 Hive KPI 表保持同一业务口径，并显式包含分区字段：

- `window_kind`
- `dt`
- `hour`

`dt` 与 `hour` 从 `CellKpi.windowStartTs` 按 UTC 时间派生，保证和现有 Hive Parquet 分区口径一致。

## Flink 集成设计

在 `FlinkJobMain` 中，保留当前 Kafka、MySQL、Hive sink。

对 `cellKpi1m` 与 `cellKpi5m` 两条流新增并行 Iceberg sink：

- `cell-kpi-iceberg-sink`
- `cell-kpi-5m-iceberg-sink`

Iceberg 写入使用 Flink DataStream Iceberg Sink：

```java
FlinkSink.forRowData(...).append()
```

因为 Iceberg sink 写入 `RowData`，需要新增 `CellKpi` 到 `RowData` 的 mapper。字段顺序必须与 Iceberg schema 保持一致：

```text
window_start_ts, window_end_ts, site_id, cell_id, grid_id,
num_chr_events, num_users,
avg_rsrp, avg_sinr, avg_prb_usage_dl, throughput_dl_mbps_avg,
drop_rate, ho_success_rate, attach_success_rate,
window_kind, dt, hour
```

## 性能打点需求

性能统计分两层。

### Job 内打点

在 Hive 与 Iceberg sink 前分别增加轻量 probe，输出 `[summary-code]` 日志。

至少统计：

- sink 名称
- records 数量
- 估算 payload bytes
- first record timestamp
- latest record timestamp
- records per second

需要分别覆盖：

- `hive-cell-kpi-1m`
- `hive-cell-kpi-5m`
- `iceberg-cell-kpi-1m`
- `iceberg-cell-kpi-5m`

### e2e 汇总打点

扩展 e2e summary，新增 Iceberg KPI 与 Hive/Iceberg 对比。

Hive 指标：

- Parquet 文件数
- Parquet 总字节数
- 分区数
- Hive 查询行数

Iceberg 指标：

- data file 数
- data file 总字节数
- partition 目录数
- metadata JSON 数
- snapshot 数

对比指标：

- Hive vs Iceberg 文件数
- Hive vs Iceberg 字节数
- Hive vs Iceberg 可见输出耗时
- Hive vs Iceberg job 内 records/sec

该对比用于本地冒烟和趋势观察，不定义为严格 benchmark。原因是 Hive 路径衡量的是 Parquet 文件可见加 Hive repair/query，而 Iceberg 路径衡量的是 Iceberg snapshot commit 后的表状态。

## 依赖要求

新增 Iceberg Flink runtime：

```xml
<dependency>
    <groupId>org.apache.iceberg</groupId>
    <artifactId>iceberg-flink-runtime-1.20</artifactId>
    <version>1.11.0</version>
</dependency>
```

如 `RowData` 转换需要 Flink Table 类型，则补充 Flink table/common 相关依赖，并优先使用 `provided` scope，避免和 Flink runtime 镜像依赖冲突。

## Docker/e2e 要求

Flink JobManager 与 TaskManager 容器必须获得相同 Iceberg 配置，并继续挂载现有 warehouse volume：

```text
./data/warehouse:/warehouse
```

e2e 必须验证：

- Hive Parquet 文件出现
- Hive 查询仍然返回 KPI 行
- Iceberg metadata 文件出现
- Iceberg data files 出现
- summary 中包含 `Iceberg KPI`
- summary 中包含 `Hive/Iceberg Compare`
- TaskManager 日志中出现 Hive 与 Iceberg sink probe 的 `[summary-code]`

## 已知验证风险

在本地验证过程中已经观察到一种失败形态：Iceberg Hadoop Catalog 表 metadata 创建成功，但 `data/` 目录没有出现，e2e 在等待 Iceberg data files 时超时。

后续实现必须把这个场景作为一等验证项处理，不能只以 metadata 存在作为 Iceberg 写入成功的证据。需要从以下方向排查：

- Iceberg writer 是否收到 `RowData`
- Iceberg branch 是否因为类型推断或 writer 异常失败
- checkpoint 是否完成且触发 Iceberg commit
- Iceberg `RowData` 类型信息是否与 Iceberg schema 完全一致
- Hadoop Catalog/warehouse 路径在 JobManager 与 TaskManager 是否一致
- shaded jar 是否引入 Iceberg/Hadoop 运行时冲突

## 非目标

本阶段不新增：

- REST Catalog
- Hive Catalog
- Spark / Trino 查询验证
- 对象存储
- Iceberg upsert/delete
- Iceberg compaction
- Iceberg snapshot cleanup
- 生产级 benchmark 报告

## 验收标准

- `mvn test` 通过。
- `docker compose -f docker/docker-compose.yml config` 通过。
- `FDB_E2E_SUMMARY=1 bash scripts/e2e-smoke-test.sh` 通过。
- Hive KPI 原有校验不回退。
- Iceberg warehouse 下同时存在 metadata 和 data files。
- e2e summary 输出 Hive KPI、Iceberg KPI、Hive/Iceberg Compare 三组指标。
- Flink TaskManager 日志中能看到 Hive 与 Iceberg 两类 sink probe 的 `[summary-code]`。

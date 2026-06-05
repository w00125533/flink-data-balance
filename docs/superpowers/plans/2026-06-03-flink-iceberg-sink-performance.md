# Flink Iceberg Sink 性能对比实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` (recommended) or `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在现有 Flink KPI Sink 链路中新增 Iceberg append 写入，并在 e2e 冒烟测试中输出 Hive 与 Iceberg 写入性能对比。

**Architecture:** 保留当前 Hive Parquet `FileSink`，在 `CellKpi` 1 分钟和 5 分钟 KPI 流上并行新增 Iceberg Hadoop Catalog Sink。Iceberg 使用 Flink DataStream `FlinkSink.forRowData(...).append()` 写入；Hive 和 Iceberg 两条分支前都增加轻量性能 probe 输出 `[summary-code]`，e2e 脚本汇总文件数、字节数、分区数、metadata/snapshot 等指标。

**Tech Stack:** Java 17, Maven, Flink 1.20.3, Iceberg 1.11.0 `iceberg-flink-runtime-1.20`, Avro, Parquet, Docker Compose, Bash e2e summary。

---

## 任务 1：补充 Maven 依赖

**Files:**

- Modify: `pom.xml`
- Modify: `flink-job/pom.xml`

- [ ] 在根 `pom.xml` 增加 `iceberg.version=1.11.0`。
- [ ] 在根 `pom.xml` dependency management 中增加 `org.apache.iceberg:iceberg-flink-runtime-1.20:${iceberg.version}`。
- [ ] 在根 `pom.xml` dependency management 中增加 `org.apache.flink:flink-table-common:${flink.version}`。
- [ ] 在 `flink-job/pom.xml` 增加 `iceberg-flink-runtime-1.20` 依赖。
- [ ] 在 `flink-job/pom.xml` 增加 `flink-table-common`，使用 `provided` scope。
- [ ] 运行 `mvn -pl flink-job -am test -DskipTests`，确认依赖解析与编译阶段可达。

## 任务 2：新增 Iceberg 配置解析

**Files:**

- Create: `flink-job/src/main/java/com/fdb/job/IcebergConfig.java`
- Create: `flink-job/src/test/java/com/fdb/job/IcebergConfigTest.java`
- Modify: `flink-job/src/main/java/com/fdb/job/FlinkJobMain.java`
- Modify: `flink-job/src/test/java/com/fdb/job/FlinkJobMainTest.java`

- [ ] 测试默认配置：enabled、warehouse、catalog、database、table。
- [ ] 测试 env 优先于 Java property。
- [ ] 测试非法 boolean 回退默认值。
- [ ] 实现 `IcebergConfig.resolve(env, properties)`。
- [ ] 在 `FlinkJobMain` 中新增 `resolveIcebergConfig(env, properties)`，委托给 `IcebergConfig`。
- [ ] 运行 `mvn -pl flink-job -am "-Dtest=IcebergConfigTest,FlinkJobMainTest" "-Dsurefire.failIfNoSpecifiedTests=false" test`。

## 任务 3：实现 CellKpi 到 RowData 映射

**Files:**

- Create: `flink-job/src/main/java/com/fdb/job/CellKpiIcebergMapper.java`
- Create: `flink-job/src/test/java/com/fdb/job/CellKpiIcebergMapperTest.java`

- [ ] 测试固定 `windowStartTs=1780383600000` 派生 `dt=2026-06-02`、`hour=07`。
- [ ] 测试所有字段按 Iceberg schema 顺序映射。
- [ ] 实现 `CellKpiIcebergMapper implements MapFunction<CellKpi, RowData>`。
- [ ] 字符串字段使用 `StringData.fromString(...)`。
- [ ] UTC 分区字段使用 `Instant.ofEpochMilli(windowStartTs)` 与 `ZoneOffset.UTC` 派生。
- [ ] 运行 `mvn -pl flink-job -am "-Dtest=CellKpiIcebergMapperTest" "-Dsurefire.failIfNoSpecifiedTests=false" test`。

## 任务 4：实现 Iceberg Sink 工厂

**Files:**

- Create: `flink-job/src/main/java/com/fdb/job/IcebergSinks.java`
- Create: `flink-job/src/test/java/com/fdb/job/IcebergSinksTest.java`

- [ ] 测试 table identifier 为 `fdb.cell_kpi`。
- [ ] 测试 schema 包含 17 列，顺序与 mapper 一致。
- [ ] 测试 partition spec 包含 identity 分区：`window_kind`、`dt`、`hour`。
- [ ] 测试 table properties 包含 `format-version=2`。
- [ ] 测试 `HadoopCatalog` 初始化后持有非空 Hadoop `Configuration`，防止运行时 `conf is null`。
- [ ] 实现 `cellKpiSchema()`、`cellKpiPartitionSpec(schema)`、`tableProperties()`。
- [ ] 实现 `hadoopCatalog(config)`，必须先设置 `new Configuration()` 再 initialize。
- [ ] 实现 `ensureTable(config)`，创建 namespace/table 或加载已存在表。
- [ ] 实现 `appendCellKpiSink(stream, config)`，使用 `CatalogLoader.hadoop(...)` 和 `TableLoader.fromCatalog(...)`。
- [ ] 运行 `mvn -pl flink-job -am "-Dtest=IcebergSinksTest" "-Dsurefire.failIfNoSpecifiedTests=false" test`。

## 任务 5：新增 Sink 性能 Probe

**Files:**

- Create: `flink-job/src/main/java/com/fdb/job/SinkPerformanceProbe.java`
- Create: `flink-job/src/test/java/com/fdb/job/SinkPerformanceProbeTest.java`

- [ ] 测试 record count 递增。
- [ ] 测试 estimated bytes 根据 `CellKpi` 字段递增。
- [ ] 测试 summary 字符串包含 `[summary-code]`、sink name、records、approx_bytes、records_per_sec。
- [ ] 实现 `SinkPerformanceProbe extends ProcessFunction<CellKpi, CellKpi>`。
- [ ] 不使用 `MapFunction<CellKpi, CellKpi>`，避免 Flink 对 Avro generated class 做错误类型推断。
- [ ] 在 `processElement` 中统计后 `out.collect(value)`。
- [ ] 暴露包内 `record(...)`/`summaryLine()` 便于单元测试。
- [ ] 运行 `mvn -pl flink-job -am "-Dtest=SinkPerformanceProbeTest" "-Dsurefire.failIfNoSpecifiedTests=false" test`。

## 任务 6：接入 FlinkJobMain

**Files:**

- Modify: `flink-job/src/main/java/com/fdb/job/FlinkJobMain.java`
- Modify: `flink-job/src/test/java/com/fdb/job/FlinkJobMainTest.java`

- [ ] 在 `main` 开始处解析 `IcebergConfig`。
- [ ] 在 `cellKpi1m` Hive sink 前增加 `SinkPerformanceProbe("hive-cell-kpi-1m", 100)`。
- [ ] 在 `cellKpi5m` Hive sink 前增加 `SinkPerformanceProbe("hive-cell-kpi-5m", 100)`。
- [ ] 使用 `process(function, new GenericTypeInfo<>(CellKpi.class))` 接入 probe。
- [ ] 当 `icebergConfig.enabled()` 为 true 时，将 `cellKpi1m` 经 Iceberg probe、`CellKpiIcebergMapper` 后写入 Iceberg。
- [ ] 当 `icebergConfig.enabled()` 为 true 时，将 `cellKpi5m` 经 Iceberg probe、`CellKpiIcebergMapper` 后写入 Iceberg。
- [ ] 对 `RowData` map 显式声明返回类型，避免 Flink 类型推断偏差。
- [ ] 运行 `mvn -pl flink-job -am "-Dtest=FlinkJobMainTest,SinkPerformanceProbeTest,CellKpiIcebergMapperTest,IcebergSinksTest" "-Dsurefire.failIfNoSpecifiedTests=false" test`。

## 任务 7：扩展 Docker 与 e2e summary

**Files:**

- Modify: `docker/docker-compose.yml`
- Modify: `scripts/e2e-summary-lib.sh`
- Modify: `scripts/e2e-smoke-test.sh`

- [ ] 在 JobManager 和 TaskManager 中增加 Iceberg 环境变量。
- [ ] 确认两个 Flink 容器继续挂载 `./data/warehouse:/warehouse`。
- [ ] 新增 `summary_iceberg_kpi(root)`，统计 data files、data bytes、partition count、metadata json、snapshot count。
- [ ] 新增 `summary_hive_iceberg_compare(hiveRoot, icebergDataRoot)`，输出 Hive/Iceberg 文件数与字节数。
- [ ] 在 e2e 中等待 Iceberg metadata 出现。
- [ ] 在 e2e 中等待 Iceberg data files 出现。
- [ ] 在 e2e 中输出 `Iceberg KPI` 与 `Hive/Iceberg Compare` summary section。
- [ ] 运行 `bash -n scripts/e2e-summary-lib.sh`。
- [ ] 运行 `bash -n scripts/e2e-smoke-test.sh`。
- [ ] 运行 `docker compose -f docker/docker-compose.yml config`。
- [ ] 运行带 profile 的 compose config，确认 JobManager/TaskManager 中出现 `FDB_ICEBERG_*`。

## 任务 8：Iceberg data file 排障与验收

**Files:**

- Modify as needed: `flink-job/src/main/java/com/fdb/job/IcebergSinks.java`
- Modify as needed: `flink-job/src/main/java/com/fdb/job/FlinkJobMain.java`
- Modify as needed: `scripts/e2e-summary-lib.sh`
- Modify as needed: `scripts/e2e-smoke-test.sh`

- [ ] 运行 `FDB_E2E_SUMMARY=1 FDB_E2E_KEEP_RUNNING_ON_FAIL=1 bash scripts/e2e-smoke-test.sh`，失败时保留容器。
- [ ] 如果 Iceberg metadata 出现但 data files 不出现，检查 `docker/data/warehouse/iceberg/fdb/cell_kpi`。
- [ ] 检查 `docker logs fdb-flink-taskmanager` 中 Iceberg writer、checkpoint、commit 相关异常。
- [ ] 检查 `docker logs fdb-flink-jobmanager` 中 checkpoint 完成情况。
- [ ] 确认 Iceberg probe 日志出现 `sink=iceberg-cell-kpi-1m` 或 `sink=iceberg-cell-kpi-5m`。
- [ ] 若 probe 没出现，优先修 Flink graph/类型信息。
- [ ] 若 probe 出现但 data files 不出现，优先查 Iceberg writer schema、checkpoint commit 和 warehouse 路径。
- [ ] 若出现 shaded jar 冲突，收窄 `flink-job` shade 依赖或调整 Iceberg/Hadoop 依赖作用域。
- [ ] 修复后重新运行 `mvn test`。
- [ ] 修复后重新运行 `FDB_E2E_SUMMARY=1 bash scripts/e2e-smoke-test.sh`。

## 最终验收

- [ ] `mvn test` 通过。
- [ ] `mvn package -DskipTests` 通过。
- [ ] `docker compose -f docker/docker-compose.yml config` 通过。
- [ ] `FDB_E2E_SUMMARY=1 bash scripts/e2e-smoke-test.sh` 通过。
- [ ] Hive KPI 原有校验通过。
- [ ] Iceberg warehouse 下存在 metadata JSON。
- [ ] Iceberg warehouse 下存在 data files。
- [ ] e2e summary 包含 `Iceberg KPI`。
- [ ] e2e summary 包含 `Hive/Iceberg Compare`。
- [ ] TaskManager 日志包含 Hive 与 Iceberg sink probe 的 `[summary-code]`。

## 备注

本计划独立于 foundation 计划，专门覆盖 Iceberg Sink 与 Hive/Iceberg 写入性能对比。若实现过程中发现 Iceberg 写入需要更换 catalog 类型、改用 Flink SQL/Table API，或需要新增查询引擎验证，应先更新规格文档，再拆出新的后续计划。

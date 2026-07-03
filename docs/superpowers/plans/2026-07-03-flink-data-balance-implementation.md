# Flink Data Balance Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the refreshed `flink-data-balance` design: PM/CFG naming, optional dynamic balancing, CHR/PM 1-minute fact full join, StarRocks query layer, result pages, sink latency observability, retention, and higher local Flink parallelism.

**Architecture:** Keep the repo as the existing Maven multi-module plus React frontend. The Flink job defaults to a direct `keyBy(cellId)` business pipeline; dynamic balancing is created only when `FDB_DYNAMIC_BALANCING_ENABLED=true`. KPI data remains in Iceberg/Hive and is queried through StarRocks external views, while anomaly data is written to StarRocks internal tables.

**Tech Stack:** Java 17, Flink 1.20.3, Maven, Avro, Kafka, Iceberg, Hive/HDFS, StarRocks, React 18, TypeScript, Vite, Ant Design, Docker Compose, GitNexus.

---

## Execution Guardrails

- Before editing Java symbols, run GitNexus impact analysis for the touched entry points. Minimum targets for this plan:
  - `com.fdb.job.FlinkJobMain`
  - `com.fdb.job.EnrichmentProcessFunction`
  - `com.fdb.job.KpiAggregator`
  - `com.fdb.common.metrics.StageMetricSample`
  - `com.fdb.observability.ObservabilityApiMain`
- If GitNexus reports HIGH or CRITICAL risk, pause and inspect affected processes before editing.
- Before each commit that changes Java, run the narrow module tests listed in the task.
- Before the final commit, run GitNexus `detect_changes` with staged scope and verify the affected flow set matches this plan.
- Compose changes must be verified with `docker compose -f docker/docker-compose.yml --profile e2e config`.
- Do not add Kafka, ZooKeeper, Hive, HDFS, StarRocks, Prometheus, or Kafka UI services to this repo when `../shared-data-infra` already provides them.

## File Structure

| Area | Files | Responsibility |
|---|---|---|
| Avro and shared models | `common/src/main/avro/PmStat.avsc`, `common/src/main/avro/CfgConfig.avsc`, `common/src/main/avro/CellKpi.avsc`, `common/src/main/avro/AnomalyEvent.avsc`, `common/src/main/java/com/fdb/common/metrics/*` | PM/CFG domain names, `JoinQuality`, split anomaly semantics, sink latency metric payloads |
| Simulator | `simulator/src/main/java/com/fdb/simulator/PmSimulator.java`, `CfgSimulator.java`, `SimulatorMain.java`, `simulator/src/main/resources/sim-pm.yaml`, `sim-cfg.yaml` | Publish CHR/PM/CFG with PM and CFG terminology |
| Flink pipeline | `flink-job/src/main/java/com/fdb/job/FlinkJobMain.java`, new `ChrMinuteFact.java`, `PmMinuteFact.java`, `MinuteFactEnvelope.java`, `ChrMinuteFactWindowFunction.java`, `PmMinuteFactWindowFunction.java`, `MinuteKpiJoinFunction.java`, `CellKpiRollupAggregator.java`, `StarRocksSinks.java`, `SinkLatencyProbe.java` | Default direct keyBy, optional dynamic balancing, 1m full join, 5m rollup, StarRocks anomaly sinks, sink latency metrics |
| Storage and infra scripts | `docker/docker-compose.yml`, `scripts/dev-up.sh`, `scripts/create-kafka-topics.sh`, `scripts/init-starrocks.sh`, `scripts/init-starrocks.sql`, `scripts/retention-maintenance.sh`, `scripts/e2e-smoke-test.sh`, `scripts/e2e-summary-lib.sh`, `docs/hive-schema.q` | Remove local MySQL, use shared StarRocks, topic retention, DDL, maintenance, e2e verification |
| Observability API | `observability-api/src/main/java/com/fdb/observability/ObservabilityApiMain.java`, new `service/StarRocksQueryService.java`, new model classes under `observability-api/src/main/java/com/fdb/observability/model/` | KPI/anomaly/sink-latency APIs, dynamic balancing visibility, StarRocks query access |
| Frontend | `frontend/src/App.tsx`, `frontend/src/types/observability.ts`, `frontend/src/api/client.ts`, new pages under `frontend/src/pages/` | Business result tabs, sink latency page, hide dynamic balancing when disabled |
| Tests | Existing `*Test.java` plus new tests named below; `frontend/src/App.test.tsx` | Red/green coverage for each behavior |

---

## Task 1: Preflight Impact Analysis

**Files:**
- Read: `AGENTS.md`
- Read: `docs/superpowers/specs/2026-04-29-flink-data-balance-design.md`

- [ ] **Step 1: Run GitNexus impact analysis for Java entry points**

Use the GitNexus MCP tool:

```text
gitnexus_impact({ target: "com.fdb.job.FlinkJobMain", direction: "upstream", minConfidence: 0.8, maxDepth: 3 })
gitnexus_impact({ target: "com.fdb.job.EnrichmentProcessFunction", direction: "upstream", minConfidence: 0.8, maxDepth: 3 })
gitnexus_impact({ target: "com.fdb.job.KpiAggregator", direction: "upstream", minConfidence: 0.8, maxDepth: 3 })
gitnexus_impact({ target: "com.fdb.common.metrics.StageMetricSample", direction: "upstream", minConfidence: 0.8, maxDepth: 3 })
gitnexus_impact({ target: "com.fdb.observability.ObservabilityApiMain", direction: "upstream", minConfidence: 0.8, maxDepth: 3 })
```

Expected: no HIGH or CRITICAL affected processes. If the index is stale, run:

```powershell
npx gitnexus analyze
```

- [ ] **Step 2: Confirm the shared infra boundary**

Run:

```powershell
docker compose -f ..\shared-data-infra\compose.yaml -f ..\shared-data-infra\compose.streaming.yaml --profile streaming config
docker compose -f ..\shared-data-infra\compose.yaml -f ..\shared-data-infra\compose.lakehouse.yaml --profile lakehouse config
docker compose -f ..\shared-data-infra\compose.yaml -f ..\shared-data-infra\compose.starrocks.yaml --profile starrocks config
```

Expected: Kafka/ZooKeeper, Hive/HDFS, and StarRocks services are defined in `../shared-data-infra`.

- [ ] **Step 3: Commit nothing**

This task only records analysis. Do not create a commit.

---

## Task 2: Rename MR/CM Domain to PM/CFG

**Files:**
- Rename: `common/src/main/avro/MrStat.avsc` -> `common/src/main/avro/PmStat.avsc`
- Rename: `common/src/main/avro/CmConfig.avsc` -> `common/src/main/avro/CfgConfig.avsc`
- Rename tests: `MrStatSchemaTest.java` -> `PmStatSchemaTest.java`, `CmConfigSchemaTest.java` -> `CfgConfigSchemaTest.java`
- Rename: `simulator/src/main/java/com/fdb/simulator/MrSimulator.java` -> `PmSimulator.java`
- Rename: `simulator/src/main/java/com/fdb/simulator/CmSimulator.java` -> `CfgSimulator.java`
- Rename resources: `simulator/src/main/resources/sim-mr.yaml` -> `sim-pm.yaml`, `sim-cm.yaml` -> `sim-cfg.yaml`
- Modify all Java, TypeScript, YAML, and shell references found by `rg -n "\bmr\b|Mr|MR|mr-|cm|Cm|CM|cm-"`.

- [ ] **Step 1: Write failing Avro schema tests**

Update the class names and generated imports in the two schema tests:

```java
class PmStatSchemaTest {
    @Test
    void roundtrip_pm_stat() throws Exception {
        PmStat original = PmStat.newBuilder()
            .setSiteId("SITE-001")
            .setCellId("CELL-001")
            .setWindowStartTs(1_000L)
            .setWindowEndTs(11_000L)
            .setPrbUsageDl(0.65f)
            .setPrbUsageUl(0.25f)
            .setActiveUsers(42)
            .setAvgRsrp(-92.0f)
            .setAvgRsrq(-8.0f)
            .setAvgSinr(12.0f)
            .setAvgCqi(10.0f)
            .setAvgMcs(18.0f)
            .setAvgBler(0.02f)
            .setThroughputDlMbps(120.0f)
            .setThroughputUlMbps(30.0f)
            .setDroppedConnections(1)
            .setHandoverSuccess(18)
            .setHandoverFailure(2)
            .setPrachAttempt(9)
            .setPrachFailure(1)
            .setRrcEstabAttempt(20)
            .setRrcEstabSuccess(19)
            .setAvgLatencyMs(18.0f)
            .setPacketLossRate(0.001f)
            .build();
        assertThat(original.getCellId()).isEqualTo("CELL-001");
    }
}
```

```java
class CfgConfigSchemaTest {
    @Test
    void roundtrip_cfg_config() {
        CfgConfig config = CfgConfig.newBuilder()
            .setSiteId("SITE-001")
            .setCellId("CELL-001")
            .setEffectiveTs(1_000L)
            .setVersion(3L)
            .setCellType(CellType.NR_SA)
            .setBandwidthMhz(100)
            .setFrequencyBand("n78")
            .setArfcn(632448)
            .setMaxPowerDbm(49.0f)
            .setAzimuth(90)
            .setCenterLat(39.9)
            .setCenterLon(116.4)
            .setCoverageRadiusM(500)
            .setPci(100)
            .setTac(40001)
            .setEci(1000L)
            .setMcc("460")
            .setMnc("00")
            .setAntennaPorts(4)
            .setNssai(new ArrayList<>())
            .setNeighborCells(new ArrayList<>())
            .setTombstone(false)
            .build();
        assertThat(config.getCellId()).isEqualTo("CELL-001");
    }
}
```

- [ ] **Step 2: Run tests and verify failure**

Run:

```powershell
mvn -pl common -am test -Dtest=PmStatSchemaTest,CfgConfigSchemaTest
```

Expected: compilation fails because `PmStat` and `CfgConfig` are not generated yet.

- [ ] **Step 3: Rename Avro records**

Use exact record names:

```json
{ "namespace": "com.fdb.common.avro", "type": "record", "name": "PmStat" }
{ "namespace": "com.fdb.common.avro", "type": "record", "name": "CfgConfig" }
```

Keep existing field names unless the refreshed design says otherwise. Keep topic defaults out of schema files.

- [ ] **Step 4: Rename simulator classes and modes**

`SimulatorMain` must accept:

```text
Usage: simulator <chr|pm|cfg> [--config <path>]
```

Mode mapping:

```java
case "chr" -> new ChrSimulator(configPath).run();
case "pm" -> new PmSimulator(configPath).run();
case "cfg" -> new CfgSimulator(configPath).run();
```

Defaults:

```java
SimulatorConfig.load("sim-pm.yaml", configPath);
config.topic("pm-stats");
new TopologyClient(bootstrap, "sim-pm");
```

```java
SimulatorConfig.load("sim-cfg.yaml", configPath);
config.topic("cfg-config");
new TopologyClient(bootstrap, "sim-cfg");
```

- [ ] **Step 5: Replace all PM/CFG references**

Run:

```powershell
rg -n "\bmr\b|Mr|MR|mr-|cm|Cm|CM|cm-" common simulator flink-job observability-api frontend scripts docker -S
```

Expected after edits: no matches except historical text inside old plan documents under `docs/superpowers/plans/2026-05-06-*`.

- [ ] **Step 6: Run tests**

Run:

```powershell
mvn -pl common,simulator -am test
```

Expected: all tests pass.

- [ ] **Step 7: Commit**

```powershell
git add common simulator
git commit -m "refactor: rename PM and CFG domain models"
```

---

## Task 3: Topic Retention and Shared Infrastructure Cleanup

**Files:**
- Modify: `scripts/create-kafka-topics.sh`
- Modify: `scripts/dev-up.sh`
- Modify: `scripts/e2e-smoke-test.sh`
- Modify: `scripts/e2e-summary-lib.sh`
- Modify: `docker/docker-compose.yml`
- Delete: `scripts/init-mysql.sql`
- Add: `scripts/init-starrocks.sql`
- Add: `scripts/init-starrocks.sh`

- [ ] **Step 1: Update topic creation test by shell execution**

Run:

```powershell
bash -n scripts/create-kafka-topics.sh
```

Expected before edits: script syntax passes but still creates old `mr-stats`, `cm-config`, and 3-day or 7-day retentions.

- [ ] **Step 2: Update `create-kafka-topics.sh` topic matrix**

Business topics:

```bash
RETENTION_MS=${FDB_RETENTION_MS:-3600000}
RETENTION_BYTES=${FDB_RETENTION_BYTES:-10737418240}
DYNAMIC_BALANCING_ENABLED=${FDB_DYNAMIC_BALANCING_ENABLED:-false}

create_topic chr-events          64 delete  "$RETENTION_MS" "$RETENTION_BYTES"
create_topic pm-stats            16 delete  "$RETENTION_MS" "$RETENTION_BYTES"
create_topic cfg-config           8 compact
create_topic topology             4 compact
create_topic fdb-stage-metrics    4 delete  "$RETENTION_MS" "$RETENTION_BYTES"
create_topic cell-kpi-1m          8 delete  "$RETENTION_MS" "$RETENTION_BYTES"
create_topic cell-kpi-5m          8 delete  "$RETENTION_MS" "$RETENTION_BYTES"
create_topic cell-anomaly-events 16 delete  "$RETENTION_MS" "$RETENTION_BYTES"
create_topic grid-anomaly-events 16 delete  "$RETENTION_MS" "$RETENTION_BYTES"
create_topic chr-dlq              4 delete  "$RETENTION_MS" "$RETENTION_BYTES"
create_topic pm-dlq               4 delete  "$RETENTION_MS" "$RETENTION_BYTES"
create_topic cfg-dlq              4 delete  "$RETENTION_MS" "$RETENTION_BYTES"
create_topic enrichment-late      4 delete  "$RETENTION_MS" "$RETENTION_BYTES"

if [ "$DYNAMIC_BALANCING_ENABLED" = "true" ]; then
  create_topic lb-heartbeat 1 delete "$RETENTION_MS" "$RETENTION_BYTES"
  create_topic lb-routing   1 compact
fi
```

Extend `create_topic` so delete topics receive both:

```bash
--config retention.ms="$retention_ms" --config retention.bytes="$retention_bytes"
```

- [ ] **Step 3: Remove project-local MySQL from compose**

Delete the `mysql` service and all `FDB_MYSQL_*` environment variables. Keep only project runtime services:

```yaml
services:
  observability-api:
  frontend:
  prometheus:
  jobmanager:
  taskmanager:
```

Add StarRocks settings to `observability-api`, `jobmanager`, and `taskmanager`:

```yaml
- FDB_STARROCKS_JDBC_URL=jdbc:mysql://starrocks-fe:9030/fdb
- FDB_STARROCKS_USER=root
- FDB_STARROCKS_PASSWORD=
- FDB_STARROCKS_DATABASE=fdb
- FDB_DYNAMIC_BALANCING_ENABLED=${FDB_DYNAMIC_BALANCING_ENABLED:-false}
- FDB_FLINK_PARALLELISM=${FDB_FLINK_PARALLELISM:-4}
```

Set TaskManager slots:

```yaml
- "FLINK_PROPERTIES=taskmanager.numberOfTaskSlots: 4\nmetrics.reporter.prom.factory.class: org.apache.flink.metrics.prometheus.PrometheusReporterFactory\nmetrics.reporter.prom.port: 9249"
```

- [ ] **Step 4: Update `dev-up.sh`**

Change startup text to:

```bash
echo "[dev-up] Starting local project containers (Flink runtime / observability)..."
```

Start shared StarRocks profile if the StarRocks FE container is not running:

```bash
shared_starrocks() {
  docker compose -f "$SHARED_INFRA_DIR/compose.yaml" -f "$SHARED_INFRA_DIR/compose.starrocks.yaml" --profile starrocks "$@"
}

if ! docker ps --format '{{.Names}}' | grep -q '^shared-data-infra-starrocks-fe-1$'; then
  shared_starrocks up -d
fi
```

Remove MySQL readiness and initialization. Add:

```bash
echo "[dev-up] Initializing StarRocks objects..."
bash scripts/init-starrocks.sh
```

- [ ] **Step 5: Add StarRocks DDL**

`scripts/init-starrocks.sql` must create:

```sql
CREATE DATABASE IF NOT EXISTS fdb;
USE fdb;

CREATE TABLE IF NOT EXISTS cell_anomaly_events (
  detection_ts BIGINT NOT NULL,
  event_ts BIGINT NOT NULL,
  site_id VARCHAR(64),
  cell_id VARCHAR(64) NOT NULL,
  grid_id VARCHAR(16),
  latitude DOUBLE,
  longitude DOUBLE,
  anomaly_type VARCHAR(64) NOT NULL,
  severity VARCHAR(16) NOT NULL,
  rule_version VARCHAR(32),
  context_json STRING
)
DUPLICATE KEY(detection_ts, cell_id, anomaly_type)
DISTRIBUTED BY HASH(cell_id) BUCKETS 16
PROPERTIES ("replication_num" = "1");

CREATE TABLE IF NOT EXISTS grid_anomaly_events (
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
DISTRIBUTED BY HASH(grid_id) BUCKETS 16
PROPERTIES ("replication_num" = "1");
```

Add Iceberg/Hive external catalog creation guarded by `IF NOT EXISTS`:

```sql
CREATE EXTERNAL CATALOG IF NOT EXISTS fdb_iceberg
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hive",
  "hive.metastore.uris" = "thrift://hive-metastore:9083"
);

CREATE VIEW IF NOT EXISTS kpi_1m AS
SELECT * FROM fdb_iceberg.fdb.cell_kpi WHERE window_kind = 'MIN_1';

CREATE VIEW IF NOT EXISTS kpi_5m AS
SELECT * FROM fdb_iceberg.fdb.cell_kpi WHERE window_kind = 'MIN_5';
```

- [ ] **Step 6: Verify compose config**

Run:

```powershell
docker compose -f docker/docker-compose.yml --profile e2e config
```

Expected: no `mysql` service and no `FDB_MYSQL_*` variables.

- [ ] **Step 7: Commit**

```powershell
git add docker scripts
git rm scripts/init-mysql.sql
git commit -m "chore: use shared StarRocks and PM Kafka topics"
```

---

## Task 4: Cell KPI Join Quality and Sink Latency Models

**Files:**
- Modify: `common/src/main/avro/CellKpi.avsc`
- Add generated enum by schema: `JoinQuality`
- Modify: `common/src/main/java/com/fdb/common/metrics/StageMetricSample.java`
- Modify tests under `common/src/test/java/com/fdb/common/`

- [ ] **Step 1: Write failing `CellKpiSchemaTest` assertion**

Add to `CellKpiSchemaTest`:

```java
assertThat(decoded.getJoinQuality()).isEqualTo(JoinQuality.JOINED);
```

Build `CellKpi` in the test with:

```java
.setJoinQuality(JoinQuality.JOINED)
```

- [ ] **Step 2: Add `joinQuality` to schema**

Insert after `windowKind`:

```json
{ "name": "joinQuality", "type": { "type": "enum", "name": "JoinQuality", "symbols": ["JOINED","CHR_ONLY","PM_ONLY"] } }
```

Update all `CellKpi.newBuilder()` and constructor usages.

- [ ] **Step 3: Extend sink metric payload**

`StageMetricSample.sink(...)` must carry:

```java
String sinkType,
String dataset,
String windowKind,
long records,
long bytes,
long durationMs,
long latencyP50Ms,
long latencyP95Ms,
long latencyP99Ms,
long failureCount,
String errorMessage,
long checkpointId
```

Use empty string for absent text and `-1L` for absent checkpoint.

- [ ] **Step 4: Update metric tests**

Add a roundtrip test:

```java
StageMetricSample sample = StageMetricSample.sinkLatency(
    "iceberg-cell-kpi-1m",
    "Iceberg KPI 1m Sink",
    "healthy",
    "iceberg",
    "kpi_1m",
    "MIN_1",
    120L,
    12_000L,
    45L,
    30L,
    45L,
    80L,
    0L,
    "",
    42L,
    1_717_400_000_000L);

StageMetricSample decoded = StageMetricSample.fromJson(sample.toJson());
assertThat(decoded.sinkType()).isEqualTo("iceberg");
assertThat(decoded.dataset()).isEqualTo("kpi_1m");
assertThat(decoded.durationMs()).isEqualTo(45L);
assertThat(decoded.latencyP99Ms()).isEqualTo(80L);
```

- [ ] **Step 5: Run tests**

Run:

```powershell
mvn -pl common -am test
```

Expected: all common tests pass.

- [ ] **Step 6: Commit**

```powershell
git add common
git commit -m "feat(common): add KPI join quality and sink latency metrics"
```

---

## Task 5: Optional Dynamic Balancing Switch

**Files:**
- Modify: `flink-job/src/main/java/com/fdb/job/FlinkJobMain.java`
- Modify: `flink-job/src/main/java/com/fdb/job/RoutingAssigner.java`
- Modify: `flink-job/src/main/java/com/fdb/job/RoutedEnvelope.java`
- Modify tests: `flink-job/src/test/java/com/fdb/job/FlinkJobMainTest.java`

- [ ] **Step 1: Add failing config tests**

Add:

```java
@Test
void dynamic_balancing_defaults_to_disabled() {
    assertThat(FlinkJobMain.resolveDynamicBalancingEnabled(Map.of(), new Properties())).isFalse();
}

@Test
void dynamic_balancing_can_be_enabled_by_environment() {
    assertThat(FlinkJobMain.resolveDynamicBalancingEnabled(
        Map.of("FDB_DYNAMIC_BALANCING_ENABLED", "true"), new Properties())).isTrue();
}

@Test
void parallelism_defaults_to_four() {
    assertThat(FlinkJobMain.resolveParallelism(Map.of(), new Properties())).isEqualTo(4);
}
```

- [ ] **Step 2: Implement config resolution**

Use:

```java
static boolean resolveDynamicBalancingEnabled(Map<String, String> env, Properties properties) {
    String configured = env.get("FDB_DYNAMIC_BALANCING_ENABLED");
    if (configured == null || configured.isBlank()) {
        configured = properties.getProperty("fdb.dynamic.balancing.enabled");
    }
    return configured != null && Boolean.parseBoolean(configured.trim());
}
```

Change invalid/default parallelism fallback to `4`.

- [ ] **Step 3: Split pipeline branch creation**

In `main`:

```java
boolean dynamicBalancingEnabled = resolveDynamicBalancingEnabled(System.getenv(), System.getProperties());
DataStream<InputEnvelope> keyedInput = mergedInput;
DataStream<RoutedEnvelope> assigned = dynamicBalancingEnabled
    ? buildDynamicallyAssignedStream(env, keyedInput, bootstrap, groupId)
    : keyedInput
        .map(envelope -> new RoutedEnvelope(envelope, 0))
        .returns(new GenericTypeInfo<>(RoutedEnvelope.class))
        .name("direct-cellid-routing");
```

Only call `buildDynamicallyAssignedStream(...)` when the switch is true. Move all `lb-routing-source`, `routing-assigner`, `vbucket-load-meter`, `lb-heartbeat-sink`, `lb-heartbeat-source`, `load-coordinator`, and `lb-routing-sink` code into that method.

- [ ] **Step 4: Change dynamic route key to `cellId`**

`RoutingAssigner` and `RoutedEnvelope.stateKey()` must use `cellId`, not `siteId`:

```java
private static String cellId(InputEnvelope envelope) {
    return envelope.key();
}
```

Expected: dynamic route and CFG/keyed business state use the same key.

- [ ] **Step 5: Run tests**

Run:

```powershell
mvn -pl flink-job -am test -Dtest=FlinkJobMainTest,RebalancePolicyTest
```

Expected: all selected tests pass.

- [ ] **Step 6: Commit**

```powershell
git add flink-job/src/main/java/com/fdb/job flink-job/src/test/java/com/fdb/job
git commit -m "feat(flink): make dynamic balancing optional"
```

---

## Task 6: CHR and PM 1-Minute Facts with 2-Minute Full Join

**Files:**
- Add: `flink-job/src/main/java/com/fdb/job/ChrMinuteFact.java`
- Add: `flink-job/src/main/java/com/fdb/job/PmMinuteFact.java`
- Add: `flink-job/src/main/java/com/fdb/job/MinuteFactEnvelope.java`
- Add: `flink-job/src/main/java/com/fdb/job/ChrMinuteFactWindowFunction.java`
- Add: `flink-job/src/main/java/com/fdb/job/PmMinuteFactWindowFunction.java`
- Add: `flink-job/src/main/java/com/fdb/job/MinuteKpiJoinFunction.java`
- Modify: `flink-job/src/main/java/com/fdb/job/FlinkJobMain.java`
- Add tests under `flink-job/src/test/java/com/fdb/job/`

- [ ] **Step 1: Write failing join tests**

Create `MinuteKpiJoinFunctionTest` with these cases:

```java
@Test
void emits_joined_when_chr_and_pm_arrive_for_same_cell_and_minute() throws Exception
```

Expected output:

```java
assertThat(kpi.getCellId()).isEqualTo("CELL-001");
assertThat(kpi.getWindowKind()).isEqualTo(WindowKind.MIN_1);
assertThat(kpi.getJoinQuality()).isEqualTo(JoinQuality.JOINED);
assertThat(kpi.getNumChrEvents()).isEqualTo(2);
assertThat(kpi.getAvgPrbUsageDl()).isEqualTo(0.60f);
```

Add:

```java
@Test
void emits_chr_only_after_lateness_timer()
@Test
void emits_pm_only_after_lateness_timer()
@Test
void cfg_state_is_updated_by_cell_id_and_tombstone_removes_it()
```

- [ ] **Step 2: Create minute fact records**

Use immutable records:

```java
record ChrMinuteFact(String cellId, String siteId, long minuteTs, long count, long uniqueUsers,
                     double rsrpSum, double sinrSum, long attachAttempts, long attachSuccess) {}
```

```java
record PmMinuteFact(String cellId, String siteId, long minuteTs, long pmWindowCount,
                    double prbUsageDlSum, double throughputDlMbpsSum, long dropCount,
                    long handoverSuccess, long handoverFailure) {}
```

- [ ] **Step 3: Implement separate 1-minute aggregates**

Use event-time windows:

```java
chrStream
    .keyBy(chr -> chr.getCellId().toString())
    .window(TumblingEventTimeWindows.of(Time.minutes(1)))
    .process(new ChrMinuteFactWindowFunction())
    .name("chr-1m-fact");

pmStream
    .keyBy(pm -> pm.getCellId().toString())
    .window(TumblingEventTimeWindows.of(Time.minutes(1)))
    .process(new PmMinuteFactWindowFunction())
    .name("pm-1m-fact");
```

PM watermark:

```java
WatermarkStrategy.<PmStat>forBoundedOutOfOrderness(Duration.ofMinutes(2))
    .withTimestampAssigner((event, ts) -> event.getWindowEndTs())
```

- [ ] **Step 4: Implement full join by `cellId + minuteTs` inside a `cellId` keyed operator**

Union facts and CFG updates as `MinuteFactEnvelope`, then:

```java
minuteFactEnvelopeStream
    .keyBy(MinuteFactEnvelope::cellId)
    .process(new MinuteKpiJoinFunction(Duration.ofMinutes(2)), new GenericTypeInfo<>(CellKpi.class))
    .name("kpi-1m-full-join")
    .uid("kpi-1m-full-join");
```

State:

```java
MapState<Long, ChrMinuteFact> chrFactsByMinute;
MapState<Long, PmMinuteFact> pmFactsByMinute;
ValueState<CfgConfig> latestCfgByCell;
```

Emit immediately when both sides arrive. Emit one-sided result when the event-time timer for `minuteTs + 60_000 + 120_000` fires.

- [ ] **Step 5: Run tests**

Run:

```powershell
mvn -pl flink-job -am test -Dtest=MinuteKpiJoinFunctionTest
```

Expected: all join tests pass.

- [ ] **Step 6: Commit**

```powershell
git add flink-job/src/main/java/com/fdb/job flink-job/src/test/java/com/fdb/job
git commit -m "feat(flink): join CHR and PM minute facts"
```

---

## Task 7: Roll 5-Minute KPI from 1-Minute KPI

**Files:**
- Add: `flink-job/src/main/java/com/fdb/job/CellKpiRollupAggregator.java`
- Modify: `flink-job/src/main/java/com/fdb/job/FlinkJobMain.java`
- Add: `flink-job/src/test/java/com/fdb/job/CellKpiRollupAggregatorTest.java`

- [ ] **Step 1: Write failing rollup test**

Test input: five `CellKpi` records with `WindowKind.MIN_1`, same `cellId`, consecutive minutes.

Expected:

```java
assertThat(result.getWindowKind()).isEqualTo(WindowKind.MIN_5);
assertThat(result.getNumChrEvents()).isEqualTo(500L);
assertThat(result.getAvgPrbUsageDl()).isEqualTo(0.70f);
assertThat(result.getJoinQuality()).isEqualTo(JoinQuality.JOINED);
```

If any child minute is one-sided, rollup quality precedence is:

```text
JOINED only when all child minutes are JOINED
CHR_ONLY when all non-empty child minutes are CHR_ONLY
PM_ONLY when all non-empty child minutes are PM_ONLY
JOINED when mixed one-sided records produce both CHR and PM evidence across the 5-minute window
```

- [ ] **Step 2: Replace existing 5-minute raw enrichment window**

Remove the current:

```java
enriched.keyBy(...).window(TumblingProcessingTimeWindows.of(Time.minutes(5)))
```

Use:

```java
DataStream<CellKpi> cellKpi5m = cellKpi1m
    .keyBy(CellKpi::getCellId)
    .window(TumblingEventTimeWindows.of(Time.minutes(5)))
    .process(new CellKpiRollupAggregator(), new GenericTypeInfo<>(CellKpi.class))
    .name("kpi-5m-rollup")
    .uid("kpi-5m-rollup");
```

- [ ] **Step 3: Run tests**

Run:

```powershell
mvn -pl flink-job -am test -Dtest=CellKpiRollupAggregatorTest,FlinkJobE2ETest
```

Expected: all selected tests pass.

- [ ] **Step 4: Commit**

```powershell
git add flink-job/src/main/java/com/fdb/job flink-job/src/test/java/com/fdb/job
git commit -m "feat(flink): roll up 5m KPI from 1m KPI"
```

---

## Task 8: Split Anomaly Outputs and Write StarRocks Internal Tables

**Files:**
- Rename: `flink-job/src/main/java/com/fdb/job/JdbcSinks.java` -> `StarRocksSinks.java`
- Modify: `flink-job/pom.xml`
- Modify: `flink-job/src/main/java/com/fdb/job/FlinkJobMain.java`
- Modify: `common/src/main/avro/AnomalyEvent.avsc`
- Add tests: `flink-job/src/test/java/com/fdb/job/StarRocksSinksTest.java`

- [ ] **Step 1: Write failing sink SQL tests**

Assert anomaly sink SQL targets:

```java
assertThat(StarRocksSinks.cellAnomalyInsertSql()).contains("INSERT INTO cell_anomaly_events");
assertThat(StarRocksSinks.gridAnomalyInsertSql()).contains("INSERT INTO grid_anomaly_events");
assertThat(StarRocksSinks.cellKpiInsertSql()).isEmpty();
```

The third assertion documents that KPI is not written to an internal StarRocks table.

- [ ] **Step 2: Implement StarRocks JDBC config**

Use env/property names:

```text
FDB_STARROCKS_JDBC_URL
FDB_STARROCKS_USER
FDB_STARROCKS_PASSWORD
FDB_STARROCKS_DATABASE
```

Default URL:

```text
jdbc:mysql://starrocks-fe:9030/fdb
```

Keep the class and method names StarRocks-specific even though the wire protocol is MySQL-compatible.

- [ ] **Step 3: Split streams and topics**

In `FlinkJobMain`:

```java
cellAnomalies.sinkTo(cellAnomalyKafkaSink).name("cell-anomaly-kafka-sink");
gridAnomalies.sinkTo(gridAnomalyKafkaSink).name("grid-anomaly-kafka-sink");

cellAnomalies.sinkTo(StarRocksSinks.cellAnomalySink()).name("cell-anomaly-starrocks-sink");
gridAnomalies.sinkTo(StarRocksSinks.gridAnomalySink()).name("grid-anomaly-starrocks-sink");
```

Topics:

```text
cell-anomaly-events
grid-anomaly-events
```

Remove all `cellKpi*.sinkTo(JdbcSinks.cellKpiSink())` calls.

- [ ] **Step 4: Run tests**

Run:

```powershell
mvn -pl flink-job -am test -Dtest=StarRocksSinksTest,FlinkJobMainTest,FlinkJobE2ETest
```

Expected: all selected tests pass.

- [ ] **Step 5: Commit**

```powershell
git add flink-job common
git rm flink-job/src/main/java/com/fdb/job/JdbcSinks.java
git commit -m "feat(flink): write anomalies to StarRocks"
```

---

## Task 9: Sink Latency Collection for Every Sink Branch

**Files:**
- Rename: `flink-job/src/main/java/com/fdb/job/SinkPerformanceProbe.java` -> `SinkLatencyProbe.java`
- Modify: `flink-job/src/main/java/com/fdb/job/FlinkJobMain.java`
- Modify tests: `flink-job/src/test/java/com/fdb/job/SinkPerformanceProbeTest.java` -> `SinkLatencyProbeTest.java`
- Modify: `observability-api/src/main/java/com/fdb/observability/service/ObservabilitySnapshotService.java`

- [ ] **Step 1: Write failing latency tests**

Create assertions:

```java
SinkLatencyProbe probe = new SinkLatencyProbe("iceberg-cell-kpi-1m", "iceberg", "kpi_1m", "MIN_1", 10);
```

Expected emitted sample:

```java
assertThat(sample.sinkType()).isEqualTo("iceberg");
assertThat(sample.dataset()).isEqualTo("kpi_1m");
assertThat(sample.records()).isEqualTo(10L);
assertThat(sample.durationMs()).isGreaterThanOrEqualTo(0L);
assertThat(sample.latencyP95Ms()).isGreaterThanOrEqualTo(sample.latencyP50Ms());
```

- [ ] **Step 2: Wrap every sink branch**

Add probes before:

```text
cell-kpi-1m-kafka-sink
cell-kpi-5m-kafka-sink
cell-kpi-hive-sink
cell-kpi-5m-hive-sink
cell-kpi-iceberg-sink
cell-kpi-5m-iceberg-sink
cell-anomaly-kafka-sink
grid-anomaly-kafka-sink
cell-anomaly-starrocks-sink
grid-anomaly-starrocks-sink
```

Use names:

```text
kafka-kpi-1m
kafka-kpi-5m
hive-kpi-1m
hive-kpi-5m
iceberg-kpi-1m
iceberg-kpi-5m
kafka-cell-anomaly
kafka-grid-anomaly
starrocks-cell-anomaly
starrocks-grid-anomaly
```

- [ ] **Step 3: Keep window and heavy sinks as separate vertices**

For each branch:

```java
.startNewChain()
.name("<specific-probe-name>")
```

After heavy sinks:

```java
.name("<specific-sink-name>")
```

Expected Flink UI contains distinct vertices for `kpi-1m-full-join`, `kpi-5m-rollup`, each KPI sink, and each anomaly sink.

- [ ] **Step 4: Run tests**

Run:

```powershell
mvn -pl common,flink-job,observability-api -am test -Dtest=SinkLatencyProbeTest,ObservabilitySnapshotServiceTest
```

Expected: all selected tests pass.

- [ ] **Step 5: Commit**

```powershell
git add common flink-job observability-api
git commit -m "feat(observability): record sink latency metrics"
```

---

## Task 10: Observability API Result Queries

**Files:**
- Add: `observability-api/src/main/java/com/fdb/observability/service/StarRocksQueryService.java`
- Add: `observability-api/src/main/java/com/fdb/observability/model/KpiResultRow.java`
- Add: `observability-api/src/main/java/com/fdb/observability/model/AnomalyResultRow.java`
- Add: `observability-api/src/main/java/com/fdb/observability/model/SinkLatencySummary.java`
- Modify: `observability-api/src/main/java/com/fdb/observability/ObservabilityApiMain.java`
- Modify: `observability-api/pom.xml`
- Add tests under `observability-api/src/test/java/com/fdb/observability/`

- [ ] **Step 1: Write failing endpoint tests**

Add tests for:

```text
GET /api/results/kpi/1m?cellId=CELL-001
GET /api/results/kpi/5m?siteId=SITE-001
GET /api/results/anomalies/cell?severity=HIGH
GET /api/results/anomalies/grid?gridId=wx4g0e
GET /api/results/sink-latency
GET /api/runtime/config
```

Expected JSON arrays use these fields:

```json
{
  "windowStartTs": 1000,
  "windowEndTs": 61000,
  "windowKind": "MIN_1",
  "joinQuality": "JOINED",
  "siteId": "SITE-001",
  "cellId": "CELL-001",
  "gridId": "wx4g0e",
  "numChrEvents": 10,
  "avgPrbUsageDl": 0.7
}
```

```json
{
  "detectionTs": 1000,
  "eventTs": 900,
  "siteId": "SITE-001",
  "cellId": "CELL-001",
  "gridId": "wx4g0e",
  "anomalyType": "LOW_SIGNAL",
  "severity": "HIGH",
  "contextJson": "{}"
}
```

- [ ] **Step 2: Implement StarRocks query service**

SQL targets:

```sql
SELECT * FROM kpi_1m WHERE window_start_ts >= ? AND window_end_ts <= ? ORDER BY window_start_ts DESC LIMIT ?
SELECT * FROM kpi_5m WHERE window_start_ts >= ? AND window_end_ts <= ? ORDER BY window_start_ts DESC LIMIT ?
SELECT * FROM cell_anomaly_events WHERE detection_ts >= ? AND detection_ts <= ? ORDER BY detection_ts DESC LIMIT ?
SELECT * FROM grid_anomaly_events WHERE detection_ts >= ? AND detection_ts <= ? ORDER BY detection_ts DESC LIMIT ?
```

Append `site_id`, `cell_id`, `grid_id`, `join_quality`, `severity`, and `anomaly_type` predicates only when query parameters are present. Use prepared statements for every value.

- [ ] **Step 3: Add runtime config endpoint**

Return:

```json
{
  "dynamicBalancingEnabled": false,
  "resultQueryLayer": "starrocks",
  "kpiStorage": "iceberg",
  "anomalyStorage": "starrocks"
}
```

- [ ] **Step 4: Map sink latency summaries**

`/api/results/sink-latency` returns:

```json
{
  "sinkName": "iceberg-kpi-1m",
  "sinkType": "iceberg",
  "dataset": "kpi_1m",
  "windowKind": "MIN_1",
  "records": 120,
  "bytes": 12000,
  "durationMs": 45,
  "p50Ms": 30,
  "p95Ms": 45,
  "p99Ms": 80,
  "failureCount": 0,
  "lastError": "",
  "checkpointId": 42,
  "updatedAt": "2026-07-03T10:00:00Z"
}
```

- [ ] **Step 5: Run tests**

Run:

```powershell
mvn -pl observability-api -am test
```

Expected: all API tests pass.

- [ ] **Step 6: Commit**

```powershell
git add observability-api
git commit -m "feat(api): expose result and sink latency queries"
```

---

## Task 11: Frontend Result Pages

**Files:**
- Modify: `frontend/src/types/observability.ts`
- Modify: `frontend/src/api/client.ts`
- Modify: `frontend/src/App.tsx`
- Add: `frontend/src/pages/KpiResults.tsx`
- Add: `frontend/src/pages/CellAnomalies.tsx`
- Add: `frontend/src/pages/GridAnomalies.tsx`
- Add: `frontend/src/pages/SinkLatency.tsx`
- Modify: `frontend/src/pages/MigrationTimeline.tsx`
- Modify tests: `frontend/src/App.test.tsx`

- [ ] **Step 1: Write failing UI tests**

Assert nav labels:

```tsx
expect(screen.getByText('流处理总览')).toBeInTheDocument();
expect(screen.getByText('KPI 1m')).toBeInTheDocument();
expect(screen.getByText('KPI 5m')).toBeInTheDocument();
expect(screen.getByText('小区异常')).toBeInTheDocument();
expect(screen.getByText('栅格异常')).toBeInTheDocument();
expect(screen.getByText('Sink 耗时')).toBeInTheDocument();
expect(screen.getByText('执行历史')).toBeInTheDocument();
expect(screen.getByText('指标面板')).toBeInTheDocument();
```

Mock `/api/runtime/config` with:

```json
{ "dynamicBalancingEnabled": false }
```

Expected: `负载迁移` is not rendered.

- [ ] **Step 2: Add API client methods**

Methods:

```ts
fetchKpiResults(windowKind: '1m' | '5m', params: ResultQueryParams): Promise<KpiResultRow[]>
fetchCellAnomalies(params: AnomalyQueryParams): Promise<AnomalyResultRow[]>
fetchGridAnomalies(params: AnomalyQueryParams): Promise<AnomalyResultRow[]>
fetchSinkLatency(): Promise<SinkLatencySummary[]>
fetchRuntimeConfig(): Promise<RuntimeConfig>
```

- [ ] **Step 3: Add business result tabs**

`App.tsx` page keys:

```ts
type PageKey = 'flow' | 'kpi1m' | 'kpi5m' | 'cellAnomalies' | 'gridAnomalies' | 'sinkLatency' | 'runs' | 'metrics' | 'migrations';
```

Show `migrations` only when runtime config says dynamic balancing is enabled.

- [ ] **Step 4: Implement KPI pages**

Controls:

```text
time range, siteId, cellId, joinQuality
```

Table columns:

```text
windowStartTs, windowEndTs, joinQuality, siteId, cellId, gridId, numChrEvents, numUsers, avgRsrp, avgSinr, avgPrbUsageDl, throughputDlMbpsAvg, dropRate, hoSuccessRate, attachSuccessRate
```

- [ ] **Step 5: Implement anomaly pages**

Cell anomaly controls:

```text
time range, siteId, cellId, severity, anomalyType
```

Grid anomaly controls:

```text
time range, gridId, severity, anomalyType
```

Grid page must render a table first. If coordinates exist, render a compact scatter panel using CSS grid or canvas without adding new GIS dependencies.

- [ ] **Step 6: Implement sink latency page**

Columns:

```text
sinkName, sinkType, dataset, windowKind, records, bytes, durationMs, p50Ms, p95Ms, p99Ms, failureCount, checkpointId, updatedAt
```

- [ ] **Step 7: Run tests**

Run:

```powershell
cd frontend
npm test -- --run
npm run build
```

Expected: tests and production build pass.

- [ ] **Step 8: Commit**

```powershell
git add frontend
git commit -m "feat(frontend): add KPI anomaly and sink latency views"
```

---

## Task 12: Retention Maintenance

**Files:**
- Add: `scripts/retention-maintenance.sh`
- Add: `flink-job/src/main/java/com/fdb/job/maintenance/IcebergRetentionTool.java`
- Add: `flink-job/src/test/java/com/fdb/job/maintenance/IcebergRetentionToolTest.java`
- Modify: `scripts/e2e-summary-lib.sh`

- [ ] **Step 1: Write failing retention command tests**

Test `IcebergRetentionTool` argument parsing:

```java
IcebergRetentionTool.Options options = IcebergRetentionTool.Options.parse(new String[] {
    "--warehouse", "hdfs://namenode:8020/warehouse/iceberg",
    "--database", "fdb",
    "--table", "cell_kpi",
    "--older-than-ms", "3600000",
    "--max-bytes", "10737418240"
});

assertThat(options.database()).isEqualTo("fdb");
assertThat(options.maxBytes()).isEqualTo(10_737_418_240L);
```

- [ ] **Step 2: Add maintenance script**

`scripts/retention-maintenance.sh` inputs:

```bash
RETENTION_MS=${FDB_RETENTION_MS:-3600000}
RETENTION_BYTES=${FDB_RETENTION_BYTES:-10737418240}
HDFS_URI=${FDB_HDFS_URI:-hdfs://namenode:8020}
```

Actions:

```text
1. Alter delete topics to retention.ms=3600000 and retention.bytes=10737418240.
2. Delete `/warehouse/fdb/cell_kpi/window_kind=*/dt=*/hour=*` directories older than the current hour minus one hour.
3. Run IcebergRetentionTool for `fdb.cell_kpi`.
4. Run StarRocks partition maintenance SQL for `cell_anomaly_events` and `grid_anomaly_events`.
5. Delete local `docker/data/observability-runs` files older than one hour.
```

- [ ] **Step 3: Implement Iceberg tool**

The tool must:

```text
load HadoopCatalog
load fdb.cell_kpi
expire snapshots older than now - retention
delete orphan files older than now - retention
print table location and approximate data bytes
return non-zero if approximate data bytes remains above max-bytes after expiration
```

- [ ] **Step 4: Run tests**

Run:

```powershell
mvn -pl flink-job -am test -Dtest=IcebergRetentionToolTest
bash -n scripts/retention-maintenance.sh
```

Expected: Java test passes and shell syntax passes.

- [ ] **Step 5: Commit**

```powershell
git add scripts flink-job/src/main/java/com/fdb/job/maintenance flink-job/src/test/java/com/fdb/job/maintenance
git commit -m "feat(storage): add retention maintenance tooling"
```

---

## Task 13: E2E and Documentation Refresh

**Files:**
- Modify: `scripts/e2e-smoke-test.sh`
- Modify: `scripts/e2e-summary-lib.sh`
- Modify: `scripts/infra-smoke-test.sh`
- Modify: `README.md`
- Modify: `docs/superpowers/specs/2026-04-29-flink-data-balance-design.md` only if implementation differs from the spec.

- [ ] **Step 1: Update e2e simulator modes**

Use:

```bash
java -jar simulator/target/simulator-0.1.0-SNAPSHOT.jar cfg > logs-cfg.log 2>&1 &
java -jar simulator/target/simulator-0.1.0-SNAPSHOT.jar pm > logs-pm.log 2>&1 &
java -jar simulator/target/simulator-0.1.0-SNAPSHOT.jar chr > logs-chr.log 2>&1 &
```

Summary topics:

```bash
summary_kafka_topic "cfg-config"
summary_kafka_topic "pm-stats"
summary_kafka_topic "chr-events"
summary_kafka_topic "cell-kpi-1m"
summary_kafka_topic "cell-kpi-5m"
summary_kafka_topic "cell-anomaly-events"
summary_kafka_topic "grid-anomaly-events"
```

- [ ] **Step 2: Replace MySQL checks with StarRocks checks**

Remove `summary_mysql_kpi`. Add:

```bash
summary_starrocks_query "KPI 1m rows" "SELECT COUNT(*) FROM fdb.kpi_1m"
summary_starrocks_query "Cell anomaly rows" "SELECT COUNT(*) FROM fdb.cell_anomaly_events"
summary_starrocks_query "Grid anomaly rows" "SELECT COUNT(*) FROM fdb.grid_anomaly_events"
```

- [ ] **Step 3: Add dynamic-disabled DAG assertion**

For default e2e:

```bash
curl -s http://localhost:8081/jobs/overview | grep -v "routing-assigner"
curl -s http://localhost:8081/jobs/overview | grep -v "vbucket-load-meter"
curl -s http://localhost:8081/jobs/overview | grep -v "load-coordinator"
```

Expected: those strings are absent.

- [ ] **Step 4: Run validation**

Run:

```powershell
mvn -q test
docker compose -f docker/docker-compose.yml --profile e2e config
bash scripts/e2e-smoke-test.sh
```

Expected:

```text
PM messages present
CFG baseline present
KPI 1m Iceberg files present
KPI 5m Iceberg files present
StarRocks views query KPI rows
StarRocks internal tables query anomaly rows
Sink latency appears at /api/results/sink-latency
Flink DAG has no dynamic-balancing vertices by default
```

- [ ] **Step 5: Run GitNexus detect changes before final commit**

Use:

```text
gitnexus_detect_changes({ scope: "staged" })
```

Expected: affected flows are Flink job pipeline, simulator, observability API, frontend, and scripts. Risk is acceptable after all tests above pass.

- [ ] **Step 6: Commit**

```powershell
git add scripts README.md docs docker common simulator flink-job observability-api frontend
git commit -m "feat: implement PM KPI StarRocks observability pipeline"
```

---

## Self-Review

Spec coverage:

- PM naming and no MR usage: Task 2 and Task 13.
- CFG naming and `cellId` keyed updates: Task 2 and Task 6.
- Dynamic balancing default disabled and hidden UI: Task 5 and Task 11.
- CHR/PM separate 1-minute facts and 2-minute full join: Task 6.
- 5-minute KPI from 1-minute KPI: Task 7.
- KPI in Iceberg, queried through StarRocks views: Task 3, Task 10, Task 13.
- Cell and grid anomaly visibility and StarRocks internal tables: Task 8, Task 10, Task 11.
- Sink write latency in observability console: Task 4, Task 9, Task 10, Task 11.
- 1h and 10GB retention: Task 3 and Task 12.
- Parallelism and TaskManager slots 4, split KPI windows and heavy sinks: Task 3, Task 5, Task 9.
- Shared infra reuse and no local MySQL: Task 3.

Placeholder scan:

- This plan contains no unresolved placeholder markers or deferred implementation instructions.

Type consistency:

- `PmStat`, `CfgConfig`, `JoinQuality`, `ChrMinuteFact`, `PmMinuteFact`, `MinuteFactEnvelope`, `MinuteKpiJoinFunction`, `CellKpiRollupAggregator`, `StarRocksSinks`, and `SinkLatencyProbe` are introduced before use in later tasks.

# Merge PM Fact Branch Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Merge the business target-state work from `impl/flink-data-balance` into current `master` while preserving the current unified `scripts/deploy.sh` local/external deployment workflow and shared-infra assumptions.

**Architecture:** Do not directly merge `impl/flink-data-balance` into `master`. Port the branch by functional slices: PM/CFG naming, CHR/PM minute facts and full join, StarRocks result sinks, observability query APIs, frontend result pages, and retention/tooling integration. The current `master` deployment entrypoint remains authoritative; old branch scripts are only used as source material.

**Tech Stack:** Java 21, Maven multi-module project, Apache Flink, Kafka, Avro, StarRocks JDBC/streaming sinks, Hive/HDFS/Iceberg, shell scripts, Vite/React/Ant Design, GitNexus.

---

## Current Branch Facts

- Current production branch: `master`
- Source branch to port: `impl/flink-data-balance`
- Merge base: `972bf26 chore: ignore local worktrees`
- `master` unique commits: 10, mainly unified deploy/local/external-yarn/shared-infra/aging fixes
- `impl/flink-data-balance` unique commits: 13, mainly PM/CFG naming, minute fact join, StarRocks result APIs, frontend result pages
- Important constraint: keep `scripts/deploy.sh` as the target deployment entrypoint. Do not resurrect `dev-up.sh`, `dev-down.sh`, or old `e2e-smoke-test.sh` as primary orchestration scripts.

## File Ownership Map

### Business Model And Avro

- Rename/modify:
  - `common/src/main/avro/MrStat.avsc` -> `common/src/main/avro/PmStat.avsc`
  - `common/src/main/avro/CmConfig.avsc` -> `common/src/main/avro/CfgConfig.avsc`
  - `common/src/test/java/com/fdb/common/avro/MrStatSchemaTest.java` -> `common/src/test/java/com/fdb/common/avro/PmStatSchemaTest.java`
  - `common/src/test/java/com/fdb/common/avro/CmConfigSchemaTest.java` -> `common/src/test/java/com/fdb/common/avro/CfgConfigSchemaTest.java`
  - `common/src/main/avro/CellKpi.avsc`
  - `common/src/main/java/com/fdb/common/metrics/StageMetricSample.java`
  - `common/src/test/java/com/fdb/common/metrics/StageMetricSampleTest.java`

### Simulator

- Rename/modify:
  - `simulator/src/main/java/com/fdb/simulator/MrSimulator.java` -> `simulator/src/main/java/com/fdb/simulator/PmSimulator.java`
  - `simulator/src/main/java/com/fdb/simulator/CmSimulator.java` -> `simulator/src/main/java/com/fdb/simulator/CfgSimulator.java`
  - `simulator/src/main/resources/sim-mr.yaml` -> `simulator/src/main/resources/sim-pm.yaml`
  - `simulator/src/main/resources/sim-cm.yaml` -> `simulator/src/main/resources/sim-cfg.yaml`
  - `simulator/src/main/java/com/fdb/simulator/SimulatorMain.java`

### Flink Job

- Create/port:
  - `flink-job/src/main/java/com/fdb/job/ChrMinuteFact.java`
  - `flink-job/src/main/java/com/fdb/job/PmMinuteFact.java`
  - `flink-job/src/main/java/com/fdb/job/MinuteFactEnvelope.java`
  - `flink-job/src/main/java/com/fdb/job/ChrMinuteFactWindowFunction.java`
  - `flink-job/src/main/java/com/fdb/job/PmMinuteFactWindowFunction.java`
  - `flink-job/src/main/java/com/fdb/job/MinuteKpiJoinFunction.java`
  - `flink-job/src/main/java/com/fdb/job/CellKpiRollupAggregator.java`
  - `flink-job/src/main/java/com/fdb/job/SinkLatencyProbe.java`
  - `flink-job/src/main/java/com/fdb/job/StarRocksSinks.java`
  - `flink-job/src/main/java/com/fdb/job/maintenance/IcebergRetentionTool.java`

- Modify:
  - `flink-job/src/main/java/com/fdb/job/FlinkJobMain.java`
  - `flink-job/src/main/java/com/fdb/job/EnrichmentProcessFunction.java`
  - `flink-job/src/main/java/com/fdb/job/EnrichedChr.java`
  - `flink-job/src/main/java/com/fdb/job/InputEnvelope.java`
  - `flink-job/src/main/java/com/fdb/job/RoutedEnvelope.java`
  - `flink-job/src/main/java/com/fdb/job/RoutingAssigner.java`
  - `flink-job/src/main/java/com/fdb/job/VBucketLoadMeter.java`
  - `flink-job/src/main/java/com/fdb/job/coordinator/RebalancePolicy.java`
  - `flink-job/src/main/java/com/fdb/job/AnomalyDetector.java`
  - `flink-job/src/main/java/com/fdb/job/CellKpiIcebergMapper.java`
  - `flink-job/src/main/java/com/fdb/job/IcebergConfig.java`
  - `flink-job/src/main/java/com/fdb/job/IcebergSinks.java`
  - `flink-job/pom.xml`

- Delete only after replacement is compiling:
  - `flink-job/src/main/java/com/fdb/job/JdbcSinks.java`
  - `flink-job/src/main/java/com/fdb/job/SinkPerformanceProbe.java`

### Observability API

- Create/port:
  - `observability-api/src/main/java/com/fdb/observability/model/AnomalyResultRow.java`
  - `observability-api/src/main/java/com/fdb/observability/model/KpiResultRow.java`
  - `observability-api/src/main/java/com/fdb/observability/model/SinkLatencySummary.java`
  - `observability-api/src/main/java/com/fdb/observability/service/StarRocksQueryService.java`

- Modify:
  - `observability-api/src/main/java/com/fdb/observability/ObservabilityApiMain.java`
  - `observability-api/src/main/java/com/fdb/observability/service/ObservabilitySnapshotService.java`
  - `observability-api/pom.xml`

### Frontend

- Create/port:
  - `frontend/src/pages/KpiResults.tsx`
  - `frontend/src/pages/CellAnomalies.tsx`
  - `frontend/src/pages/GridAnomalies.tsx`
  - `frontend/src/pages/SinkLatency.tsx`
  - `frontend/src/components/flowEdges.ts`
  - `frontend/src/components/flowEdges.test.ts`

- Modify:
  - `frontend/src/App.tsx`
  - `frontend/src/App.test.tsx`
  - `frontend/src/api/client.ts`
  - `frontend/src/components/StreamingFlowGraph.tsx`
  - `frontend/src/types/observability.ts`

### Deployment, DDL, And Docs

- Modify current-master files:
  - `scripts/deploy.sh`
  - `scripts/init-kafka-topics.sh`
  - `scripts/init-starrocks.sql`
  - `scripts/init-hive.sh`
  - `scripts/e2e-summary-lib.sh`
  - `scripts/test-e2e-summary-lib.sh`
  - `docker/docker-compose.yml`
  - `docs/hive-schema.q`
  - `README.md`

- Add only if still needed after integrating into `deploy.sh`:
  - `scripts/retention-maintenance.sh`
  - `scripts/test-retention-maintenance.sh`

- Do not restore as primary entrypoints:
  - `scripts/dev-up.sh`
  - `scripts/dev-down.sh`
  - old standalone `scripts/e2e-smoke-test.sh`

---

### Task 1: Create Integration Branch And Baseline

**Files:**
- No code files modified.

- [ ] **Step 1: Confirm clean worktree**

Run:

```bash
git status --short
```

Expected: no output. If output exists, stop and decide whether the changes belong to this merge.

- [ ] **Step 2: Create integration branch from current master**

Run:

```bash
git switch master
git pull --ff-only
git switch -c integrate-pm-fact-branch
```

Expected: new branch `integrate-pm-fact-branch`.

- [ ] **Step 3: Capture branch comparison**

Run:

```bash
git merge-base master impl/flink-data-balance
git rev-list --left-right --count master...impl/flink-data-balance
git diff --stat master...impl/flink-data-balance
```

Expected: merge base remains `972bf26...`; counts are around `10 13`; diff is large.

- [ ] **Step 4: Run baseline verification on master**

Run:

```bash
mvn test
cd frontend && npm test -- --run && npm run build
cd ..
docker compose -f docker/docker-compose.yml --profile e2e config
```

Expected: Maven tests pass, frontend tests/build pass, compose config renders.

- [ ] **Step 5: Commit baseline marker only if generated files changed**

Run:

```bash
git status --short
```

Expected: no tracked changes. Do not commit if no files changed.

---

### Task 2: Port PM/CFG Domain Rename

**Files:**
- Rename: `common/src/main/avro/MrStat.avsc` -> `common/src/main/avro/PmStat.avsc`
- Rename: `common/src/main/avro/CmConfig.avsc` -> `common/src/main/avro/CfgConfig.avsc`
- Rename: `simulator/src/main/java/com/fdb/simulator/MrSimulator.java` -> `simulator/src/main/java/com/fdb/simulator/PmSimulator.java`
- Rename: `simulator/src/main/java/com/fdb/simulator/CmSimulator.java` -> `simulator/src/main/java/com/fdb/simulator/CfgSimulator.java`
- Modify: `flink-job/src/main/java/com/fdb/job/*`
- Modify: `observability-api/src/main/java/com/fdb/observability/*`
- Modify: `frontend/src/**/*`
- Modify: `scripts/deploy.sh`
- Modify: `scripts/init-kafka-topics.sh`
- Modify: `README.md`

- [ ] **Step 1: Run GitNexus impact/context before Java symbol edits**

Run:

```bash
npx gitnexus analyze
```

Then inspect affected symbols with GitNexus for:

```text
MrStat
CmConfig
FlinkJobMain
EnrichmentProcessFunction
InputEnvelope
StageMetricSample
```

Expected: no HIGH/CRITICAL risk that blocks a mechanical rename. If GitNexus impact tooling is unavailable, record that limitation and use local references from `rg` as the fallback.

- [ ] **Step 2: Cherry-pick the domain rename without committing**

Run:

```bash
git cherry-pick --no-commit 3a8d66f
```

Expected: conflicts are likely in scripts/docs. Resolve by keeping current master deployment architecture and the PM/CFG names from the branch.

- [ ] **Step 3: Normalize topic/env names in current deployment flow**

Required target names:

```text
pm-stats
cfg-config
pm-dlq
cfg-dlq
pm-source
cfg-source
FDB_PM_TOPIC
FDB_CFG_TOPIC
FDB_PM_RETENTION_MS
```

Required compatibility rule: do not keep `FDB_MR_*` as the primary documented variable. If temporary fallback is necessary for compatibility, it must be internal only and documented as deprecated.

- [ ] **Step 4: Remove old MR/CM Java references**

Run:

```bash
rg -n "\bMrStat\b|\bCmConfig\b|mr-source|cm-source|mr-stats|cm-config|MrSimulator|CmSimulator" common simulator flink-job observability-api frontend scripts docker README.md -S
```

Expected: no production-code matches. Matches in historical docs under `docs/superpowers/plans` or `docs/superpowers/specs` may remain if they are clearly historical.

- [ ] **Step 5: Run focused tests**

Run:

```bash
mvn -pl common,simulator,flink-job,observability-api -am test
```

Expected: compilation succeeds and tests pass.

- [ ] **Step 6: Commit domain rename**

Run:

```bash
git add common simulator flink-job observability-api frontend scripts docker README.md
git commit -m "refactor: use PM and CFG domain names"
```

Expected: commit contains rename and naming updates only, not the minute fact join.

---

### Task 3: Add CellKpi Join Quality And Sink Latency Metrics

**Files:**
- Modify: `common/src/main/avro/CellKpi.avsc`
- Modify: `common/src/main/java/com/fdb/common/metrics/StageMetricSample.java`
- Test: `common/src/test/java/com/fdb/common/avro/CellKpiSchemaTest.java`
- Test: `common/src/test/java/com/fdb/common/metrics/StageMetricSampleTest.java`

- [ ] **Step 1: Cherry-pick schema/metrics commit without committing**

Run:

```bash
git cherry-pick --no-commit d3b59ea
```

Expected: conflicts possible in `StageMetricSample.java`; resolve with the branch model but keep current master package structure.

- [ ] **Step 2: Verify `CellKpi` schema contains join quality**

Required schema field:

```json
{ "name": "joinQuality", "type": { "type": "enum", "name": "JoinQuality", "symbols": ["JOINED", "CHR_ONLY", "PM_ONLY"] } }
```

Expected: generated Avro class exposes `getJoinQuality()` and `setJoinQuality(...)`.

- [ ] **Step 3: Verify sink latency metrics model**

Required metric fields in `StageMetricSample`:

```text
stageId
label
status
inEps
outEps
latencyP50Ms
latencyP95Ms
watermarkLagMs
dlqCount
summary
updatedAt
```

If the branch added sink-specific fields, preserve them only if `ObservabilitySnapshotService` consumes them. Avoid duplicating row-count metrics in both stage and sink summaries.

- [ ] **Step 4: Run common tests**

Run:

```bash
mvn -pl common -am test
```

Expected: Avro schema tests and metrics tests pass.

- [ ] **Step 5: Commit schema/metrics**

Run:

```bash
git add common
git commit -m "feat(common): add KPI join quality and sink latency metrics"
```

---

### Task 4: Port CHR/PM Minute Fact Full Join

**Files:**
- Create: `flink-job/src/main/java/com/fdb/job/ChrMinuteFact.java`
- Create: `flink-job/src/main/java/com/fdb/job/PmMinuteFact.java`
- Create: `flink-job/src/main/java/com/fdb/job/MinuteFactEnvelope.java`
- Create: `flink-job/src/main/java/com/fdb/job/ChrMinuteFactWindowFunction.java`
- Create: `flink-job/src/main/java/com/fdb/job/PmMinuteFactWindowFunction.java`
- Create: `flink-job/src/main/java/com/fdb/job/MinuteKpiJoinFunction.java`
- Modify: `flink-job/src/main/java/com/fdb/job/FlinkJobMain.java`
- Modify: `flink-job/src/main/java/com/fdb/job/EnrichmentProcessFunction.java`
- Test: `flink-job/src/test/java/com/fdb/job/ChrMinuteFactWindowFunctionTest.java`
- Test: `flink-job/src/test/java/com/fdb/job/PmMinuteFactWindowFunctionTest.java`
- Test: `flink-job/src/test/java/com/fdb/job/MinuteKpiJoinFunctionTest.java`
- Test: `flink-job/src/test/java/com/fdb/job/FlinkJobMainTest.java`

- [ ] **Step 1: Run GitNexus impact/context before Flink pipeline edits**

Inspect:

```text
FlinkJobMain
EnrichmentProcessFunction
KpiAggregator
CellKpiWindowFunction
AnomalyDetector
```

Expected: document affected flows: source ingestion, enrichment, anomaly detection, KPI output.

- [ ] **Step 2: Cherry-pick dynamic-balancing optional commit without committing**

Run:

```bash
git cherry-pick --no-commit 9a7a013
```

Expected: adapt conflicts so `FDB_DYNAMIC_BALANCING_ENABLED=false` uses direct `keyBy(cellId)` and does not require `lb-routing` or `lb-heartbeat`.

- [ ] **Step 3: Cherry-pick minute fact join commit without committing**

Run:

```bash
git cherry-pick --no-commit 37b17a4
```

Expected: conflicts in `FlinkJobMain.java` are likely. Preserve current master checkpointing, Iceberg config, shared-infra env names, and current sink setup until StarRocks sink task.

- [ ] **Step 4: Confirm target Flink topology**

Required target flow:

```text
chr-source -> chr-1m-fact
pm-source  -> pm-1m-fact
cfg-source -> cfg-minute-env
chr fact + pm fact + cfg -> MinuteKpiJoinFunction -> cellKpi1m
cellKpi1m -> 5m rollup in Task 5
```

Expected: `kpi-1m-full-join` exists. Old `kpi-1m` over `EnrichedChr` must not remain as the primary KPI path.

- [ ] **Step 5: Run focused Flink tests**

Run:

```bash
mvn -pl flink-job -am test -Dtest=ChrMinuteFactWindowFunctionTest,PmMinuteFactWindowFunctionTest,MinuteKpiJoinFunctionTest,FlinkJobMainTest
```

Expected: focused tests pass.

- [ ] **Step 6: Run full Flink module tests**

Run:

```bash
mvn -pl flink-job -am test
```

Expected: all Flink tests pass.

- [ ] **Step 7: Commit minute fact join**

Run:

```bash
git add flink-job
git commit -m "feat(flink): join CHR and PM minute facts"
```

---

### Task 5: Port 5m Rollup From 1m KPI

**Files:**
- Create: `flink-job/src/main/java/com/fdb/job/CellKpiRollupAggregator.java`
- Modify: `flink-job/src/main/java/com/fdb/job/FlinkJobMain.java`
- Test: `flink-job/src/test/java/com/fdb/job/CellKpiRollupAggregatorTest.java`

- [ ] **Step 1: Cherry-pick 5m rollup commit without committing**

Run:

```bash
git cherry-pick --no-commit fa03c55
```

Expected: `cellKpi5m` is derived from `cellKpi1m`, not from raw/enriched CHR.

- [ ] **Step 2: Verify rollup semantics**

Required behavior:

```text
MIN_5 output aggregates five MIN_1 windows by cellId.
JOINED if any child window has both CHR and PM evidence.
CHR_ONLY if only CHR evidence exists.
PM_ONLY if only PM evidence exists.
```

- [ ] **Step 3: Run tests**

Run:

```bash
mvn -pl flink-job -am test -Dtest=CellKpiRollupAggregatorTest,FlinkJobMainTest
```

Expected: tests pass.

- [ ] **Step 4: Commit 5m rollup**

Run:

```bash
git add flink-job
git commit -m "feat(flink): roll up 5m KPI from 1m KPI"
```

---

### Task 6: Port StarRocks Sinks And Remove Project MySQL Path

**Files:**
- Create: `flink-job/src/main/java/com/fdb/job/StarRocksSinks.java`
- Modify: `flink-job/src/main/java/com/fdb/job/FlinkJobMain.java`
- Modify: `flink-job/pom.xml`
- Modify: `docker/docker-compose.yml`
- Modify: `scripts/init-starrocks.sql`
- Modify: `scripts/deploy.sh`
- Delete: `flink-job/src/main/java/com/fdb/job/JdbcSinks.java`
- Delete if still present: `scripts/init-mysql.sql`, `docs/mysql-schema.sql`
- Test: `flink-job/src/test/java/com/fdb/job/StarRocksSinksTest.java`

- [ ] **Step 1: Cherry-pick shared-StarRocks commit without committing**

Run:

```bash
git cherry-pick --no-commit 05fa918
```

Expected: significant conflicts in scripts. Keep current `scripts/deploy.sh`; do not restore old `dev-up.sh` as primary.

- [ ] **Step 2: Cherry-pick StarRocks anomaly sink commit without committing**

Run:

```bash
git cherry-pick --no-commit a39bdae
```

Expected: `StarRocksSinks` contains KPI and anomaly sinks. `FlinkJobMain` writes cell/grid anomalies to StarRocks.

- [ ] **Step 3: Adapt DDL to current shared StarRocks**

Update `scripts/init-starrocks.sql` so it creates:

```text
cell_kpi
cell_anomaly_events
grid_anomaly_events
```

Required `cell_kpi` columns include:

```text
window_start_ts
window_end_ts
window_kind
join_quality
site_id
cell_id
grid_id
num_chr_events
num_users
avg_rsrp
avg_sinr
avg_prb_usage_dl
throughput_dl_mbps_avg
drop_rate
ho_success_rate
attach_success_rate
```

- [ ] **Step 4: Adapt `scripts/deploy.sh init/check/status/prune`**

Required changes:

```text
init: create pm-stats, cfg-config, cell/grid anomaly topics, StarRocks DDL
check: verify shared StarRocks reachability
status: report cell_kpi and anomaly row ranges
prune: prune StarRocks KPI/anomaly tables using current master aging safeguards
```

Do not use old `scripts/init-starrocks.sh` as a separate required entrypoint unless `deploy.sh` calls it internally.

- [ ] **Step 5: Run tests and config check**

Run:

```bash
mvn -pl flink-job -am test -Dtest=StarRocksSinksTest,FlinkJobMainTest
docker compose -f docker/docker-compose.yml --profile e2e config
```

Expected: tests pass and compose config renders.

- [ ] **Step 6: Commit StarRocks sink integration**

Run:

```bash
git add flink-job docker scripts docs README.md
git commit -m "feat: write KPI and anomalies to shared StarRocks"
```

---

### Task 7: Port Sink Latency Observability

**Files:**
- Create: `flink-job/src/main/java/com/fdb/job/SinkLatencyProbe.java`
- Delete: `flink-job/src/main/java/com/fdb/job/SinkPerformanceProbe.java`
- Create: `observability-api/src/main/java/com/fdb/observability/model/SinkLatencySummary.java`
- Modify: `observability-api/src/main/java/com/fdb/observability/service/ObservabilitySnapshotService.java`
- Modify: `observability-api/src/main/java/com/fdb/observability/ObservabilityApiMain.java`
- Test: `flink-job/src/test/java/com/fdb/job/SinkLatencyProbeTest.java`
- Test: `observability-api/src/test/java/com/fdb/observability/service/ObservabilitySnapshotServiceTest.java`

- [ ] **Step 1: Cherry-pick sink latency commit without committing**

Run:

```bash
git cherry-pick --no-commit ade0b8b
```

Expected: adapt metric names to PM/CFG and current API model.

- [ ] **Step 2: Confirm API and Prometheus metrics**

Required endpoint:

```text
/api/results/sink-latency
```

Required Prometheus metrics:

```text
fdb_sink_write_rows_total{sink="...",window="..."}
fdb_sink_write_latency_ms{sink="...",window="..."}
```

- [ ] **Step 3: Run tests**

Run:

```bash
mvn -pl flink-job,observability-api -am test -Dtest=SinkLatencyProbeTest,ObservabilitySnapshotServiceTest,PrometheusMetricsTest
```

Expected: tests pass.

- [ ] **Step 4: Commit sink latency**

Run:

```bash
git add flink-job observability-api
git commit -m "feat(observability): record sink latency metrics"
```

---

### Task 8: Port Result Query API

**Files:**
- Create: `observability-api/src/main/java/com/fdb/observability/service/StarRocksQueryService.java`
- Create: `observability-api/src/main/java/com/fdb/observability/model/KpiResultRow.java`
- Create: `observability-api/src/main/java/com/fdb/observability/model/AnomalyResultRow.java`
- Modify: `observability-api/src/main/java/com/fdb/observability/ObservabilityApiMain.java`
- Modify: `observability-api/pom.xml`
- Test: `observability-api/src/test/java/com/fdb/observability/service/StarRocksQueryServiceTest.java`
- Test: `observability-api/src/test/java/com/fdb/observability/ObservabilityResultEndpointsTest.java`

- [ ] **Step 1: Cherry-pick result API commit without committing**

Run:

```bash
git cherry-pick --no-commit b29983e
```

Expected: API exposes KPI/anomaly result endpoints backed by shared StarRocks.

- [ ] **Step 2: Verify endpoint list**

Required endpoints:

```text
GET /api/results/kpi/1m
GET /api/results/kpi/5m
GET /api/results/anomalies/cell
GET /api/results/anomalies/grid
GET /api/runtime/config
```

Expected: query parameter validation rejects invalid limits/window filters with HTTP 400.

- [ ] **Step 3: Run observability tests**

Run:

```bash
mvn -pl observability-api -am test
```

Expected: observability API tests pass.

- [ ] **Step 4: Commit result API**

Run:

```bash
git add observability-api
git commit -m "feat(api): expose StarRocks result queries"
```

---

### Task 9: Port Frontend Result Pages

**Files:**
- Create: `frontend/src/pages/KpiResults.tsx`
- Create: `frontend/src/pages/CellAnomalies.tsx`
- Create: `frontend/src/pages/GridAnomalies.tsx`
- Create: `frontend/src/pages/SinkLatency.tsx`
- Create: `frontend/src/components/flowEdges.ts`
- Create: `frontend/src/components/flowEdges.test.ts`
- Modify: `frontend/src/App.tsx`
- Modify: `frontend/src/App.test.tsx`
- Modify: `frontend/src/api/client.ts`
- Modify: `frontend/src/types/observability.ts`
- Modify: `frontend/src/components/StreamingFlowGraph.tsx`

- [ ] **Step 1: Cherry-pick frontend result pages commit without committing**

Run:

```bash
git cherry-pick --no-commit cdd2283
```

Expected: conflicts possible in `App.tsx` and API client. Preserve current frontend layout unless the branch adds required result navigation.

- [ ] **Step 2: Verify frontend pages**

Required menu entries:

```text
KPI 1m
KPI 5m
小区异常
栅格异常
Sink 耗时
```

Required client methods:

```text
fetchKpiResults("1m")
fetchKpiResults("5m")
fetchCellAnomalies()
fetchGridAnomalies()
fetchSinkLatency()
```

- [ ] **Step 3: Run frontend tests/build**

Run:

```bash
cd frontend
npm test -- --run
npm run build
cd ..
```

Expected: tests and build pass.

- [ ] **Step 4: Commit frontend**

Run:

```bash
git add frontend
git commit -m "feat(frontend): add KPI anomaly and sink latency views"
```

---

### Task 10: Integrate Retention Tooling With Current Deploy Script

**Files:**
- Create: `flink-job/src/main/java/com/fdb/job/maintenance/IcebergRetentionTool.java`
- Modify: `flink-job/pom.xml`
- Modify: `scripts/deploy.sh`
- Optional create: `scripts/retention-maintenance.sh`
- Test: `flink-job/src/test/java/com/fdb/job/maintenance/IcebergRetentionToolTest.java`
- Test: `scripts/test-retention-maintenance.sh`

- [ ] **Step 1: Cherry-pick retention commit without committing**

Run:

```bash
git cherry-pick --no-commit ad8e0ee
```

Expected: conflicts with current `master` aging logic. Current `master` fix `32e422e` must win where behavior overlaps.

- [ ] **Step 2: Preserve current aging safeguards**

Required behavior:

```text
StarRocks prune uses explicit thresholds and does not delete recent rows.
HDFS/Iceberg prune skips in-progress metadata/token files that are still active.
Status reports min/max timestamps and row counts after prune.
```

- [ ] **Step 3: Wire retention through `scripts/deploy.sh prune`**

Required command:

```bash
sh scripts/deploy.sh local prune
```

and, for external:

```bash
sh scripts/deploy.sh external-yarn prune
```

Expected: no separate script is required for normal operation.

- [ ] **Step 4: Run tests**

Run:

```bash
mvn -pl flink-job -am test -Dtest=IcebergRetentionToolTest
bash scripts/test-retention-maintenance.sh
```

Expected: Java and shell tests pass. If `bash` is unavailable on Windows, run via Git Bash or WSL and record the environment.

- [ ] **Step 5: Commit retention integration**

Run:

```bash
git add flink-job scripts README.md
git commit -m "feat(storage): integrate KPI retention maintenance"
```

---

### Task 11: Reconcile Deployment Scripts And Docs

**Files:**
- Modify: `scripts/deploy.sh`
- Modify: `scripts/init-kafka-topics.sh`
- Modify: `scripts/init-starrocks.sql`
- Modify: `scripts/init-hive.sh`
- Modify: `scripts/e2e-summary-lib.sh`
- Modify: `scripts/test-e2e-summary-lib.sh`
- Modify: `docker/docker-compose.yml`
- Modify: `README.md`
- Modify: `AGENTS.md` only if infrastructure boundaries changed

- [ ] **Step 1: Ensure current target entrypoints**

Required primary commands:

```bash
sh scripts/deploy.sh local check
sh scripts/deploy.sh local init
sh scripts/deploy.sh local submit
sh scripts/deploy.sh local stop
sh scripts/deploy.sh local smoke
sh scripts/deploy.sh local status
sh scripts/deploy.sh local prune
sh scripts/deploy.sh external-yarn check
sh scripts/deploy.sh external-yarn init
sh scripts/deploy.sh external-yarn submit
sh scripts/deploy.sh external-yarn stop
sh scripts/deploy.sh external-yarn smoke
sh scripts/deploy.sh external-yarn status
sh scripts/deploy.sh external-yarn prune
```

Expected: README documents these commands and does not require old `dev-up.sh`/`dev-down.sh` except for any intentionally retained infra-only compatibility note.

- [ ] **Step 2: Ensure shared-infra boundary**

Before modifying compose, inspect:

```bash
Get-ChildItem ..\shared-data-infra
```

Expected: HDFS, Hive, Kafka, ZooKeeper, StarRocks, Prometheus, Grafana stay in `../shared-data-infra`. This project must not reintroduce duplicate Kafka/Hive/HDFS/StarRocks/Prometheus/MySQL infra.

- [ ] **Step 3: Render compose config**

Run:

```bash
docker compose -f docker/docker-compose.yml --profile e2e config
```

Expected: config renders and uses shared external network/services.

- [ ] **Step 4: Run script tests**

Run:

```bash
bash scripts/test-e2e-summary-lib.sh
```

Expected: tests pass.

- [ ] **Step 5: Commit docs/scripts reconciliation**

Run:

```bash
git add scripts docker README.md docs AGENTS.md
git commit -m "chore: reconcile PM pipeline with unified deploy workflow"
```

---

### Task 12: Local Build And Smoke Verification

**Files:**
- No planned edits unless verification finds defects.

- [ ] **Step 1: Full Maven test**

Run:

```bash
mvn test
```

Expected: all Java module tests pass.

- [ ] **Step 2: Frontend verification**

Run:

```bash
cd frontend
npm test -- --run
npm run build
cd ..
```

Expected: tests and build pass.

- [ ] **Step 3: Compose verification**

Run:

```bash
docker compose -f docker/docker-compose.yml --profile e2e config
```

Expected: config renders.

- [ ] **Step 4: Start or reuse shared infra**

Run:

```bash
cd ..\shared-data-infra
sh scripts/infra-up.sh lakehouse lakehouse-tools streaming starrocks observability
cd ..\flink-data-balance
```

Expected: shared infra containers are running and healthy where healthchecks exist.

- [ ] **Step 5: Initialize local target**

Run:

```bash
sh scripts/deploy.sh local check
sh scripts/deploy.sh local init
```

Expected: Kafka topics include `pm-stats` and `cfg-config`; StarRocks tables exist.

- [ ] **Step 6: Submit and smoke**

Run:

```bash
sh scripts/deploy.sh local submit
sh scripts/deploy.sh local smoke
```

Expected: Flink job starts; CHR/PM/CFG sources are healthy; `cell_kpi` MIN_1 rows appear; anomaly topics/tables are queryable.

- [ ] **Step 7: Status and prune**

Run:

```bash
sh scripts/deploy.sh local status
sh scripts/deploy.sh local prune
sh scripts/deploy.sh local status
```

Expected: status reports row ranges; prune removes only expired data according to configured retention.

- [ ] **Step 8: Commit verification fixes**

If verification required fixes:

```bash
git add <changed-files>
git commit -m "fix: stabilize PM pipeline integration"
```

If no fixes were needed, do not create an empty commit.

---

### Task 13: External-YARN Connectivity Verification

**Files:**
- Modify only if verification finds defects:
  - `scripts/deploy.sh`
  - `README.md`

- [ ] **Step 1: Run non-blocking external check**

Run:

```bash
sh scripts/deploy.sh external-yarn check
```

Expected: the command reports missing external infrastructure as warnings or controlled failures according to current policy; it must not assume Docker exists on the external host.

- [ ] **Step 2: Run external init dry/partial path where possible**

Run only when external env vars are available:

```bash
sh scripts/deploy.sh external-yarn init
```

Expected: creates or validates topics/tables/HDFS directories. If external infrastructure is absent, record the skipped checks in README or final notes.

- [ ] **Step 3: Verify README external deployment section**

Required docs:

```text
Flink submission is executed by scripts/deploy.sh external-yarn submit from the deployment host.
YARN must have Flink installed or FLINK_HOME/flink command available.
External Kafka/HDFS/Hive/StarRocks endpoints are configured through .env.
Connectivity checks may be run before the external environment is fully provisioned.
```

- [ ] **Step 4: Commit external docs/fixes**

Run:

```bash
git add scripts README.md
git commit -m "docs: align external-yarn PM deployment checks"
```

Only commit if files changed.

---

### Task 14: Final Risk Review And Push

**Files:**
- No planned edits.

- [ ] **Step 1: Run GitNexus detect changes**

Run:

```bash
npx gitnexus detect_changes
```

Expected: affected flows match the intended scope: common schema, simulator, Flink PM/KPI/anomaly pipeline, observability API, frontend result pages, deploy scripts.

- [ ] **Step 2: Review remaining MR/CM references**

Run:

```bash
rg -n "\bMR\b|\bMr\b|\bmr-\b|\bmr_\b|\bCM\b|\bCm\b|\bcm-\b|\bcm_\b|mr-stats|cm-config" common simulator flink-job observability-api frontend scripts docker README.md -S
```

Expected: no production-code references. Any historical-doc matches must be intentionally retained.

- [ ] **Step 3: Final verification suite**

Run:

```bash
mvn test
cd frontend && npm test -- --run && npm run build
cd ..
docker compose -f docker/docker-compose.yml --profile e2e config
```

Expected: all pass.

- [ ] **Step 4: Summarize commits**

Run:

```bash
git log --oneline master..HEAD
git status --short
```

Expected: clean worktree; commits are logically grouped by the tasks above.

- [ ] **Step 5: Push integration branch**

Run:

```bash
git push -u origin integrate-pm-fact-branch
```

Expected: remote branch is created for review. Do not fast-forward `master` until local smoke and review pass.

---

## Self-Review

**Spec coverage:** This plan covers PM/CFG rename, CHR/PM 1m fact full join, 5m rollup, StarRocks result sinks, sink latency, result APIs, frontend pages, retention, local deployment, external-yarn deployment, and final GitNexus/change verification.

**Known integration risks:**

- `impl/flink-data-balance` was written before current `scripts/deploy.sh` target state. Script conflicts must be resolved in favor of current `deploy.sh`.
- Current running local environment still has `mr-stats`/`cm-config`; after Task 2 and Task 11, local init must create `pm-stats`/`cfg-config`.
- Existing checkpoint/savepoint compatibility from MR/CM to PM/CFG is not guaranteed. Treat this as a new job topology unless explicit state migration is added.
- StarRocks `cell_kpi` schema changes include `join_quality`; existing old rows/tables may need drop/recreate in local smoke.
- Current `master` aging fix `32e422e` must not be overwritten by older retention code from `impl/flink-data-balance`.

**Execution recommendation:** Use an integration branch and commit after every task. If a task creates broad conflicts, abort that cherry-pick and manually port only the needed files from `impl/flink-data-balance` with `git show impl/flink-data-balance:<path>`.

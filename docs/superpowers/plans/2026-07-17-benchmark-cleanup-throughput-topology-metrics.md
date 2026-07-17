# Benchmark Cleanup Throughput Topology Metrics Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make benchmark runs self-contained by cleaning data per run, reporting real Flink throughput rates, and adding topology-service metrics to single-run HTML reports.

**Architecture:** Add an explicit deploy `prepare` hook before each benchmark run, implemented through existing `scripts/deploy.sh` target dispatch. Extend topology-service to emit a small JSON metrics file and have the benchmark runner read it after topology generation. Change Flink throughput collection to use true per-second REST metrics, falling back to zero when rate metrics are unavailable instead of labeling cumulative counters as `/s`.

**Tech Stack:** Java 17, Maven/JUnit 5/AssertJ, Flink REST API, existing shell deploy scripts, Jackson JSON.

---

### Task 1: Per-run Benchmark Prepare Hook

**Files:**
- Modify: `benchmark-runner/src/main/java/com/fdb/benchmark/DeployCommandClient.java`
- Modify: `benchmark-runner/src/main/java/com/fdb/benchmark/ShellDeployCommandClient.java`
- Modify: `benchmark-runner/src/main/java/com/fdb/benchmark/BenchmarkOrchestrator.java`
- Modify: `scripts/deploy.sh`
- Test: `benchmark-runner/src/test/java/com/fdb/benchmark/BenchmarkOrchestratorTest.java`
- Test: `benchmark-runner/src/test/java/com/fdb/benchmark/ShellDeployCommandClientTest.java`

- [x] Add `prepare(BenchmarkRunPlan plan)` to the deploy client.
- [x] Make `BenchmarkOrchestrator.run()` call `deploy.prepare(plan)` before starting topology/simulators.
- [x] Add `prepare` command dispatch to `scripts/deploy.sh` for local and external-yarn targets.
- [x] Implement local prepare as topic reset plus storage prune using existing deploy helpers.
- [x] Implement external-yarn prepare as a conservative prune/init hook without requiring connectivity to pass in local CI.
- [x] Verify with `mvn -pl benchmark-runner -am -Dtest=BenchmarkOrchestratorTest,ShellDeployCommandClientTest test` and `bash scripts/test-deploy-dispatch.sh`.

### Task 2: Real Flink Throughput Rates

**Files:**
- Modify: `benchmark-runner/src/main/java/com/fdb/benchmark/FlinkRestClient.java`
- Modify: `benchmark-runner/src/test/java/com/fdb/benchmark/FlinkRestClientTest.java`
- Modify: `benchmark-runner/src/main/java/com/fdb/benchmark/HtmlReportWriter.java`

- [x] Update tests so `/jobs/{jobId}` cumulative `read-records` is not treated as `/s`.
- [x] Fetch true Flink rate metrics from `/jobs/{jobId}/vertices/{vertexId}/metrics?get=numRecordsInPerSecond,numRecordsOutPerSecond,numBytesInPerSecond,numBytesOutPerSecond,busyTimeMsPerSecond,idleTimeMsPerSecond,backPressuredTimeMsPerSecond`.
- [x] Use rates for `Records In/s`, `Records Out/s`, `Bytes In/s`, `Bytes Out/s`.
- [x] Keep current cumulative counters out of the `/s` columns, and expose them as `Records In Total` and `Records Out Total` in the operator table.
- [x] Verify with `mvn -pl benchmark-runner -am -Dtest=FlinkRestClientTest,HtmlReportWriterTest test`.

### Task 3: Topology Metrics Capture

**Files:**
- Modify: `topology-service/src/main/java/com/fdb/topology/TopologyMain.java`
- Modify: `topology-service/src/main/java/com/fdb/topology/KafkaTopologyPublisher.java`
- Create: `topology-service/src/main/java/com/fdb/topology/TopologyMetrics.java`
- Test: `topology-service/src/test/java/com/fdb/topology/TopologyMainTest.java`

- [x] Add `TopologyMetrics` with generated records, sites, bands, generation duration, publish duration, total duration, publish failures, lat/lon range.
- [x] Make `KafkaTopologyPublisher.publishAll` return published/failure counts.
- [x] Make `TopologyMain` write JSON to `FDB_TOPOLOGY_METRICS_FILE` when set.
- [x] Preserve existing summary logs.
- [x] Verify with `mvn -pl topology-service -am test`.

### Task 4: Benchmark Report Integration

**Files:**
- Modify: `benchmark-runner/src/main/java/com/fdb/benchmark/JavaSimulatorProcessManager.java`
- Create: `benchmark-runner/src/main/java/com/fdb/benchmark/TopologyMetricsSnapshot.java`
- Modify: `benchmark-runner/src/main/java/com/fdb/benchmark/RunObservation.java`
- Modify: `benchmark-runner/src/main/java/com/fdb/benchmark/BenchmarkRunResult.java`
- Modify: `benchmark-runner/src/main/java/com/fdb/benchmark/BenchmarkDecisionEngine.java`
- Modify: `benchmark-runner/src/main/java/com/fdb/benchmark/BenchmarkResultWriter.java`
- Modify: `benchmark-runner/src/main/java/com/fdb/benchmark/HtmlReportWriter.java`
- Test: `benchmark-runner/src/test/java/com/fdb/benchmark/JavaSimulatorProcessManagerTest.java`
- Test: `benchmark-runner/src/test/java/com/fdb/benchmark/HtmlReportWriterTest.java`

- [x] Set `FDB_TOPOLOGY_METRICS_FILE` per run before launching topology-service.
- [x] Read the JSON file after topology-service exits.
- [x] Carry topology metrics through observation/result JSON.
- [x] Add `Topology Generation` table to single-run report.
- [x] Verify with `mvn -pl benchmark-runner -am test`.

### Task 5: Final Verification

**Files:**
- Modify if needed: `README.md`
- Modify if needed: `docs/superpowers/specs/2026-04-29-flink-data-balance-design.md`

- [x] Run `docker compose --env-file .env.local -f docker/docker-compose.yml --profile e2e config`.
- [x] Run `bash scripts/test-deploy-dispatch.sh`.
- [x] Run `mvn -pl benchmark-runner,topology-service -am test`.
- [x] Run GitNexus `detect_changes` before commit.

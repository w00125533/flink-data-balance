# Backpressure Optimization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reduce benchmark backpressure around source/direct-routing/enrichment by isolating diagnostic vertices, reducing unnecessary branch work, lowering user-anomaly intermediate fan-out, sampling probes, and allowing hot-stage parallelism overrides.

**Architecture:** Keep the business dataflow unchanged, but optimize expensive branches. Coverage filtering applies only to the coverage branch; user anomaly detection keeps the same "user + dimension consecutive" semantics inside one keyed process; metrics probes keep report visibility through configurable sampling.

**Tech Stack:** Java 17, Apache Flink DataStream API, Maven, JUnit.

---

### Task 1: Impact Analysis

**Files:**
- Inspect: `flink-job/src/main/java/com/fdb/job/FlinkJobMain.java`
- Inspect: `flink-job/src/main/java/com/fdb/job/anomaly/UserEventCepAnomalyDetector.java`
- Inspect: `flink-job/src/main/java/com/fdb/job/metrics/StageMetricsProbe.java`
- Inspect: `flink-job/src/main/java/com/fdb/job/metrics/SinkLatencyProbe.java`

- [ ] **Step 1: Run GitNexus impact analysis**

Run impact analysis for the Java classes above and check HIGH/CRITICAL risks before editing.

### Task 2: Diagnostic Chaining and Branch Filtering

**Files:**
- Modify: `flink-job/src/main/java/com/fdb/job/FlinkJobMain.java`
- Test: `flink-job/src/test/java/com/fdb/job/FlinkJobMainTest.java`

- [ ] **Step 1: Add tests for diagnostic split boundaries and coverage filter**

Verify that diagnostic chaining applies around enrichment/user/grid branches, and coverage branch contains a low-signal filter before geohash `keyBy`.

- [ ] **Step 2: Implement split boundaries**

Use `disableChainingIfDiagnostic(...)` consistently after enrichment metrics, user anomaly metrics, grid anomaly metrics, late sink, and KPI metrics. Use `startNewChain()` or `disableChaining()` where Flink otherwise fuses adjacent operators in the report.

- [ ] **Step 3: Implement coverage-only pre-filter**

Apply `filter(enriched -> chr.rsrp != null && chr.rsrp < rules.rsrpThreshold())` only before the coverage branch `keyBy`, leaving the main enriched stream unchanged.

### Task 3: User Anomaly Single Operator

**Files:**
- Modify: `flink-job/src/main/java/com/fdb/job/anomaly/UserEventCepAnomalyDetector.java`
- Create: `flink-job/src/main/java/com/fdb/job/anomaly/UserEventAnomalyProcessFunction.java`
- Test: `flink-job/src/test/java/com/fdb/job/anomaly/UserEventCepAnomalyDetectorTest.java`

- [ ] **Step 1: Add behavior tests**

Keep existing behavior: no IMSI means no output; consecutive abnormal events emit one anomaly; normal events reset the streak; dimensions are independent.

- [ ] **Step 2: Implement keyed process function**

`keyBy(imsi)` and store streaks in `MapState<String, DimensionState>`, where the map key is the rule dimension such as `accessFailure`, `handoverFailure`, `rsrp`, `sinr`, or `latencyMs`.

- [ ] **Step 3: Preserve output schema**

Build `AnomalyEvent` through the existing `AnomalyRuleEvaluation` and `AnomalyEventFactory` path to keep fields and IDs stable.

### Task 4: Probe Sampling

**Files:**
- Modify: `flink-job/src/main/java/com/fdb/job/metrics/MetricRuntimeConfig.java`
- Modify: `flink-job/src/main/java/com/fdb/job/metrics/StageMetricsProbe.java`
- Modify: `flink-job/src/main/java/com/fdb/job/metrics/SinkLatencyProbe.java`
- Test: `flink-job/src/test/java/com/fdb/job/metrics/StageMetricsProbeTest.java`
- Test: `flink-job/src/test/java/com/fdb/job/metrics/SinkLatencyProbeTest.java`

- [ ] **Step 1: Add config fields**

Add `FDB_METRICS_SAMPLE_EVERY_RECORDS` and `FDB_SINK_METRICS_SAMPLE_EVERY_RECORDS`, defaulting to `1` to preserve current behavior unless benchmark overrides them.

- [ ] **Step 2: Sample expensive probe work**

Always increment Flink counters and pass records through; only run latency extraction, approximate byte estimation, and Kafka metrics publication for sampled records.

### Task 5: Hot Stage Parallelism

**Files:**
- Modify: `flink-job/src/main/java/com/fdb/job/FlinkJobMain.java`
- Modify: `README.md`
- Modify: `docs/superpowers/specs/2026-04-29-flink-data-balance-design.md`
- Test: `flink-job/src/test/java/com/fdb/job/FlinkJobMainTest.java`

- [ ] **Step 1: Add env resolution helpers**

Add stage-specific parallelism helpers with fallback to `FDB_FLINK_PARALLELISM`.

- [ ] **Step 2: Apply parallelism**

Apply `FDB_FLINK_ENRICHMENT_PARALLELISM`, `FDB_FLINK_USER_ANOMALY_PARALLELISM`, `FDB_FLINK_GRID_ANOMALY_PARALLELISM`, and `FDB_FLINK_KPI_PARALLELISM` to the matching heavy stages.

### Task 6: Verification

**Files:**
- Verify all changed Java modules and docs.

- [ ] **Step 1: Run targeted tests**

Run `mvn -q -pl flink-job -am test`.

- [ ] **Step 2: Run package build**

Run `mvn -q -DskipTests package`.

- [ ] **Step 3: Run GitNexus detect_changes**

Confirm Java symbol impact matches the intended Flink job, anomaly, and metrics scope.

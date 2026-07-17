# Benchmark Single-Run Report Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `benchmark-runner` single-run `report.html` useful for performance diagnosis, and lightly refine the batch `index.html` where it lacks comparable summary detail.

**Architecture:** Keep the runner self-contained. Extend snapshot records with small nested detail records for Flink operators and FDB sink metrics, enrich the REST clients to populate them, and update `HtmlReportWriter` to render metric cards and tables instead of raw `toString()` blocks. Keep raw JSON artifacts written by `BenchmarkResultWriter` as the machine-readable source of truth.

**Tech Stack:** Java 17 records, Jackson, JUnit 5, AssertJ, Maven.

---

### Task 1: Single-Run HTML Structure

**Files:**
- Modify: `benchmark-runner/src/test/java/com/fdb/benchmark/HtmlReportWriterTest.java`
- Modify: `benchmark-runner/src/main/java/com/fdb/benchmark/HtmlReportWriter.java`

- [ ] **Step 1: Write failing HTML assertions**

Add assertions that the single-run page contains the required sections: `Run Summary`, `Flink Resources`, `Operator Throughput`, `Latency`, `Sink & Storage`, `Raw Artifacts`, and links to the JSON files.

- [ ] **Step 2: Run the focused test**

Run: `mvn -pl benchmark-runner -Dtest=HtmlReportWriterTest test`

Expected: FAIL because the current single-run page only renders `Flink Snapshot`, `FDB Metrics Snapshot`, and `Storage Snapshot`.

- [ ] **Step 3: Implement structured single-run sections**

Update `HtmlReportWriter.runReport` to render metric cards and tables for the sections above, using existing snapshot fields first.

- [ ] **Step 4: Verify the test passes**

Run: `mvn -pl benchmark-runner -Dtest=HtmlReportWriterTest test`

Expected: PASS.

### Task 2: Flink Operator Details

**Files:**
- Modify: `benchmark-runner/src/test/java/com/fdb/benchmark/FlinkRestClientTest.java`
- Modify: `benchmark-runner/src/main/java/com/fdb/benchmark/FlinkSnapshot.java`
- Modify: `benchmark-runner/src/main/java/com/fdb/benchmark/FlinkRestClient.java`
- Modify: `benchmark-runner/src/main/java/com/fdb/benchmark/BenchmarkRunnerMain.java`
- Modify: `benchmark-runner/src/test/java/com/fdb/benchmark/BenchmarkResultWriterTest.java`

- [ ] **Step 1: Write failing parser assertions**

Assert that Flink REST parsing captures TaskManager count, slot count, and one operator row with vertex name, parallelism, records in/out, bytes in/out, busy, idle, and backpressure.

- [ ] **Step 2: Run the focused test**

Run: `mvn -pl benchmark-runner -Dtest=FlinkRestClientTest test`

Expected: FAIL because `FlinkSnapshot` has no operator detail list and currently returns zero TaskManagers/slots.

- [ ] **Step 3: Extend the snapshot model and parser**

Add `FlinkOperatorSnapshot` and a `List<FlinkOperatorSnapshot>` field to `FlinkSnapshot`. Parse `/taskmanagers`, `/jobs/{jobId}/vertices`, and vertex metric fields from Flink REST. Keep missing endpoint behavior tolerant by returning zeros and an empty list.

- [ ] **Step 4: Verify parser tests pass**

Run: `mvn -pl benchmark-runner -Dtest=FlinkRestClientTest,BenchmarkResultWriterTest test`

Expected: PASS.

### Task 3: FDB Latency And Sink Details

**Files:**
- Modify: `benchmark-runner/src/test/java/com/fdb/benchmark/ObservabilityClientTest.java`
- Modify: `benchmark-runner/src/main/java/com/fdb/benchmark/FdbMetricsSnapshot.java`
- Modify: `benchmark-runner/src/main/java/com/fdb/benchmark/ObservabilityClient.java`
- Modify: `benchmark-runner/src/main/java/com/fdb/benchmark/BenchmarkRunnerMain.java`
- Modify: `benchmark-runner/src/test/java/com/fdb/benchmark/BenchmarkResultWriterTest.java`

- [ ] **Step 1: Write failing parser assertions**

Assert that Observability parsing keeps stage latency rows and sink latency rows, including p50/p95/p99, records, bytes, and failures when provided.

- [ ] **Step 2: Run the focused test**

Run: `mvn -pl benchmark-runner -Dtest=ObservabilityClientTest test`

Expected: FAIL because the current snapshot stores only aggregate p95 values.

- [ ] **Step 3: Extend the snapshot model and parser**

Add `StageLatencySnapshot` and `SinkLatencySnapshot` lists to `FdbMetricsSnapshot`. Populate them from `/api/flow/status` or `/api/flow/stages`, and `/api/results/sink-latency`.

- [ ] **Step 4: Verify parser tests pass**

Run: `mvn -pl benchmark-runner -Dtest=ObservabilityClientTest,BenchmarkResultWriterTest test`

Expected: PASS.

### Task 4: Batch Summary Page Refinement

**Files:**
- Modify: `benchmark-runner/src/test/java/com/fdb/benchmark/HtmlReportWriterTest.java`
- Modify: `benchmark-runner/src/main/java/com/fdb/benchmark/HtmlReportWriter.java`

- [ ] **Step 1: Write failing index assertions**

Assert that `index.html` includes a batch summary strip with total runs, stable runs, unstable/failed runs, best stable sink/cell level, and a richer run table with checkpoint, backpressure, watermark lag, and storage summary columns.

- [ ] **Step 2: Run the focused test**

Run: `mvn -pl benchmark-runner -Dtest=HtmlReportWriterTest test`

Expected: FAIL because the current index only shows stable bounds, a simple runs table, and recommendations.

- [ ] **Step 3: Implement summary strip and richer columns**

Update `HtmlReportWriter.index` with compact cards and additional table columns using the existing and newly extended snapshot fields.

- [ ] **Step 4: Verify HTML tests pass**

Run: `mvn -pl benchmark-runner -Dtest=HtmlReportWriterTest test`

Expected: PASS.

### Task 5: Full Verification

**Files:**
- No additional source files.

- [ ] **Step 1: Run benchmark-runner tests**

Run: `mvn -pl benchmark-runner test`

Expected: PASS.

- [ ] **Step 2: Run dry-run report generation**

Run: `mvn -pl benchmark-runner -am package` then `FDB_ENV_FILE=.env.local bash scripts/benchmark.sh local --dry-run`

Expected: PASS and generated single-run `report.html` contains the structured sections.

- [ ] **Step 3: Run GitNexus detect_changes**

Run: `npx gitnexus detect_changes --repo "D:\agent-code\flink-data-balance" --scope unstaged`

Expected: affected scope is limited to benchmark runner report generation and this plan.

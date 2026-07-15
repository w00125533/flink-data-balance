# Entity Anomaly CEP Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement entity-aware anomaly detection with cell KPI CEP, user event CEP, and separate cell/user/grid anomaly outputs.

**Architecture:** `AnomalyEvent` becomes an entity-aware schema shared by cell, user, and grid anomaly streams. Flink emits cell anomalies from `CellKpi MIN_1` using CEP, emits user anomalies from enriched CHR using CEP, keeps coverage-hole detection as the grid branch, then routes all three anomaly streams through the selected result sink. Observability API and frontend add user anomalies and display the new entity/window fields.

**Tech Stack:** Java 17, Flink 1.20, Flink CEP, Avro, Kafka, StarRocks JDBC, Iceberg, Hive FileSink, Maven, React 18, TypeScript, Vitest.

---

## File Structure

Modify:

- `common/src/main/avro/AnomalyEvent.avsc` - add entity/window fields and new anomaly enum symbols.
- `common/src/test/java/com/fdb/common/avro/AnomalyEventSchemaTest.java` - validate nullable entity schema and new symbols.
- `flink-job/pom.xml` - add Flink CEP dependency.
- `flink-job/src/main/resources/job-default.yaml` - add anomaly rule defaults.
- `flink-job/src/main/java/com/fdb/job/config/RuleConfig.java` - expand rule config.
- `flink-job/src/main/java/com/fdb/job/config/JobConfig.java` - resolve new rule config values.
- `flink-job/src/main/java/com/fdb/job/enrich/EnrichmentProcessFunction.java` - stop blocking CHR on missing CFG and rename side-output semantics.
- `flink-job/src/main/java/com/fdb/job/FlinkJobMain.java` - rewire anomaly DAG and pass three anomaly streams into result sinks.
- `flink-job/src/main/java/com/fdb/job/anomaly/CoverageHoleDetector.java` - emit entity-aware grid anomaly records.
- `flink-job/src/main/java/com/fdb/job/sink/ResultSinks.java` - accept and attach user anomaly streams.
- `flink-job/src/main/java/com/fdb/job/sink/StarRocksSinks.java` - add entity-aware insert SQL and user anomaly sink.
- `flink-job/src/main/java/com/fdb/job/sink/HiveSinks.java` - add user anomaly output path/sink.
- `flink-job/src/main/java/com/fdb/job/sink/IcebergConfig.java` - add user anomaly table config.
- `flink-job/src/main/java/com/fdb/job/sink/IcebergSinks.java` - add user anomaly table and entity-aware schema.
- `flink-job/src/main/java/com/fdb/job/sink/AnomalyEventIcebergMapper.java` - map entity/window fields.
- `scripts/init-kafka-topics.sh` - add `user-anomaly-events`.
- `scripts/init-starrocks.sql` - rebuild three anomaly tables with entity/window columns.
- `docs/hive-schema.q` - add Hive `user_anomaly_events` table.
- `scripts/init-hive.sh` - keep loading `docs/hive-schema.q`.
- `scripts/retention-maintenance.sh` - include `user_anomaly_events`.
- `scripts/deploy.sh` - include user anomaly table in smoke/summary/retention queries.
- `observability-api/src/main/java/com/fdb/observability/model/AnomalyResultRow.java` - add entity/window/imsi fields.
- `observability-api/src/main/java/com/fdb/observability/service/StarRocksQueryService.java` - add user anomaly query and filters.
- `observability-api/src/main/java/com/fdb/observability/ObservabilityApiMain.java` - add `/api/results/anomalies/user`.
- `observability-api/src/main/java/com/fdb/observability/service/ObservabilitySnapshotService.java` - include user anomaly default sink summaries.
- `frontend/src/types/observability.ts` - add entity/window/imsi fields and query params.
- `frontend/src/api/client.ts` - add `fetchUserAnomalies`.
- `frontend/src/App.tsx` - add user anomaly nav entry/page.
- `frontend/src/pages/CellAnomalies.tsx` - show entity/window fields and new anomaly type hint.
- `frontend/src/pages/GridAnomalies.tsx` - show entity/window fields.
- `frontend/src/components/flowEdges.ts` - add user anomaly sink nodes and split anomaly edges.
- `frontend/src/components/StreamingFlowGraph.tsx` - add user anomaly node positions.

Create:

- `flink-job/src/main/java/com/fdb/job/anomaly/AnomalyRuleEvaluation.java` - internal per-rule evaluation event.
- `flink-job/src/main/java/com/fdb/job/anomaly/AnomalySignal.java` - internal trigger/recovery signal.
- `flink-job/src/main/java/com/fdb/job/anomaly/AnomalyEventFactory.java` - centralized entity-aware `AnomalyEvent` builders.
- `flink-job/src/main/java/com/fdb/job/anomaly/CellKpiCepAnomalyDetector.java` - cell KPI CEP pipeline.
- `flink-job/src/main/java/com/fdb/job/anomaly/UserEventCepAnomalyDetector.java` - user event CEP pipeline.
- `flink-job/src/test/java/com/fdb/job/config/RuleConfigTest.java` - new config assertions.
- `flink-job/src/test/java/com/fdb/job/anomaly/AnomalyEventFactoryTest.java` - entity field construction.
- `flink-job/src/test/java/com/fdb/job/anomaly/CellKpiCepAnomalyDetectorTest.java` - cell CEP behavior.
- `flink-job/src/test/java/com/fdb/job/anomaly/UserEventCepAnomalyDetectorTest.java` - user CEP behavior.
- `flink-job/src/test/java/com/fdb/job/enrich/EnrichmentProcessFunctionTest.java` - CFG-missing main-stream behavior.
- `frontend/src/pages/UserAnomalies.tsx` - user anomaly result page.
- `frontend/src/pages/UserAnomalies.test.tsx` - user anomaly page behavior.

Remove from active DAG only:

- `flink-job/src/main/java/com/fdb/job/anomaly/AnomalyDetector.java` remains in the tree for compatibility if needed, but `FlinkJobMain` must not instantiate it.
- Existing tests that assert old `LOW_SIGNAL` / `ATTACH_FAILURE_BURST` / `HANDOVER_FAIL_PATTERN` / `CONFIG_MISMATCH` output must be rewritten or removed.

---

### Task 0: Pre-Change Safety Gates

**Files:**
- Read: `AGENTS.md`
- Read: `docs/superpowers/specs/2026-04-29-flink-data-balance-design.md`
- Analyze before editing Java symbols: `common/src/main/avro/AnomalyEvent.avsc`, `flink-job/src/main/java/com/fdb/job/FlinkJobMain.java`, `flink-job/src/main/java/com/fdb/job/config/RuleConfig.java`, `flink-job/src/main/java/com/fdb/job/enrich/EnrichmentProcessFunction.java`, `flink-job/src/main/java/com/fdb/job/sink/ResultSinks.java`, `observability-api/src/main/java/com/fdb/observability/service/StarRocksQueryService.java`

- [ ] **Step 1: Create an isolated implementation workspace**

Run:

```powershell
git status --short --branch
```

Expected: note any existing user changes. Do not revert unrelated files.

- [ ] **Step 2: Run GitNexus impact analysis before Java edits**

Use GitNexus impact analysis for these Java classes before modifying them:

```text
FlinkJobMain
RuleConfig
EnrichmentProcessFunction
ResultSinks
StarRocksSinks
IcebergSinks
StarRocksQueryService
```

Expected: no HIGH or CRITICAL findings that block this plan. If HIGH or CRITICAL findings appear, inspect the affected process before editing.

- [ ] **Step 3: Commit only after each task passes its listed verification**

Use this commit cadence:

```powershell
git add <task files>
git commit -m "<scope>: <task result>"
```

Before every commit run:

```text
GitNexus detect_changes with scope=staged
```

Expected: changed files and affected processes match the task scope.

---

### Task 1: Entity-Aware Anomaly Schema

**Files:**
- Modify: `common/src/main/avro/AnomalyEvent.avsc`
- Modify: `common/src/test/java/com/fdb/common/avro/AnomalyEventSchemaTest.java`

- [ ] **Step 1: Write the schema test first**

Replace the body of `roundtrip_anomaly_event` with this cell-anomaly roundtrip and add a second user-anomaly roundtrip:

```java
@Test
void roundtrip_cell_anomaly_event_with_nullable_user_and_coordinate_context() throws Exception {
    AnomalyEvent original = AnomalyEvent.newBuilder()
        .setDetectionTs(1714387210000L)
        .setEventTs(1714387200000L)
        .setEntityType(EntityType.CELL)
        .setEntityId("CELL-001-1")
        .setWindowStartTs(1714387080000L)
        .setWindowEndTs(1714387200000L)
        .setImsi(null)
        .setSiteId("SITE-001")
        .setCellId("CELL-001-1")
        .setGridId(null)
        .setLatitude(null)
        .setLongitude(null)
        .setAnomalyType(AnomalyType.CELL_RADIO_BAD)
        .setSeverity(Severity.LOW)
        .setRuleVersion("v1.0")
        .setContextJson("{\"metric\":\"avgRsrp\"}")
        .build();

    AnomalyEvent decoded = roundtrip(original);

    assertThat(decoded).isEqualTo(original);
    assertThat(decoded.getEntityType()).isEqualTo(EntityType.CELL);
    assertThat(decoded.getEntityId()).isEqualTo("CELL-001-1");
    assertThat(decoded.getAnomalyType()).isEqualTo(AnomalyType.CELL_RADIO_BAD);
    assertThat(decoded.getImsi()).isNull();
    assertThat(decoded.getLatitude()).isNull();
}

@Test
void roundtrip_user_anomaly_event() throws Exception {
    AnomalyEvent original = AnomalyEvent.newBuilder()
        .setDetectionTs(1714387210000L)
        .setEventTs(1714387200000L)
        .setEntityType(EntityType.USER)
        .setEntityId("460001234567890")
        .setWindowStartTs(1714386600000L)
        .setWindowEndTs(1714387200000L)
        .setImsi("460001234567890")
        .setSiteId("SITE-001")
        .setCellId("CELL-001-1")
        .setGridId("wx4g0ec")
        .setLatitude(39.9042)
        .setLongitude(116.4074)
        .setAnomalyType(AnomalyType.USER_QOE_BAD)
        .setSeverity(Severity.MEDIUM)
        .setRuleVersion("v1.0")
        .setContextJson("{\"metric\":\"latencyMs\"}")
        .build();

    AnomalyEvent decoded = roundtrip(original);

    assertThat(decoded.getEntityType()).isEqualTo(EntityType.USER);
    assertThat(decoded.getEntityId()).isEqualTo("460001234567890");
    assertThat(decoded.getAnomalyType()).isEqualTo(AnomalyType.USER_QOE_BAD);
}

private static AnomalyEvent roundtrip(AnomalyEvent original) throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    var encoder = EncoderFactory.get().binaryEncoder(out, null);
    new SpecificDatumWriter<>(AnomalyEvent.class).write(original, encoder);
    encoder.flush();
    return new SpecificDatumReader<>(AnomalyEvent.class).read(null,
        DecoderFactory.get().binaryDecoder(new ByteArrayInputStream(out.toByteArray()), null));
}
```

- [ ] **Step 2: Run the test and verify it fails**

Run:

```powershell
mvn "-pl" "common" "-Dtest=AnomalyEventSchemaTest" test
```

Expected: compilation fails because `EntityType`, new fields, and new anomaly symbols do not exist.

- [ ] **Step 3: Update `AnomalyEvent.avsc`**

Replace the fields array with this schema:

```json
[
  { "name": "detectionTs", "type": "long" },
  { "name": "eventTs", "type": "long" },
  { "name": "entityType", "type": { "type": "enum", "name": "EntityType",
    "symbols": ["CELL","USER","GRID"] } },
  { "name": "entityId", "type": "string" },
  { "name": "windowStartTs", "type": "long", "default": 0 },
  { "name": "windowEndTs", "type": "long", "default": 0 },
  { "name": "imsi", "type": ["null", "string"], "default": null },
  { "name": "siteId", "type": ["null", "string"], "default": null },
  { "name": "cellId", "type": ["null", "string"], "default": null },
  { "name": "gridId", "type": ["null", "string"], "default": null },
  { "name": "latitude", "type": ["null", "double"], "default": null },
  { "name": "longitude", "type": ["null", "double"], "default": null },
  { "name": "anomalyType", "type": { "type": "enum", "name": "AnomalyType",
    "symbols": ["LOW_SIGNAL","ATTACH_FAILURE_BURST","HANDOVER_FAIL_PATTERN",
                "CONFIG_MISMATCH","COVERAGE_HOLE","CELL_RADIO_BAD",
                "CELL_SERVICE_BAD","USER_FAILURE","USER_QOE_BAD"] } },
  { "name": "severity", "type": { "type": "enum", "name": "Severity",
    "symbols": ["LOW","MEDIUM","HIGH"] } },
  { "name": "ruleVersion", "type": "string" },
  { "name": "contextJson", "type": "string" }
]
```

- [ ] **Step 4: Generate Avro sources and run schema tests**

Run:

```powershell
mvn "-pl" "common" generate-sources
mvn "-pl" "common" "-Dtest=AnomalyEventSchemaTest" test
```

Expected: `AnomalyEventSchemaTest` passes.

- [ ] **Step 5: Commit**

Run:

```powershell
git add common/src/main/avro/AnomalyEvent.avsc common/src/test/java/com/fdb/common/avro/AnomalyEventSchemaTest.java
git commit -m "feat(common): add entity-aware anomaly event schema"
```

---

### Task 2: Rule Configuration Defaults

**Files:**
- Modify: `flink-job/src/main/java/com/fdb/job/config/RuleConfig.java`
- Modify: `flink-job/src/main/java/com/fdb/job/config/JobConfig.java`
- Modify: `flink-job/src/main/resources/job-default.yaml`
- Create: `flink-job/src/test/java/com/fdb/job/config/RuleConfigTest.java`

- [ ] **Step 1: Write config tests**

Create `RuleConfigTest.java`:

```java
package com.fdb.job.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Test;

class RuleConfigTest {

    @Test
    void defaults_match_entity_anomaly_design() {
        RuleConfig rules = RuleConfig.defaults();

        assertThat(rules.cellConsecutiveMinutes()).isEqualTo(3);
        assertThat(rules.cellRsrpMin()).isEqualTo(-110f);
        assertThat(rules.cellSinrMin()).isEqualTo(-3f);
        assertThat(rules.cellAttachSuccessMin()).isEqualTo(0.95f);
        assertThat(rules.cellHoSuccessMin()).isEqualTo(0.90f);
        assertThat(rules.cellDropRateMax()).isEqualTo(0.05f);
        assertThat(rules.userConsecutiveEvents()).isEqualTo(3);
        assertThat(rules.userWindowMinutes()).isEqualTo(10);
        assertThat(rules.userRsrpMin()).isEqualTo(-110f);
        assertThat(rules.userSinrMin()).isEqualTo(-3f);
        assertThat(rules.userLatencyMsMax()).isEqualTo(500f);
        assertThat(rules.coverageHoleThreshold()).isEqualTo(50);
        assertThat(rules.ruleVersion()).isEqualTo("v1.0");
    }

    @Test
    void explicit_env_keys_override_yaml_defaults() throws Exception {
        var config = com.fdb.common.config.ConfigLoader.builder()
            .defaultResource("job-default.yaml")
            .envSource(Map.of())
            .build()
            .load();
        RuleConfig rules = JobConfig.rulesFrom(config, Map.of(
            "FDB_ANOMALY_CELL_CONSECUTIVE_MINUTES", "4",
            "FDB_ANOMALY_USER_LATENCY_MS_MAX", "750"
        ), new Properties());

        assertThat(rules.cellConsecutiveMinutes()).isEqualTo(4);
        assertThat(rules.userLatencyMsMax()).isEqualTo(750f);
    }
}
```

- [ ] **Step 2: Run the test and verify it fails**

Run:

```powershell
mvn "-pl" "flink-job" "-Dtest=RuleConfigTest" test
```

Expected: compilation fails because the new `RuleConfig` accessors do not exist.

- [ ] **Step 3: Expand `RuleConfig`**

Replace the record with:

```java
public record RuleConfig(
    float rsrpThreshold,
    float sinrThreshold,
    int attachFailBurstThreshold,
    int coverageHoleThreshold,
    int cellConsecutiveMinutes,
    float cellRsrpMin,
    float cellSinrMin,
    float cellAttachSuccessMin,
    float cellHoSuccessMin,
    float cellDropRateMax,
    int userConsecutiveEvents,
    int userWindowMinutes,
    float userRsrpMin,
    float userSinrMin,
    float userLatencyMsMax,
    String ruleVersion
) implements Serializable {
    public static RuleConfig defaults() {
        return new RuleConfig(
            -110f, -3f, 10, 50,
            3, -110f, -3f, 0.95f, 0.90f, 0.05f,
            3, 10, -110f, -3f, 500f, "v1.0");
    }
}
```

- [ ] **Step 4: Resolve new values in `JobConfig.rules()`**

Add this package-private resolver and make `rules()` call it with `System.getenv()` and `System.getProperties()`:

```java
public RuleConfig rules() {
    return rulesFrom(config, System.getenv(), System.getProperties());
}

static RuleConfig rulesFrom(ConfigLoader.Config config, Map<String, String> env, Properties properties) {
    return new RuleConfig(
        (float) config.getDouble("rules.lowSignal.rsrpThreshold", -110),
        (float) config.getDouble("rules.lowSignal.sinrThreshold", -3),
        config.getInt("rules.attachFailureBurst.threshold", 10),
        config.getInt("rules.coverageHole.threshold", 50),
        intSetting(config, env, properties, "rules.cell.consecutiveMinutes",
            "FDB_ANOMALY_CELL_CONSECUTIVE_MINUTES", "fdb.anomaly.cell.consecutive.minutes", 3),
        floatSetting(config, env, properties, "rules.cell.rsrpMin",
            "FDB_ANOMALY_CELL_RSRP_MIN", "fdb.anomaly.cell.rsrp.min", -110f),
        floatSetting(config, env, properties, "rules.cell.sinrMin",
            "FDB_ANOMALY_CELL_SINR_MIN", "fdb.anomaly.cell.sinr.min", -3f),
        floatSetting(config, env, properties, "rules.cell.attachSuccessMin",
            "FDB_ANOMALY_CELL_ATTACH_SUCCESS_MIN", "fdb.anomaly.cell.attach.success.min", 0.95f),
        floatSetting(config, env, properties, "rules.cell.hoSuccessMin",
            "FDB_ANOMALY_CELL_HO_SUCCESS_MIN", "fdb.anomaly.cell.ho.success.min", 0.90f),
        floatSetting(config, env, properties, "rules.cell.dropRateMax",
            "FDB_ANOMALY_CELL_DROP_RATE_MAX", "fdb.anomaly.cell.drop.rate.max", 0.05f),
        intSetting(config, env, properties, "rules.user.consecutiveEvents",
            "FDB_ANOMALY_USER_CONSECUTIVE_EVENTS", "fdb.anomaly.user.consecutive.events", 3),
        intSetting(config, env, properties, "rules.user.windowMinutes",
            "FDB_ANOMALY_USER_WINDOW_MINUTES", "fdb.anomaly.user.window.minutes", 10),
        floatSetting(config, env, properties, "rules.user.rsrpMin",
            "FDB_ANOMALY_USER_RSRP_MIN", "fdb.anomaly.user.rsrp.min", -110f),
        floatSetting(config, env, properties, "rules.user.sinrMin",
            "FDB_ANOMALY_USER_SINR_MIN", "fdb.anomaly.user.sinr.min", -3f),
        floatSetting(config, env, properties, "rules.user.latencyMsMax",
            "FDB_ANOMALY_USER_LATENCY_MS_MAX", "fdb.anomaly.user.latency.ms.max", 500f),
        stringSetting(config, env, properties, "rules.version",
            "FDB_ANOMALY_RULE_VERSION", "fdb.anomaly.rule.version", "v1.0"));
}

private static int intSetting(ConfigLoader.Config config, Map<String, String> env, Properties properties,
                              String yamlKey, String envKey, String propertyKey, int defaultValue) {
    String value = env.get(envKey);
    if (value == null || value.isBlank()) {
        value = properties.getProperty(propertyKey);
    }
    return value == null || value.isBlank() ? config.getInt(yamlKey, defaultValue) : Integer.parseInt(value.trim());
}

private static float floatSetting(ConfigLoader.Config config, Map<String, String> env, Properties properties,
                                  String yamlKey, String envKey, String propertyKey, float defaultValue) {
    String value = env.get(envKey);
    if (value == null || value.isBlank()) {
        value = properties.getProperty(propertyKey);
    }
    return value == null || value.isBlank()
        ? (float) config.getDouble(yamlKey, defaultValue)
        : Float.parseFloat(value.trim());
}

private static String stringSetting(ConfigLoader.Config config, Map<String, String> env, Properties properties,
                                    String yamlKey, String envKey, String propertyKey, String defaultValue) {
    String value = env.get(envKey);
    if (value == null || value.isBlank()) {
        value = properties.getProperty(propertyKey);
    }
    if (value != null && !value.isBlank()) {
        return value.trim();
    }
    String configured = config.getStringOrNull(yamlKey);
    return configured == null || configured.isBlank() ? defaultValue : configured.trim();
}
```

- [ ] **Step 5: Extend `job-default.yaml`**

Add:

```yaml
  version: v1.0
  cell:
    consecutiveMinutes: 3
    rsrpMin: -110
    sinrMin: -3
    attachSuccessMin: 0.95
    hoSuccessMin: 0.90
    dropRateMax: 0.05
  user:
    consecutiveEvents: 3
    windowMinutes: 10
    rsrpMin: -110
    sinrMin: -3
    latencyMsMax: 500
```

- [ ] **Step 6: Run config tests**

Run:

```powershell
mvn "-pl" "flink-job" "-Dtest=RuleConfigTest" test
```

Expected: tests pass.

- [ ] **Step 7: Commit**

Run:

```powershell
git add flink-job/src/main/java/com/fdb/job/config/RuleConfig.java flink-job/src/main/java/com/fdb/job/config/JobConfig.java flink-job/src/main/resources/job-default.yaml flink-job/src/test/java/com/fdb/job/config/RuleConfigTest.java
git commit -m "feat(flink): add entity anomaly rule config"
```

---

### Task 3: Anomaly Event Factory And Rule Evaluation Model

**Files:**
- Create: `flink-job/src/main/java/com/fdb/job/anomaly/AnomalyRuleEvaluation.java`
- Create: `flink-job/src/main/java/com/fdb/job/anomaly/AnomalySignal.java`
- Create: `flink-job/src/main/java/com/fdb/job/anomaly/AnomalyEventFactory.java`
- Create: `flink-job/src/test/java/com/fdb/job/anomaly/AnomalyEventFactoryTest.java`
- Modify: `flink-job/src/main/java/com/fdb/job/anomaly/CoverageHoleDetector.java`

- [ ] **Step 1: Write factory tests**

Create `AnomalyEventFactoryTest.java`:

```java
package com.fdb.job.anomaly;

import static org.assertj.core.api.Assertions.assertThat;

import com.fdb.common.avro.AnomalyEvent;
import com.fdb.common.avro.AnomalyType;
import com.fdb.common.avro.EntityType;
import com.fdb.common.avro.Severity;
import com.fdb.job.config.RuleConfig;
import org.junit.jupiter.api.Test;

class AnomalyEventFactoryTest {

    @Test
    void builds_cell_event_without_user_or_coordinate_requirements() {
        AnomalyEvent event = AnomalyEventFactory.fromEvaluation(
            evaluation(EntityType.CELL, "CELL-001", "avgRsrp", AnomalyType.CELL_RADIO_BAD, -115d));

        assertThat(event.getEntityType()).isEqualTo(EntityType.CELL);
        assertThat(event.getEntityId()).isEqualTo("CELL-001");
        assertThat(event.getCellId()).isEqualTo("CELL-001");
        assertThat(event.getImsi()).isNull();
        assertThat(event.getLatitude()).isNull();
        assertThat(event.getWindowStartTs()).isEqualTo(1_000L);
        assertThat(event.getWindowEndTs()).isEqualTo(61_000L);
        assertThat(event.getRuleVersion()).isEqualTo("v1.0");
    }

    @Test
    void builds_user_event_with_imsi_entity_id() {
        AnomalyEvent event = AnomalyEventFactory.fromEvaluation(
            evaluation(EntityType.USER, "460001234567890", "latencyMs", AnomalyType.USER_QOE_BAD, 900d));

        assertThat(event.getEntityType()).isEqualTo(EntityType.USER);
        assertThat(event.getEntityId()).isEqualTo("460001234567890");
        assertThat(event.getImsi()).isEqualTo("460001234567890");
        assertThat(event.getAnomalyType()).isEqualTo(AnomalyType.USER_QOE_BAD);
    }

    private static AnomalyRuleEvaluation evaluation(
        EntityType entityType,
        String entityId,
        String dimension,
        AnomalyType anomalyType,
        double observedValue) {
        return new AnomalyRuleEvaluation(
            entityType, entityId, dimension, true,
            1_000L, 61_000L, 61_000L,
            "SITE-001", "CELL-001", entityType == EntityType.USER ? entityId : null,
            "wx4g0e", null, null,
            anomalyType, Severity.HIGH, "v1.0",
            dimension, -1d, observedValue,
            "{\"metric\":\"" + dimension + "\"}");
    }
}
```

- [ ] **Step 2: Run the test and verify it fails**

Run:

```powershell
mvn "-pl" "flink-job" "-Dtest=AnomalyEventFactoryTest" test
```

Expected: compilation fails because model/factory classes do not exist.

- [ ] **Step 3: Create `AnomalyRuleEvaluation`**

Use:

```java
package com.fdb.job.anomaly;

import com.fdb.common.avro.AnomalyType;
import com.fdb.common.avro.EntityType;
import com.fdb.common.avro.Severity;
import java.io.Serializable;

public record AnomalyRuleEvaluation(
    EntityType entityType,
    String entityId,
    String ruleDimension,
    boolean abnormal,
    long windowStartTs,
    long windowEndTs,
    long eventTs,
    String siteId,
    String cellId,
    String imsi,
    String gridId,
    Double latitude,
    Double longitude,
    AnomalyType anomalyType,
    Severity severity,
    String ruleVersion,
    String metricName,
    double threshold,
    double observedValue,
    String contextJson
) implements Serializable {
    public String key() {
        return entityType + "|" + entityId + "|" + ruleDimension;
    }
}
```

- [ ] **Step 4: Create `AnomalySignal`**

Use:

```java
package com.fdb.job.anomaly;

import java.io.Serializable;
import java.util.List;

public record AnomalySignal(
    SignalType type,
    String key,
    AnomalyRuleEvaluation current,
    List<AnomalyRuleEvaluation> streak
) implements Serializable {
    public enum SignalType {
        TRIGGER,
        RECOVERY
    }

    public static AnomalySignal trigger(List<AnomalyRuleEvaluation> streak) {
        AnomalyRuleEvaluation last = streak.get(streak.size() - 1);
        return new AnomalySignal(SignalType.TRIGGER, last.key(), last, List.copyOf(streak));
    }

    public static AnomalySignal recovery(AnomalyRuleEvaluation evaluation) {
        return new AnomalySignal(SignalType.RECOVERY, evaluation.key(), evaluation, List.of(evaluation));
    }
}
```

- [ ] **Step 5: Create `AnomalyEventFactory`**

Use:

```java
package com.fdb.job.anomaly;

import com.fdb.common.avro.AnomalyEvent;

public final class AnomalyEventFactory {
    private AnomalyEventFactory() {}

    public static AnomalyEvent fromEvaluation(AnomalyRuleEvaluation evaluation) {
        return AnomalyEvent.newBuilder()
            .setDetectionTs(System.currentTimeMillis())
            .setEventTs(evaluation.eventTs())
            .setEntityType(evaluation.entityType())
            .setEntityId(evaluation.entityId())
            .setWindowStartTs(evaluation.windowStartTs())
            .setWindowEndTs(evaluation.windowEndTs())
            .setImsi(evaluation.imsi())
            .setSiteId(evaluation.siteId())
            .setCellId(evaluation.cellId())
            .setGridId(evaluation.gridId())
            .setLatitude(evaluation.latitude())
            .setLongitude(evaluation.longitude())
            .setAnomalyType(evaluation.anomalyType())
            .setSeverity(evaluation.severity())
            .setRuleVersion(evaluation.ruleVersion())
            .setContextJson(evaluation.contextJson())
            .build();
    }
}
```

- [ ] **Step 6: Convert `CoverageHoleDetector` to the factory**

Replace its emit block with:

```java
AnomalyRuleEvaluation evaluation = new AnomalyRuleEvaluation(
    EntityType.GRID,
    ctx.getCurrentKey(),
    "coverageHole",
    true,
    bucket * WINDOW_MS,
    bucket * WINDOW_MS + WINDOW_MS,
    chr.getEventTs(),
    chr.getSiteId().toString(),
    chr.getCellId().toString(),
    null,
    ctx.getCurrentKey(),
    chr.getLatitude(),
    chr.getLongitude(),
    AnomalyType.COVERAGE_HOLE,
    Severity.HIGH,
    rules.ruleVersion(),
    "lowSignalCount",
    rules.coverageHoleThreshold(),
    count,
    String.format("{\"low_signal_count\":%d,\"window_ms\":%d}", count, WINDOW_MS));
out.collect(AnomalyEventFactory.fromEvaluation(evaluation));
```

Add imports for `EntityType` and remove the call to `AnomalyDetector.buildAnomaly`.

- [ ] **Step 7: Run factory and coverage tests**

Run:

```powershell
mvn "-pl" "flink-job" "-Dtest=AnomalyEventFactoryTest,FlinkJobE2ETest#coverage_hole_pipeline_groups_by_grid" test
```

Expected: factory test and coverage-hole method pass.

- [ ] **Step 8: Commit**

Run:

```powershell
git add flink-job/src/main/java/com/fdb/job/anomaly flink-job/src/test/java/com/fdb/job/anomaly/AnomalyEventFactoryTest.java
git commit -m "feat(flink): add entity anomaly event factory"
```

---

### Task 4: Cell KPI CEP Detector

**Files:**
- Modify: `flink-job/pom.xml`
- Create: `flink-job/src/main/java/com/fdb/job/anomaly/CellKpiCepAnomalyDetector.java`
- Create: `flink-job/src/test/java/com/fdb/job/anomaly/CellKpiCepAnomalyDetectorTest.java`

- [ ] **Step 1: Add Flink CEP dependency**

Add to `flink-job/pom.xml` dependencies:

```xml
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-cep</artifactId>
    <version>${flink.version}</version>
</dependency>
```

- [ ] **Step 2: Write cell CEP tests**

Create tests that call `CellKpiCepAnomalyDetector.detect(...)` in a local `StreamExecutionEnvironment`:

```java
@Test
void emits_after_three_consecutive_bad_one_minute_kpis() throws Exception {
    List<AnomalyEvent> output = run(List.of(
        kpi(0, -111f, 1f, 0.99f, 0.99f, 0.0f),
        kpi(60_000, -112f, 1f, 0.99f, 0.99f, 0.0f),
        kpi(120_000, -113f, 1f, 0.99f, 0.99f, 0.0f)));

    assertThat(output).hasSize(1);
    assertThat(output.get(0).getEntityType()).isEqualTo(EntityType.CELL);
    assertThat(output.get(0).getEntityId()).isEqualTo("CELL-001");
    assertThat(output.get(0).getAnomalyType()).isEqualTo(AnomalyType.CELL_RADIO_BAD);
}

@Test
void normal_period_breaks_consecutive_cell_streak() throws Exception {
    List<AnomalyEvent> output = run(List.of(
        kpi(0, -111f, 1f, 0.99f, 0.99f, 0.0f),
        kpi(60_000, -90f, 10f, 0.99f, 0.99f, 0.0f),
        kpi(120_000, -113f, 1f, 0.99f, 0.99f, 0.0f)));

    assertThat(output).isEmpty();
}

@Test
void active_cell_streak_emits_once_until_recovery() throws Exception {
    List<AnomalyEvent> output = run(List.of(
        kpi(0, -111f, 1f, 0.99f, 0.99f, 0.0f),
        kpi(60_000, -112f, 1f, 0.99f, 0.99f, 0.0f),
        kpi(120_000, -113f, 1f, 0.99f, 0.99f, 0.0f),
        kpi(180_000, -114f, 1f, 0.99f, 0.99f, 0.0f),
        kpi(240_000, -90f, 10f, 0.99f, 0.99f, 0.0f),
        kpi(300_000, -111f, 1f, 0.99f, 0.99f, 0.0f),
        kpi(360_000, -112f, 1f, 0.99f, 0.99f, 0.0f),
        kpi(420_000, -113f, 1f, 0.99f, 0.99f, 0.0f)));

    assertThat(output).hasSize(2);
}
```

Add these helper builders below the assertions:

```java
private static List<AnomalyEvent> run(List<CellKpi> input) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    List<AnomalyEvent> output = new ArrayList<>();
    try (CloseableIterator<AnomalyEvent> it = CellKpiCepAnomalyDetector
        .detect(env.fromCollection(input, new GenericTypeInfo<>(CellKpi.class)), RuleConfig.defaults())
        .executeAndCollect()) {
        while (it.hasNext()) {
            output.add(it.next());
        }
    }
    return output;
}

private static CellKpi kpi(long startTs, float rsrp, float sinr, float attach, float ho, float drop) {
    return CellKpi.newBuilder()
        .setWindowStartTs(startTs)
        .setWindowEndTs(startTs + 60_000L)
        .setWindowKind(WindowKind.MIN_1)
        .setJoinQuality(JoinQuality.JOINED)
        .setSiteId("SITE-001")
        .setCellId("CELL-001")
        .setGridId("wx4g0e")
        .setNumChrEvents(100)
        .setNumUsers(10)
        .setRsrpSampleCount(100)
        .setSinrSampleCount(100)
        .setAttachAttempts(20)
        .setAvgRsrp(rsrp)
        .setAvgSinr(sinr)
        .setAvgPrbUsageDl(0.5f)
        .setThroughputDlMbpsAvg(100f)
        .setDropRate(drop)
        .setHoSuccessRate(ho)
        .setAttachSuccessRate(attach)
        .build();
}
```

- [ ] **Step 3: Run the tests and verify they fail**

Run:

```powershell
mvn "-pl" "flink-job" "-Dtest=CellKpiCepAnomalyDetectorTest" test
```

Expected: compilation fails because `CellKpiCepAnomalyDetector` does not exist.

- [ ] **Step 4: Implement the detector with CEP**

Implement `CellKpiCepAnomalyDetector.detect(DataStream<CellKpi>, RuleConfig)`:

```java
public static DataStream<AnomalyEvent> detect(DataStream<CellKpi> input, RuleConfig rules) {
    DataStream<AnomalyRuleEvaluation> evaluations = input
        .flatMap((CellKpi kpi, Collector<AnomalyRuleEvaluation> out) -> emitEvaluations(kpi, rules, out))
        .returns(new GenericTypeInfo<>(AnomalyRuleEvaluation.class))
        .name("cell-kpi-anomaly-evaluations");

    DataStream<AnomalySignal> triggers = triggerSignals(evaluations, rules.cellConsecutiveMinutes());
    DataStream<AnomalySignal> recoveries = evaluations
        .filter(evaluation -> !evaluation.abnormal())
        .map(AnomalySignal::recovery)
        .returns(new GenericTypeInfo<>(AnomalySignal.class))
        .name("cell-kpi-anomaly-recoveries");

    return triggers.union(recoveries)
        .keyBy(AnomalySignal::key)
        .process(new ActivationFunction(), new GenericTypeInfo<>(AnomalyEvent.class))
        .name("cell-kpi-cep-anomaly-activation");
}
```

Use `CEP.pattern` with strict `next`:

```java
Pattern<AnomalyRuleEvaluation, ?> pattern = Pattern
    .<AnomalyRuleEvaluation>begin("first", AfterMatchSkipStrategy.skipPastLastEvent())
    .where(new SimpleCondition<>() {
        @Override
        public boolean filter(AnomalyRuleEvaluation value) {
            return value.abnormal();
        }
    })
    .next("second")
    .where(new SimpleCondition<>() {
        @Override
        public boolean filter(AnomalyRuleEvaluation value) {
            return value.abnormal();
        }
    })
    .next("third")
    .where(new SimpleCondition<>() {
        @Override
        public boolean filter(AnomalyRuleEvaluation value) {
            return value.abnormal();
        }
    })
    .within(Duration.ofMinutes(Math.max(1, consecutiveMinutes)));
```

The `PatternProcessFunction` should collect `AnomalySignal.trigger(List.of(first, second, third))`.

The `ActivationFunction` should maintain `ValueState<Boolean> active`. On `RECOVERY`, update active to `false`. On `TRIGGER`, emit `AnomalyEventFactory.fromEvaluation(signal.current())` only when active is not true, then update active to `true`.

- [ ] **Step 5: Run cell CEP tests**

Run:

```powershell
mvn "-pl" "flink-job" "-Dtest=CellKpiCepAnomalyDetectorTest" test
```

Expected: tests pass.

- [ ] **Step 6: Commit**

Run:

```powershell
git add flink-job/pom.xml flink-job/src/main/java/com/fdb/job/anomaly/CellKpiCepAnomalyDetector.java flink-job/src/test/java/com/fdb/job/anomaly/CellKpiCepAnomalyDetectorTest.java
git commit -m "feat(flink): add cell KPI CEP anomaly detector"
```

---

### Task 5: User Event CEP Detector

**Files:**
- Create: `flink-job/src/main/java/com/fdb/job/anomaly/UserEventCepAnomalyDetector.java`
- Create: `flink-job/src/test/java/com/fdb/job/anomaly/UserEventCepAnomalyDetectorTest.java`

- [ ] **Step 1: Write user CEP tests**

Create tests with these assertions:

```java
@Test
void emits_user_failure_after_three_attach_failures_within_window() throws Exception {
    List<AnomalyEvent> output = run(List.of(
        enriched(chr("a", 0, ChrEventType.ATTACH, 1, -90f, 10f, 10f)),
        enriched(chr("b", 60_000, ChrEventType.ATTACH, 2, -90f, 10f, 10f)),
        enriched(chr("c", 120_000, ChrEventType.ATTACH, 3, -90f, 10f, 10f))));

    assertThat(output).hasSize(1);
    assertThat(output.get(0).getEntityType()).isEqualTo(EntityType.USER);
    assertThat(output.get(0).getEntityId()).isEqualTo("460001234567890");
    assertThat(output.get(0).getAnomalyType()).isEqualTo(AnomalyType.USER_FAILURE);
}

@Test
void success_event_breaks_user_failure_streak() throws Exception {
    List<AnomalyEvent> output = run(List.of(
        enriched(chr("a", 0, ChrEventType.ATTACH, 1, -90f, 10f, 10f)),
        enriched(chr("b", 60_000, ChrEventType.ATTACH, 0, -90f, 10f, 10f)),
        enriched(chr("c", 120_000, ChrEventType.ATTACH, 3, -90f, 10f, 10f))));

    assertThat(output).isEmpty();
}

@Test
void emits_user_qoe_for_three_high_latency_events() throws Exception {
    List<AnomalyEvent> output = run(List.of(
        enriched(chr("a", 0, ChrEventType.DATA_SESSION, 0, -90f, 10f, 600f)),
        enriched(chr("b", 60_000, ChrEventType.DATA_SESSION, 0, -90f, 10f, 700f)),
        enriched(chr("c", 120_000, ChrEventType.DATA_SESSION, 0, -90f, 10f, 800f))));

    assertThat(output).extracting(AnomalyEvent::getAnomalyType)
        .containsExactly(AnomalyType.USER_QOE_BAD);
}

@Test
void skips_records_with_blank_imsi() throws Exception {
    ChrEvent event = ChrEvent.newBuilder(chr("a", 0, ChrEventType.ATTACH, 1, -90f, 10f, 10f))
        .setImsi("")
        .build();

    assertThat(run(List.of(enriched(event), enriched(event), enriched(event)))).isEmpty();
}
```

Add these helpers below the assertions:

```java
private static List<AnomalyEvent> run(List<EnrichedChr> input) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    List<AnomalyEvent> output = new ArrayList<>();
    try (CloseableIterator<AnomalyEvent> it = UserEventCepAnomalyDetector
        .detect(env.fromCollection(input, new GenericTypeInfo<>(EnrichedChr.class)), RuleConfig.defaults())
        .executeAndCollect()) {
        while (it.hasNext()) {
            output.add(it.next());
        }
    }
    return output;
}

private static EnrichedChr enriched(ChrEvent chr) {
    return new EnrichedChr(chr, null, null);
}

private static ChrEvent chr(
    String id, long offsetMs, ChrEventType type, int resultCode, Float rsrp, Float sinr, Float latencyMs) {
    ChrEvent.Builder builder = ChrEvent.newBuilder()
        .setChrId(id)
        .setEventTs(1_000_000L + offsetMs)
        .setImsi("460001234567890")
        .setSiteId("SITE-001")
        .setCellId("CELL-001")
        .setEventType(type)
        .setRatType(RatType.LTE)
        .setPci(100)
        .setTac(40001)
        .setEci(1000L)
        .setMcc("460")
        .setMnc("00")
        .setResultCode(resultCode)
        .setLatitude(39.9)
        .setLongitude(116.4)
        .setGridId("wx4g0e");
    if (rsrp != null) {
        builder.setRsrp(rsrp);
    }
    if (sinr != null) {
        builder.setSinr(sinr);
    }
    if (latencyMs != null) {
        builder.setLatencyMs(latencyMs);
    }
    return builder.build();
}
```

- [ ] **Step 2: Run the tests and verify they fail**

Run:

```powershell
mvn "-pl" "flink-job" "-Dtest=UserEventCepAnomalyDetectorTest" test
```

Expected: compilation fails because `UserEventCepAnomalyDetector` does not exist.

- [ ] **Step 3: Implement user rule evaluation**

Implement `UserEventCepAnomalyDetector.detect(DataStream<EnrichedChr>, RuleConfig)` with a CEP trigger stream and recovery stream:

```java
public static DataStream<AnomalyEvent> detect(DataStream<EnrichedChr> input, RuleConfig rules) {
    DataStream<AnomalyRuleEvaluation> evaluations = input
        .flatMap((EnrichedChr enriched, Collector<AnomalyRuleEvaluation> out) ->
            emitEvaluations(enriched, rules, out))
        .returns(new GenericTypeInfo<>(AnomalyRuleEvaluation.class))
        .name("user-event-anomaly-evaluations");

    DataStream<AnomalySignal> triggers = triggerSignals(evaluations, rules.userConsecutiveEvents(),
        Duration.ofMinutes(rules.userWindowMinutes()));
    DataStream<AnomalySignal> recoveries = evaluations
        .filter(evaluation -> !evaluation.abnormal())
        .map(AnomalySignal::recovery)
        .returns(new GenericTypeInfo<>(AnomalySignal.class))
        .name("user-event-anomaly-recoveries");

    return triggers.union(recoveries)
        .keyBy(AnomalySignal::key)
        .process(new ActivationFunction(), new GenericTypeInfo<>(AnomalyEvent.class))
        .name("user-event-cep-anomaly-activation");
}
```

Emit these evaluations:

```java
if (isBlank(chr.getImsi())) {
    return;
}
if (chr.getEventType() == ChrEventType.ATTACH || chr.getEventType() == ChrEventType.SERVICE_REQUEST
    || chr.getEventType() == ChrEventType.RRC_SETUP_FAIL) {
    emitFailureEvaluation(chr, rules, "accessFailure", chr.getResultCode() != 0, out);
}
if (chr.getEventType() == ChrEventType.HANDOVER) {
    emitFailureEvaluation(chr, rules, "handoverFailure", chr.getResultCode() != 0, out);
}
if (chr.getRsrp() != null) {
    emitQoeEvaluation(chr, rules, "rsrp", chr.getRsrp() < rules.userRsrpMin(),
        rules.userRsrpMin(), chr.getRsrp(), out);
}
if (chr.getSinr() != null) {
    emitQoeEvaluation(chr, rules, "sinr", chr.getSinr() < rules.userSinrMin(),
        rules.userSinrMin(), chr.getSinr(), out);
}
if (chr.getLatencyMs() != null) {
    emitQoeEvaluation(chr, rules, "latencyMs", chr.getLatencyMs() > rules.userLatencyMsMax(),
        rules.userLatencyMsMax(), chr.getLatencyMs(), out);
}
```

Set `entityType=USER`, `entityId=imsi`, `windowStartTs=chr.eventTs - userWindowMinutes`, `windowEndTs=chr.eventTs`, and `imsi=chr.imsi`.

- [ ] **Step 4: Run user CEP tests**

Run:

```powershell
mvn "-pl" "flink-job" "-Dtest=UserEventCepAnomalyDetectorTest" test
```

Expected: tests pass.

- [ ] **Step 5: Commit**

Run:

```powershell
git add flink-job/src/main/java/com/fdb/job/anomaly/UserEventCepAnomalyDetector.java flink-job/src/test/java/com/fdb/job/anomaly/UserEventCepAnomalyDetectorTest.java
git commit -m "feat(flink): add user event CEP anomaly detector"
```

---

### Task 6: Enrichment Continues Without CFG

**Files:**
- Modify: `flink-job/src/main/java/com/fdb/job/enrich/EnrichmentProcessFunction.java`
- Create: `flink-job/src/test/java/com/fdb/job/enrich/EnrichmentProcessFunctionTest.java`
- Modify: `flink-job/src/main/java/com/fdb/job/FlinkJobMain.java`

- [ ] **Step 1: Write enrichment tests**

Create a test using `KeyedOneInputStreamOperatorTestHarness<String, RoutedEnvelope, EnrichedChr>`:

```java
@Test
void emits_enriched_chr_with_null_cfg_when_cfg_is_missing() throws Exception {
    try (var harness = harness()) {
        harness.open();
        ChrEvent chr = chr("chr-1");

        harness.processElement(new StreamRecord<>(
            new RoutedEnvelope(new InputEnvelope.ChrEnv(chr), 1), 1L));

        List<EnrichedChr> output = harness.extractOutputValues();
        assertThat(output).hasSize(1);
        assertThat(output.get(0).chrEvent()).isEqualTo(chr);
        assertThat(output.get(0).cfgConfig()).isNull();
        assertThat(harness.getSideOutput(EnrichmentProcessFunction.ENRICHMENT_LATE))
            .extracting(record -> record.getValue().getChrId().toString())
            .containsExactly("chr-1");
    }
}

private static KeyedOneInputStreamOperatorTestHarness<String, RoutedEnvelope, EnrichedChr> harness()
    throws Exception {
    KeyedOneInputStreamOperatorTestHarness<String, RoutedEnvelope, EnrichedChr> harness =
        new KeyedOneInputStreamOperatorTestHarness<>(
            new KeyedProcessOperator<>(new EnrichmentProcessFunction()),
            RoutedEnvelope::stateKey,
            Types.STRING);
    harness.setup(new KryoSerializer<>(EnrichedChr.class, new ExecutionConfig()));
    return harness;
}

private static ChrEvent chr(String id) {
    return ChrEvent.newBuilder()
        .setChrId(id)
        .setEventTs(1_000_000L)
        .setImsi("460001234567890")
        .setSiteId("SITE-001")
        .setCellId("CELL-001")
        .setEventType(ChrEventType.DATA_SESSION)
        .setRatType(RatType.LTE)
        .setPci(100)
        .setTac(40001)
        .setEci(1000L)
        .setMcc("460")
        .setMnc("00")
        .setResultCode(0)
        .setLatitude(39.9)
        .setLongitude(116.4)
        .build();
}
```

- [ ] **Step 2: Run the test and verify it fails**

Run:

```powershell
mvn "-pl" "flink-job" "-Dtest=EnrichmentProcessFunctionTest" test
```

Expected: test fails because current code buffers and emits to `CHR_DLQ` after a timer.

- [ ] **Step 3: Change side output semantics**

In `EnrichmentProcessFunction`, replace `CHR_DLQ` with:

```java
public static final OutputTag<ChrEvent> ENRICHMENT_LATE =
    new OutputTag<>("enrichment-late", new GenericTypeInfo<>(ChrEvent.class));
```

In `processChr`, replace the CFG-missing branch with:

```java
if (cfg == null) {
    PmStat latestPm = latestPm();
    ctx.output(ENRICHMENT_LATE, chr);
    out.collect(new EnrichedChr(chr, null, latestPm));
    return;
}
```

Keep `processPm`, `processCfg`, and `flushBuffer` compiling. The buffer state can be removed if no longer used.

- [ ] **Step 4: Update `FlinkJobMain` side sink**

Replace the `chr-dlq` side-output sink for enrichment with a sink to `enrichment-late`:

```java
if (resultSinkConfig.dlqEnabled()) {
    KafkaSink<ChrEvent> enrichmentLateSink = KafkaSink.<ChrEvent>builder()
        .setBootstrapServers(bootstrap)
        .setRecordSerializer(KafkaRecordSerializationSchema.builder()
            .setTopic("enrichment-late")
            .setValueSerializationSchema(new FlinkAvroSerializationSchema<>(ChrEvent.class))
            .build())
        .build();
    enrichedRaw.getSideOutput(EnrichmentProcessFunction.ENRICHMENT_LATE)
        .sinkTo(enrichmentLateSink)
        .name("enrichment-late-sink");
}
```

- [ ] **Step 5: Run enrichment tests**

Run:

```powershell
mvn "-pl" "flink-job" "-Dtest=EnrichmentProcessFunctionTest" test
```

Expected: tests pass.

- [ ] **Step 6: Commit**

Run:

```powershell
git add flink-job/src/main/java/com/fdb/job/enrich/EnrichmentProcessFunction.java flink-job/src/main/java/com/fdb/job/FlinkJobMain.java flink-job/src/test/java/com/fdb/job/enrich/EnrichmentProcessFunctionTest.java
git commit -m "feat(flink): continue enrichment without cfg"
```

---

### Task 7: Rewire Flink DAG To Cell/User/Grid Anomalies

**Files:**
- Modify: `flink-job/src/main/java/com/fdb/job/FlinkJobMain.java`
- Modify: `flink-job/src/test/java/com/fdb/job/FlinkJobE2ETest.java`
- Modify: `flink-job/src/test/java/com/fdb/job/FlinkJobMainTest.java`

- [ ] **Step 1: Update E2E tests**

Remove the old `anomaly_pipeline_detects_all_rules` assertion for `AnomalyDetector`.

Add an E2E test that verifies active outputs:

```java
@Test
void entity_anomaly_pipelines_emit_cell_user_and_grid_types() throws Exception {
    List<AnomalyEvent> cell = runCellKpiCep();
    List<AnomalyEvent> user = runUserCep();
    List<AnomalyEvent> grid = runGridCoverage();

    assertThat(cell).extracting(AnomalyEvent::getAnomalyType)
        .contains(AnomalyType.CELL_RADIO_BAD);
    assertThat(user).extracting(AnomalyEvent::getAnomalyType)
        .contains(AnomalyType.USER_FAILURE);
    assertThat(grid).extracting(AnomalyEvent::getAnomalyType)
        .containsExactly(AnomalyType.COVERAGE_HOLE);
}
```

- [ ] **Step 2: Run the test and verify it fails**

Run:

```powershell
mvn "-pl" "flink-job" "-Dtest=FlinkJobE2ETest" test
```

Expected: failures because DAG/test still uses old detector imports and old schema fields.

- [ ] **Step 3: Remove old detector from `FlinkJobMain`**

Remove:

```java
import com.fdb.job.anomaly.AnomalyDetector;
```

Add:

```java
import com.fdb.job.anomaly.CellKpiCepAnomalyDetector;
import com.fdb.job.anomaly.UserEventCepAnomalyDetector;
```

Delete this old branch:

```java
DataStream<AnomalyEvent> cellAnomalies = enriched
    .keyBy(ec -> ec.chrEvent().getCellId().toString())
    .process(new AnomalyDetector(rules), new GenericTypeInfo<>(AnomalyEvent.class))
    .name("anomaly-detector")
    .uid("anomaly-detector");
```

- [ ] **Step 4: Add user anomalies after enrichment**

After `RuleConfig rules = JobConfig.load().rules();`, add:

```java
DataStream<AnomalyEvent> userAnomalies = UserEventCepAnomalyDetector
    .detect(enriched, rules)
    .name("user-event-cep-anomaly")
    .uid("user-event-cep-anomaly");
```

- [ ] **Step 5: Add cell anomalies after `cellKpi1m`**

After `cellKpi1m` is defined, add:

```java
DataStream<AnomalyEvent> cellAnomalies = CellKpiCepAnomalyDetector
    .detect(cellKpi1m, rules)
    .name("cell-kpi-cep-anomaly")
    .uid("cell-kpi-cep-anomaly");
```

- [ ] **Step 6: Pass three anomaly streams to result sinks**

Change the call to:

```java
ResultSinks.attachBusinessResultSinks(
    cellKpi1m, cellKpi5m, cellAnomalies, userAnomalies, coverageAnomalies,
    resultSinkConfig, bootstrap, icebergConfig, metricConfig);
```

- [ ] **Step 7: Run Flink job tests**

Run:

```powershell
mvn "-pl" "flink-job" "-Dtest=FlinkJobE2ETest,FlinkJobMainTest" test
```

Expected: tests pass.

- [ ] **Step 8: Commit**

Run:

```powershell
git add flink-job/src/main/java/com/fdb/job/FlinkJobMain.java flink-job/src/test/java/com/fdb/job/FlinkJobE2ETest.java flink-job/src/test/java/com/fdb/job/FlinkJobMainTest.java
git commit -m "feat(flink): rewire anomaly DAG to entity CEP"
```

---

### Task 8: Result Sinks, Tables, Topics, And Retention

**Files:**
- Modify: `flink-job/src/main/java/com/fdb/job/sink/ResultSinks.java`
- Modify: `flink-job/src/main/java/com/fdb/job/sink/StarRocksSinks.java`
- Modify: `flink-job/src/main/java/com/fdb/job/sink/HiveSinks.java`
- Modify: `flink-job/src/main/java/com/fdb/job/sink/IcebergConfig.java`
- Modify: `flink-job/src/main/java/com/fdb/job/sink/IcebergSinks.java`
- Modify: `flink-job/src/main/java/com/fdb/job/sink/AnomalyEventIcebergMapper.java`
- Modify tests under `flink-job/src/test/java/com/fdb/job/sink/`
- Modify: `scripts/init-kafka-topics.sh`
- Modify: `scripts/init-starrocks.sql`
- Modify: `docs/hive-schema.q`
- Read: `scripts/init-hive.sh`
- Modify: `scripts/retention-maintenance.sh`
- Modify: `scripts/deploy.sh`
- Modify relevant script tests in `scripts/test-*.sh`

- [ ] **Step 1: Write sink stage-id tests**

Update `ResultSinksTest.lists_all_business_stages_for_selected_sink`:

```java
assertThat(ResultSinks.businessStageIds(ResultSinkType.STARROCKS))
    .containsExactly(
        "starrocks-kpi-1m",
        "starrocks-kpi-5m",
        "starrocks-cell-anomaly",
        "starrocks-user-anomaly",
        "starrocks-grid-anomaly");
```

- [ ] **Step 2: Run sink tests and verify they fail**

Run:

```powershell
mvn "-pl" "flink-job" "-Dtest=ResultSinksTest,StarRocksSinksTest,IcebergConfigTest,IcebergSinksTest,HiveSinksTest,AnomalyEventIcebergMapperTest" test
```

Expected: tests fail because user anomaly sinks and entity schema are not present.

- [ ] **Step 3: Extend `ResultSinks` signature**

Change `attachBusinessResultSinks` and private attach methods to accept:

```java
DataStream<AnomalyEvent> cellAnomalies,
DataStream<AnomalyEvent> userAnomalies,
DataStream<AnomalyEvent> gridAnomalies
```

Add `anomalyStageId(sinkType, "user")` to `businessStageIds`.

For every sink type add user branch with dataset `user_anomaly_events`, window kind `ANOMALY`, and stage id `<sink>-user-anomaly`.

- [ ] **Step 4: Update StarRocks anomaly SQL**

Create one entity-aware insert SQL helper:

```java
private static String anomalyInsertSql(String table) {
    return "INSERT INTO " + table + " "
        + "(anomaly_id, detection_ts, event_ts, entity_type, entity_id, window_start_ts, window_end_ts, "
        + "imsi, site_id, cell_id, grid_id, latitude, longitude, anomaly_type, severity, rule_version, context_json) "
        + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
}
```

Add:

```java
public static org.apache.flink.connector.jdbc.core.datastream.sink.JdbcSink<AnomalyEvent> userAnomalySink() {
    StarRocksJdbcConfig config = resolveConfig(System.getenv(), System.getProperties());
    return JdbcSink.<AnomalyEvent>builder()
        .withQueryStatement(anomalyInsertSql(qualifiedTable(config.database(), "user_anomaly_events")),
            (statement, event) -> bindValues(statement, anomalyValues("user", event)))
        .withExecutionOptions(JdbcExecutionOptions.defaults())
        .buildAtLeastOnce(connectionOptions(config));
}
```

Use `anomalyValues(scope, event)` for cell/user/grid. Stable ID should include scope, `eventTs`, `entityType`, `entityId`, `anomalyType`, `ruleVersion`, and `contextJson` hash.

- [ ] **Step 5: Update Iceberg schema and config**

Add `String userAnomalyTable` to `IcebergConfig` between cell and grid tables.

Add:

```java
static TableIdentifier userAnomalyIdentifier(IcebergConfig config) {
    return TableIdentifier.of(config.database(), config.userAnomalyTable());
}
```

Entity-aware Iceberg anomaly schema must contain exactly:

```text
detection_ts, event_ts, entity_type, entity_id, window_start_ts, window_end_ts,
imsi, site_id, cell_id, grid_id, latitude, longitude,
anomaly_type, severity, rule_version, context_json, dt, hour
```

- [ ] **Step 6: Update Hive sinks and Hive DDL**

Add:

```java
static String userAnomalyOutputPath(String warehousePath) {
    return warehousePath + "/user_anomaly_events";
}

public static FileSink<AnomalyEvent> userAnomalySink() {
    return anomalySink(userAnomalyOutputPath(warehousePath()));
}
```

Add `user_anomaly_events` to `docs/hive-schema.q` with the same entity/window columns used by StarRocks, using Hive-compatible column types and `dt/hour` partitions.

- [ ] **Step 7: Update scripts**

Add `user-anomaly-events` to `scripts/init-kafka-topics.sh` with delete retention.

In `scripts/init-starrocks.sql`, use three entity-aware tables:

```sql
CREATE TABLE IF NOT EXISTS user_anomaly_events (
  anomaly_id VARCHAR(128) NOT NULL,
  detection_ts BIGINT NOT NULL,
  event_ts BIGINT NOT NULL,
  entity_type VARCHAR(16) NOT NULL,
  entity_id VARCHAR(128) NOT NULL,
  window_start_ts BIGINT NOT NULL,
  window_end_ts BIGINT NOT NULL,
  imsi VARCHAR(32),
  site_id VARCHAR(64),
  cell_id VARCHAR(64),
  grid_id VARCHAR(32),
  latitude DOUBLE,
  longitude DOUBLE,
  anomaly_type VARCHAR(64) NOT NULL,
  severity VARCHAR(16) NOT NULL,
  rule_version VARCHAR(32) NOT NULL,
  context_json STRING
) PRIMARY KEY(anomaly_id)
DISTRIBUTED BY HASH(anomaly_id) BUCKETS 8
PROPERTIES ("replication_num" = "1");
```

Use the same column set for `cell_anomaly_events` and `grid_anomaly_events`.

Add `user_anomaly_events` to retention loops and smoke/summary count queries.

- [ ] **Step 8: Run sink and script tests**

Run:

```powershell
mvn "-pl" "flink-job" "-Dtest=ResultSinksTest,StarRocksSinksTest,IcebergConfigTest,IcebergSinksTest,HiveSinksTest,AnomalyEventIcebergMapperTest" test
bash -n scripts/deploy.sh
bash -n scripts/init-kafka-topics.sh
bash -n scripts/init-hive.sh
bash -n scripts/retention-maintenance.sh
bash scripts/test-init-kafka-topics.sh
bash scripts/test-retention-maintenance.sh
```

Expected: all listed commands pass.

- [ ] **Step 9: Commit**

Run:

```powershell
git add flink-job/src/main/java/com/fdb/job/sink flink-job/src/test/java/com/fdb/job/sink scripts
git commit -m "feat(sinks): add user anomaly result outputs"
```

---

### Task 9: Observability API User Anomaly Endpoint

**Files:**
- Modify: `observability-api/src/main/java/com/fdb/observability/model/AnomalyResultRow.java`
- Modify: `observability-api/src/main/java/com/fdb/observability/service/StarRocksQueryService.java`
- Modify: `observability-api/src/main/java/com/fdb/observability/ObservabilityApiMain.java`
- Modify: `observability-api/src/main/java/com/fdb/observability/service/ObservabilitySnapshotService.java`
- Modify: `observability-api/src/test/java/com/fdb/observability/ObservabilityResultEndpointsTest.java`
- Modify: `observability-api/src/test/java/com/fdb/observability/service/StarRocksQueryServiceTest.java`
- Modify: `observability-api/src/test/java/com/fdb/observability/service/ObservabilitySnapshotServiceTest.java`

- [ ] **Step 1: Write API endpoint test**

Add to `ObservabilityResultEndpointsTest`:

```java
@Test
void returnsUserAnomalyResults() throws Exception {
  FakeStarRocksQueryService queries = new FakeStarRocksQueryService();
  server = startServer(new ObservabilitySnapshotService(), queries);

  JsonNode body = get("/api/results/anomalies/user?imsi=460001234567890&entityId=460001234567890");

  assertThat(queries.lastUserAnomalyParams).containsEntry("imsi", "460001234567890");
  assertThat(body.get(0).get("entityType").asText()).isEqualTo("USER");
  assertThat(body.get(0).get("entityId").asText()).isEqualTo("460001234567890");
  assertThat(body.get(0).get("windowStartTs").asLong()).isEqualTo(100L);
  assertThat(body.get(0).get("windowEndTs").asLong()).isEqualTo(900L);
}
```

- [ ] **Step 2: Run API tests and verify they fail**

Run:

```powershell
mvn "-pl" "observability-api" "-Dtest=ObservabilityResultEndpointsTest,StarRocksQueryServiceTest" test
```

Expected: compilation fails because user query methods and new row fields do not exist.

- [ ] **Step 3: Extend `AnomalyResultRow`**

Use:

```java
public record AnomalyResultRow(
    Long detectionTs,
    Long eventTs,
    String entityType,
    String entityId,
    Long windowStartTs,
    Long windowEndTs,
    String imsi,
    String siteId,
    String cellId,
    String gridId,
    String anomalyType,
    String severity,
    String contextJson,
    Double latitude,
    Double longitude,
    String ruleVersion
) {
}
```

- [ ] **Step 4: Add query method and filters**

In `StarRocksQueryService`, add:

```java
public List<AnomalyResultRow> queryUserAnomalies(Map<String, String> queryParameters) throws SQLException {
  return queryAnomalies("user_anomaly_events", queryParameters);
}
```

Add filters to anomaly `QuerySpec`:

```java
new Filter("entityType", "entity_type"),
new Filter("entityId", "entity_id"),
new Filter("imsi", "imsi"),
new Filter("siteId", "site_id"),
new Filter("cellId", "cell_id"),
new Filter("gridId", "grid_id"),
new Filter("severity", "severity"),
new Filter("anomalyType", "anomaly_type")
```

Map new fields in `readAnomalyRow`.

- [ ] **Step 5: Register endpoint**

Add to `ObservabilityApiMain.createServer`:

```java
server.createContext("/api/results/anomalies/user",
    exchange -> writeQueryJson(exchange, () -> queryService.queryUserAnomalies(queryParameters(exchange))));
```

- [ ] **Step 6: Add default sink summaries**

In `ObservabilitySnapshotService`, add user anomaly defaults for Kafka, StarRocks, Hive, and Iceberg:

```java
defaults.add(sinkDefault("starrocks-user-anomaly", "User Anomaly StarRocks Sink",
    "starrocks", "user_anomaly_events", "ANOMALY", now));
```

Repeat with `kafka-user-anomaly`, `hive-user-anomaly`, and `iceberg-user-anomaly`.

- [ ] **Step 7: Run API tests**

Run:

```powershell
mvn "-pl" "observability-api" test
```

Expected: observability-api tests pass.

- [ ] **Step 8: Commit**

Run:

```powershell
git add observability-api/src/main/java observability-api/src/test/java
git commit -m "feat(observability): expose user anomaly results"
```

---

### Task 10: Frontend User Anomaly View And Flow Graph

**Files:**
- Modify: `frontend/src/types/observability.ts`
- Modify: `frontend/src/api/client.ts`
- Modify: `frontend/src/App.tsx`
- Modify: `frontend/src/pages/CellAnomalies.tsx`
- Modify: `frontend/src/pages/GridAnomalies.tsx`
- Create: `frontend/src/pages/UserAnomalies.tsx`
- Create: `frontend/src/pages/UserAnomalies.test.tsx`
- Modify: `frontend/src/components/flowEdges.ts`
- Modify: `frontend/src/components/flowEdges.test.ts`
- Modify: `frontend/src/components/StreamingFlowGraph.tsx`
- Modify: `frontend/src/App.test.tsx`

- [ ] **Step 1: Add frontend API and type tests**

In `App.test.tsx`, extend mocked client:

```ts
fetchUserAnomalies: vi.fn()
```

Add the nav assertion:

```ts
expect(screen.getByRole('button', { name: '用户异常' })).toBeInTheDocument();
```

Add a page load test:

```ts
test('loads mocked user anomaly rows from the user anomaly page', async () => {
  client.fetchUserAnomalies.mockResolvedValue([
    {
      detectionTs: 1717400000000,
      eventTs: 1717399999000,
      entityType: 'USER',
      entityId: '460001234567890',
      windowStartTs: 1717399400000,
      windowEndTs: 1717399999000,
      imsi: '460001234567890',
      siteId: 'SITE-001',
      cellId: 'CELL-001',
      gridId: 'wx4g0e',
      anomalyType: 'USER_QOE_BAD',
      severity: 'HIGH',
      contextJson: '{"metric":"latencyMs"}',
      latitude: 39.9,
      longitude: 116.4,
      ruleVersion: 'v1.0'
    }
  ]);

  render(<App />);
  fireEvent.click(await screen.findByRole('button', { name: '用户异常' }));

  expect(await screen.findByRole('columnheader', { name: 'entityType' })).toBeInTheDocument();
  expect(await screen.findByText('USER_QOE_BAD')).toBeInTheDocument();
  expect(client.fetchUserAnomalies).toHaveBeenCalledWith({});
});
```

- [ ] **Step 2: Update flow edge tests**

Change active StarRocks expectation to include:

```ts
expect(edges).toContainEqual(['enrichment', 'starrocks-user-anomaly']);
```

Add current sink stage ids:

```ts
'kafka-user-anomaly',
'starrocks-user-anomaly',
'hive-user-anomaly',
'iceberg-user-anomaly'
```

- [ ] **Step 3: Run frontend tests and verify they fail**

Run:

```powershell
Push-Location frontend
npm test -- --run
Pop-Location
```

Expected: tests fail because user anomaly page/API and flow nodes do not exist.

- [ ] **Step 4: Extend types and client**

Add to `AnomalyQueryParams`:

```ts
entityType?: string;
entityId?: string;
imsi?: string;
```

Extend `AnomalyResultRow`:

```ts
entityType: string | null;
entityId: string | null;
windowStartTs: number | null;
windowEndTs: number | null;
imsi: string | null;
```

Add to `client.ts`:

```ts
export function fetchUserAnomalies(params: AnomalyQueryParams): Promise<AnomalyResultRow[]> {
  return getJson<AnomalyResultRow[]>(withQuery('/api/results/anomalies/user', params));
}
```

- [ ] **Step 5: Create `UserAnomalies.tsx`**

Create `UserAnomalies.tsx` with local `rows`, `params`, `loading`, and `error` state. The component must render an inline `Form<AnomalyQueryParams>` with `startTs`, `endTs`, `imsi`, `cellId`, `severity`, `anomalyType`, `entityId`, and `limit` filters, call `fetchUserAnomalies(params)` in `useEffect`, and render a table with these columns:

```ts
const columns: ColumnsType<AnomalyResultRow> = [
  { title: 'detectionTs', dataIndex: 'detectionTs', width: 150 },
  { title: 'eventTs', dataIndex: 'eventTs', width: 150 },
  { title: 'entityType', dataIndex: 'entityType', width: 120 },
  { title: 'entityId', dataIndex: 'entityId', width: 180 },
  { title: 'windowStartTs', dataIndex: 'windowStartTs', width: 150 },
  { title: 'windowEndTs', dataIndex: 'windowEndTs', width: 150 },
  { title: 'imsi', dataIndex: 'imsi', width: 180 },
  { title: 'siteId', dataIndex: 'siteId', width: 130 },
  { title: 'cellId', dataIndex: 'cellId', width: 140 },
  { title: 'anomalyType', dataIndex: 'anomalyType', width: 160 },
  { title: 'severity', dataIndex: 'severity', width: 110 },
  { title: 'ruleVersion', dataIndex: 'ruleVersion', width: 120 },
  { title: 'contextJson', dataIndex: 'contextJson', width: 280, ellipsis: true }
];
```

- [ ] **Step 6: Wire App navigation**

Add page key:

```ts
| 'userAnomalies'
```

Add menu entry:

```ts
{ key: 'userAnomalies', label: '用户异常' },
```

Add page render:

```tsx
{page === 'userAnomalies' ? <UserAnomalies /> : null}
```

- [ ] **Step 7: Update flow graph**

Add node positions in `StreamingFlowGraph.tsx` for:

```ts
'kafka-user-anomaly'
'starrocks-user-anomaly'
'hive-user-anomaly'
'iceberg-user-anomaly'
```

Add flow edges from `enrichment` to user anomaly sink stages. Add user anomaly sink stages to tests.

- [ ] **Step 8: Update existing cell and grid anomaly pages**

In `CellAnomalies.tsx` and `GridAnomalies.tsx`, add table columns:

```ts
{ title: 'entityType', dataIndex: 'entityType', width: 120 },
{ title: 'entityId', dataIndex: 'entityId', width: 180 },
{ title: 'windowStartTs', dataIndex: 'windowStartTs', width: 150 },
{ title: 'windowEndTs', dataIndex: 'windowEndTs', width: 150 }
```

Add an `entityId` filter to each page:

```tsx
<Form.Item label="entityId" name="entityId">
  <Input placeholder="entity id" allowClear style={{ width: 180 }} />
</Form.Item>
```

Update `anomalyRowKey` and `gridAnomalyRowKey` to include `entityType`, `entityId`, `windowStartTs`, and `windowEndTs`.

- [ ] **Step 9: Run frontend tests and build**

Run:

```powershell
Push-Location frontend
npm test -- --run
npm run build
Pop-Location
```

Expected: tests and build pass.

- [ ] **Step 10: Commit**

Run:

```powershell
git add frontend/src
git commit -m "feat(frontend): add user anomaly results view"
```

---

### Task 11: End-To-End Verification And Documentation Refresh

**Files:**
- Modify: `docs/superpowers/specs/2026-04-29-flink-data-balance-design.md`
- Modify: `README.md` if it already documents result outputs or anomaly pages.

- [ ] **Step 1: Run targeted backend tests**

Run:

```powershell
mvn "-pl" "common,flink-job,observability-api" test
```

Expected: Maven test run exits 0.

- [ ] **Step 2: Run frontend verification**

Run:

```powershell
Push-Location frontend
npm test -- --run
npm run build
Pop-Location
```

Expected: Vitest and TypeScript/Vite build exit 0.

- [ ] **Step 3: Run script checks**

Run:

```powershell
bash -n scripts/deploy.sh
bash -n scripts/init-kafka-topics.sh
bash -n scripts/init-hive.sh
bash -n scripts/retention-maintenance.sh
bash scripts/test-init-kafka-topics.sh
bash scripts/test-retention-maintenance.sh
```

Expected: all commands exit 0.

- [ ] **Step 4: Run local smoke if shared infra is already running**

Run:

```powershell
bash scripts/deploy.sh local check
bash scripts/deploy.sh local init
bash scripts/deploy.sh local submit
bash scripts/deploy.sh local smoke
```

Expected: local check/init/submit/smoke pass when shared infra is available. If shared infra is not running, record that the full smoke was skipped and include the failed prerequisite command output in the final implementation notes.

- [ ] **Step 5: Refresh spec status**

In `docs/superpowers/specs/2026-04-29-flink-data-balance-design.md`, mark these acceptance items complete only after the corresponding verification commands pass:

```text
小区异常由 CellKpi MIN_1 后的 CEP 检测产出
用户异常由 enrich 后的 CEP 检测产出
KPI 1m、KPI 5m、小区异常、用户异常、栅格异常均跟随 FDB_RESULT_SINK 写入
StarRocks/Iceberg/Hive/Kafka 均具备三类异常输出
控制台提供用户异常页面
```

- [ ] **Step 6: Final GitNexus check**

Run GitNexus `detect_changes` with `scope=all`.

Expected: affected processes match the implemented areas: common Avro schema, Flink anomaly pipeline, result sinks, observability result API, frontend anomaly views, scripts.

- [ ] **Step 7: Final commit**

Run:

```powershell
git add docs README.md
git commit -m "docs: mark entity anomaly CEP implementation status"
```

If `README.md` has no relevant section, do not create unrelated README content; commit only the spec status refresh.

---

## Execution Notes

- Do not reintroduce the old event-level cell anomaly branch in `FlinkJobMain`.
- Keep `CoverageHoleDetector` as the grid anomaly branch and only update its output model.
- Keep the selected result sink model: each run writes business outputs to one selected result sink.
- Do not add migration notes for existing anomaly tables; dev initialization rebuilds the three anomaly tables.
- Before every Java-edit commit, run GitNexus `detect_changes` on staged changes.
- If a task touches Docker Compose infrastructure, first inspect `../shared-data-infra` and run `docker compose -f docker/docker-compose.yml --profile e2e config`. This plan does not require Docker Compose changes.

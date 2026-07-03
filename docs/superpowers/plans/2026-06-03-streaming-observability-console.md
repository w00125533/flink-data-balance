# Streaming Observability Console Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在 `flink-data-balance` 中新增嵌入式 React 观测控制台、轻量 Java 观测 API、Prometheus 指标采集和 Grafana dashboard，用流程图实时展示流处理状态、数据源延迟、负载迁移和 Sink 写入性能。

**Architecture:** 前端作为新增 `frontend/` Vite 模块，使用 Ant Design 和 AntV G6 展示流程图、状态面板、迁移时间线和 Grafana 入口。后端新增 `observability-api/` Java 模块，提供 REST + SSE 聚合接口，v1 先用内存与 fixture 数据打通端到端，后续接入 Flink REST、Prometheus 查询和迁移事件文件。Docker Compose 增加 frontend、observability-api、Prometheus、Grafana 服务。

**Tech Stack:** Java 17, Maven, JUnit 5, React 18, TypeScript, Vite, Ant Design, AntV G6, Prometheus, Grafana, Docker Compose.

---

## File Structure

- Create: `observability-api/pom.xml`  
  观测 API Maven 模块配置，依赖 JDK 内置 HTTP Server、JUnit 5、Jackson。
- Create: `observability-api/src/main/java/com/fdb/observability/ObservabilityApiMain.java`  
  Java HTTP 服务入口，监听 `FDB_OBSERVABILITY_PORT`，注册 REST 与 SSE 路由。
- Create: `observability-api/src/main/java/com/fdb/observability/model/StageStatus.java`  
  流程阶段状态模型。
- Create: `observability-api/src/main/java/com/fdb/observability/model/SourceSummary.java`  
  CHR/MR/CM 数据源摘要模型。
- Create: `observability-api/src/main/java/com/fdb/observability/model/MigrationEvent.java`  
  负载迁移事件模型。
- Create: `observability-api/src/main/java/com/fdb/observability/model/SinkSummary.java`  
  MySQL/Hive/Iceberg Sink 性能摘要模型。
- Create: `observability-api/src/main/java/com/fdb/observability/service/ObservabilitySnapshotService.java`  
  v1 状态聚合服务，先提供确定性 fixture；后续替换为 Flink/Prometheus 数据源。
- Create: `observability-api/src/test/java/com/fdb/observability/service/ObservabilitySnapshotServiceTest.java`  
  覆盖状态摘要、迁移事件、Sink 摘要。
- Modify: `pom.xml`  
  将 `observability-api` 加入 Maven modules。
- Create: `frontend/package.json`  
  前端依赖、开发脚本和测试脚本。
- Create: `frontend/index.html`
- Create: `frontend/vite.config.ts`
- Create: `frontend/tsconfig.json`
- Create: `frontend/src/main.tsx`
- Create: `frontend/src/App.tsx`
- Create: `frontend/src/types/observability.ts`  
  前端共享类型，与 Java API JSON 字段保持一致。
- Create: `frontend/src/api/client.ts`  
  REST fetch 与 SSE client。
- Create: `frontend/src/components/StreamingFlowGraph.tsx`
- Create: `frontend/src/components/StageStatusPanel.tsx`
- Create: `frontend/src/components/SourceLatencyCard.tsx`
- Create: `frontend/src/components/MigrationDiffPanel.tsx`
- Create: `frontend/src/components/GrafanaEmbedPanel.tsx`
- Create: `frontend/src/pages/FlowOverview.tsx`
- Create: `frontend/src/pages/MigrationTimeline.tsx`
- Create: `frontend/src/pages/MetricsDashboard.tsx`
- Create: `frontend/src/App.test.tsx`
- Create: `docker/prometheus/prometheus.yml`
- Create: `docker/grafana/provisioning/datasources/prometheus.yml`
- Create: `docker/grafana/provisioning/dashboards/dashboards.yml`
- Create: `docs/grafana/streaming-observability-dashboard.json`
- Modify: `docker/docker-compose.yml`
- Modify: `README.md`
- Modify: `AGENTS.md`
- Modify: `docs/superpowers/specs/2026-04-29-flink-data-balance-design.md`

---

### Task 1: Wire Maven Module And API Models

**Files:**
- Modify: `pom.xml`
- Create: `observability-api/pom.xml`
- Create: `observability-api/src/main/java/com/fdb/observability/model/StageStatus.java`
- Create: `observability-api/src/main/java/com/fdb/observability/model/SourceSummary.java`
- Create: `observability-api/src/main/java/com/fdb/observability/model/MigrationEvent.java`
- Create: `observability-api/src/main/java/com/fdb/observability/model/SinkSummary.java`

- [ ] **Step 1: Add `observability-api` to parent Maven modules**

In root `pom.xml`, add this module next to existing modules:

```xml
<module>observability-api</module>
```

- [ ] **Step 2: Create `observability-api/pom.xml`**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>

  <parent>
    <groupId>com.fdb</groupId>
    <artifactId>flink-data-balance-parent</artifactId>
    <version>0.1.0-SNAPSHOT</version>
  </parent>

  <artifactId>observability-api</artifactId>
  <name>observability-api</name>

  <dependencies>
    <dependency>
      <groupId>com.fasterxml.jackson.core</groupId>
      <artifactId>jackson-databind</artifactId>
      <version>2.17.2</version>
    </dependency>
    <dependency>
      <groupId>org.junit.jupiter</groupId>
      <artifactId>junit-jupiter</artifactId>
      <scope>test</scope>
    </dependency>
    <dependency>
      <groupId>org.assertj</groupId>
      <artifactId>assertj-core</artifactId>
      <scope>test</scope>
    </dependency>
  </dependencies>
</project>
```

- [ ] **Step 3: Create model records**

Create `StageStatus.java`:

```java
package com.fdb.observability.model;

public record StageStatus(
    String stageId,
    String label,
    String status,
    double inEps,
    double outEps,
    long latencyP50Ms,
    long latencyP95Ms,
    long watermarkLagMs,
    long dlqCount,
    String summary,
    String updatedAt) {
}
```

Create `SourceSummary.java`:

```java
package com.fdb.observability.model;

public record SourceSummary(
    String source,
    String status,
    double eps,
    long kafkaLag,
    long eventDelayMs,
    String summary,
    String updatedAt) {
}
```

Create `MigrationEvent.java`:

```java
package com.fdb.observability.model;

import java.util.List;

public record MigrationEvent(
    String migrationId,
    int routingVersionBefore,
    int routingVersionAfter,
    String reason,
    String status,
    String startedAt,
    String completedAt,
    List<MovedVBucket> movedVbuckets) {

  public record MovedVBucket(
      int vbucket,
      int fromSubtask,
      int toSubtask,
      double estimatedEps,
      List<String> hotKeys) {
  }
}
```

Create `SinkSummary.java`:

```java
package com.fdb.observability.model;

public record SinkSummary(
    String sink,
    String window,
    String status,
    long rowsWritten,
    long writeLatencyP95Ms,
    String summary,
    String updatedAt) {
}
```

- [ ] **Step 4: Run compile for the new module**

Run:

```bash
mvn -pl observability-api test
```

Expected: build succeeds with no tests or with zero failures.

- [ ] **Step 5: Commit**

```bash
git add pom.xml observability-api/pom.xml observability-api/src/main/java/com/fdb/observability/model
git commit -m "feat: add observability api module models"
```

### Task 2: Implement Snapshot Service With Tests

**Files:**
- Create: `observability-api/src/main/java/com/fdb/observability/service/ObservabilitySnapshotService.java`
- Create: `observability-api/src/test/java/com/fdb/observability/service/ObservabilitySnapshotServiceTest.java`

- [ ] **Step 1: Write failing tests**

```java
package com.fdb.observability.service;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class ObservabilitySnapshotServiceTest {

  private final ObservabilitySnapshotService service = new ObservabilitySnapshotService();

  @Test
  void returnsCoreStages() {
    assertThat(service.stageStatuses())
        .extracting("stageId")
        .contains("chr-source", "mr-source", "cm-source", "enrichment", "load-coordinator", "iceberg-sink");
  }

  @Test
  void returnsThreeSourceSummaries() {
    assertThat(service.sourceSummaries())
        .extracting("source")
        .containsExactlyInAnyOrder("chr", "mr", "cm");
  }

  @Test
  void includesMigrationEventWithMovedVbucket() {
    assertThat(service.migrationEvents()).hasSize(1);
    assertThat(service.migrationEvents().get(0).movedVbuckets()).isNotEmpty();
  }

  @Test
  void includesHiveAndIcebergSinkSummaries() {
    assertThat(service.sinkSummaries())
        .extracting("sink")
        .contains("mysql", "hive", "iceberg");
  }
}
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
mvn -pl observability-api test
```

Expected: FAIL because `ObservabilitySnapshotService` does not exist.

- [ ] **Step 3: Implement `ObservabilitySnapshotService`**

```java
package com.fdb.observability.service;

import com.fdb.observability.model.MigrationEvent;
import com.fdb.observability.model.SinkSummary;
import com.fdb.observability.model.SourceSummary;
import com.fdb.observability.model.StageStatus;
import java.time.OffsetDateTime;
import java.util.List;

public final class ObservabilitySnapshotService {

  public List<StageStatus> stageStatuses() {
    String now = OffsetDateTime.now().toString();
    return List.of(
        new StageStatus("chr-source", "CHR Source", "healthy", 12800.0, 12800.0, 12, 32, 850, 0, "CHR 输入稳定", now),
        new StageStatus("mr-source", "MR Source", "healthy", 3100.0, 3100.0, 18, 45, 1200, 0, "MR 话统输入正常", now),
        new StageStatus("cm-source", "CM Source", "healthy", 12.0, 12.0, 5, 20, 300, 0, "CM 配置版本稳定", now),
        new StageStatus("kafka", "Kafka Topics", "healthy", 15912.0, 15880.0, 15, 40, 900, 0, "Kafka lag 处于可控范围", now),
        new StageStatus("assigner", "VBucket Assigner", "warning", 15880.0, 15720.0, 28, 95, 1100, 0, "subtask-3 负载偏高", now),
        new StageStatus("enrichment", "Enrichment Process", "healthy", 15720.0, 15650.0, 45, 160, 1350, 2, "CHR/MR/CM 关联正常", now),
        new StageStatus("load-coordinator", "Load Coordinator", "warning", 64.0, 1.0, 8, 25, 0, 0, "检测到 imbalance ratio 2.4", now),
        new StageStatus("mysql-sink", "MySQL Sink", "healthy", 900.0, 900.0, 20, 80, 0, 0, "异常事件写入正常", now),
        new StageStatus("hive-sink", "Hive Sink", "healthy", 280.0, 280.0, 60, 240, 0, 0, "Parquet 文件提交正常", now),
        new StageStatus("iceberg-sink", "Iceberg Sink", "warning", 280.0, 260.0, 80, 420, 0, 0, "Iceberg 写入延迟高于 Hive", now));
  }

  public List<SourceSummary> sourceSummaries() {
    String now = OffsetDateTime.now().toString();
    return List.of(
        new SourceSummary("chr", "healthy", 12800.0, 120, 850, "CHR 输入稳定，事件时间延迟 850ms", now),
        new SourceSummary("mr", "healthy", 3100.0, 80, 1200, "MR 话统输入正常，参与关联小区 3000 个", now),
        new SourceSummary("cm", "healthy", 12.0, 0, 300, "CM 配置版本稳定，最近无拓扑突变", now));
  }

  public List<MigrationEvent> migrationEvents() {
    return List.of(new MigrationEvent(
        "mig-20260603-001",
        17,
        18,
        "subtask 3 EPS 是中位数 2.4 倍",
        "applied",
        "2026-06-03T10:12:20+08:00",
        "2026-06-03T10:12:42+08:00",
        List.of(new MigrationEvent.MovedVBucket(42, 3, 1, 1800.0, List.of("cell-001", "cell-008")))));
  }

  public List<SinkSummary> sinkSummaries() {
    String now = OffsetDateTime.now().toString();
    return List.of(
        new SinkSummary("mysql", "anomaly", "healthy", 1800, 80, "异常事件 JDBC 幂等写入正常", now),
        new SinkSummary("hive", "1m", "healthy", 360, 240, "Hive 1m KPI Parquet commit 正常", now),
        new SinkSummary("iceberg", "1m", "warning", 340, 420, "Iceberg 1m KPI 写入延迟高于 Hive", now),
        new SinkSummary("hive", "5m", "healthy", 72, 260, "Hive 5m KPI Parquet commit 正常", now),
        new SinkSummary("iceberg", "5m", "warning", 70, 460, "Iceberg 5m KPI 写入延迟高于 Hive", now));
  }
}
```

- [ ] **Step 4: Run tests to verify pass**

Run:

```bash
mvn -pl observability-api test
```

Expected: PASS, 4 tests.

- [ ] **Step 5: Commit**

```bash
git add observability-api/src/main/java/com/fdb/observability/service observability-api/src/test/java/com/fdb/observability/service
git commit -m "feat: add observability snapshot service"
```

### Task 3: Implement REST And SSE API

**Files:**
- Create: `observability-api/src/main/java/com/fdb/observability/ObservabilityApiMain.java`
- Create: `observability-api/src/test/java/com/fdb/observability/ObservabilityApiMainTest.java`

- [ ] **Step 1: Write route serialization test**

```java
package com.fdb.observability;

import static org.assertj.core.api.Assertions.assertThat;

import com.fdb.observability.service.ObservabilitySnapshotService;
import org.junit.jupiter.api.Test;

class ObservabilityApiMainTest {

  @Test
  void serializesStageStatusJson() throws Exception {
    String json = ObservabilityApiMain.toJson(new ObservabilitySnapshotService().stageStatuses());
    assertThat(json).contains("\"stageId\":\"chr-source\"");
    assertThat(json).contains("\"stageId\":\"iceberg-sink\"");
  }
}
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
mvn -pl observability-api test
```

Expected: FAIL because `ObservabilityApiMain` does not exist.

- [ ] **Step 3: Implement API main**

```java
package com.fdb.observability;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fdb.observability.service.ObservabilitySnapshotService;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.Executors;

public final class ObservabilityApiMain {
  private static final ObjectMapper JSON = new ObjectMapper();

  private ObservabilityApiMain() {
  }

  public static void main(String[] args) throws Exception {
    int port = Integer.parseInt(System.getenv().getOrDefault("FDB_OBSERVABILITY_PORT", "18080"));
    ObservabilitySnapshotService service = new ObservabilitySnapshotService();
    HttpServer server = HttpServer.create(new InetSocketAddress("0.0.0.0", port), 0);
    server.createContext("/api/flow/status", exchange -> writeJson(exchange, service.stageStatuses()));
    server.createContext("/api/flow/sources", exchange -> writeJson(exchange, service.sourceSummaries()));
    server.createContext("/api/flow/migrations", exchange -> writeJson(exchange, service.migrationEvents()));
    server.createContext("/api/flow/sinks", exchange -> writeJson(exchange, service.sinkSummaries()));
    server.createContext("/api/metrics/summary", exchange -> writeJson(exchange, Map.of(
        "stages", service.stageStatuses(),
        "sources", service.sourceSummaries(),
        "sinks", service.sinkSummaries())));
    server.createContext("/api/events/stream", exchange -> writeSse(exchange, service));
    server.setExecutor(Executors.newCachedThreadPool());
    server.start();
  }

  static String toJson(Object value) throws IOException {
    return JSON.writeValueAsString(value);
  }

  private static void writeJson(HttpExchange exchange, Object body) throws IOException {
    byte[] bytes = toJson(body).getBytes(StandardCharsets.UTF_8);
    exchange.getResponseHeaders().add("Content-Type", "application/json; charset=utf-8");
    exchange.getResponseHeaders().add("Access-Control-Allow-Origin", "*");
    exchange.sendResponseHeaders(200, bytes.length);
    try (OutputStream output = exchange.getResponseBody()) {
      output.write(bytes);
    }
  }

  private static void writeSse(HttpExchange exchange, ObservabilitySnapshotService service) throws IOException {
    exchange.getResponseHeaders().add("Content-Type", "text/event-stream; charset=utf-8");
    exchange.getResponseHeaders().add("Cache-Control", "no-cache");
    exchange.getResponseHeaders().add("Access-Control-Allow-Origin", "*");
    exchange.sendResponseHeaders(200, 0);
    try (OutputStream output = exchange.getResponseBody()) {
      String payload = "event: stage_status\n"
          + "data: " + toJson(service.stageStatuses()) + "\n\n"
          + "event: migration_event\n"
          + "data: " + toJson(service.migrationEvents()) + "\n\n";
      output.write(payload.getBytes(StandardCharsets.UTF_8));
      output.flush();
    }
  }
}
```

- [ ] **Step 4: Run tests**

Run:

```bash
mvn -pl observability-api test
```

Expected: PASS.

- [ ] **Step 5: Smoke test API locally**

Run:

```bash
mvn -pl observability-api exec:java -Dexec.mainClass=com.fdb.observability.ObservabilityApiMain
```

In another terminal:

```bash
curl -fsS http://localhost:18080/api/flow/status
```

Expected: JSON includes `chr-source`, `load-coordinator`, `iceberg-sink`.

- [ ] **Step 6: Commit**

```bash
git add observability-api/src/main/java/com/fdb/observability/ObservabilityApiMain.java observability-api/src/test/java/com/fdb/observability/ObservabilityApiMainTest.java
git commit -m "feat: expose observability rest and sse api"
```

### Task 4: Scaffold Frontend Module

**Files:**
- Create: `frontend/package.json`
- Create: `frontend/index.html`
- Create: `frontend/vite.config.ts`
- Create: `frontend/tsconfig.json`
- Create: `frontend/src/main.tsx`
- Create: `frontend/src/App.tsx`
- Create: `frontend/src/types/observability.ts`

- [ ] **Step 1: Create `frontend/package.json`**

```json
{
  "name": "flink-data-balance-observability-ui",
  "private": true,
  "version": "0.1.0",
  "type": "module",
  "scripts": {
    "dev": "vite --host 0.0.0.0",
    "build": "tsc -b && vite build",
    "test": "vitest run"
  },
  "dependencies": {
    "@ant-design/icons": "^5.3.7",
    "@antv/g6": "^5.0.49",
    "@vitejs/plugin-react": "^4.3.1",
    "antd": "^5.18.3",
    "echarts": "^5.5.1",
    "react": "^18.3.1",
    "react-dom": "^18.3.1"
  },
  "devDependencies": {
    "@testing-library/jest-dom": "^6.4.6",
    "@testing-library/react": "^15.0.7",
    "@types/react": "^18.3.3",
    "@types/react-dom": "^18.3.0",
    "jsdom": "^24.1.0",
    "typescript": "^5.4.5",
    "vite": "^5.3.1",
    "vitest": "^1.6.0"
  }
}
```

- [ ] **Step 2: Add Vite and TypeScript config**

Create `frontend/index.html`:

```html
<!doctype html>
<html lang="zh-CN">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>Flink Data Balance Observability</title>
  </head>
  <body>
    <div id="root"></div>
    <script type="module" src="/src/main.tsx"></script>
  </body>
</html>
```

Create `frontend/vite.config.ts`:

```ts
import react from '@vitejs/plugin-react';
import { defineConfig } from 'vite';

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      '/api': 'http://localhost:18080'
    }
  }
});
```

Create `frontend/tsconfig.json`:

```json
{
  "compilerOptions": {
    "target": "ES2020",
    "useDefineForClassFields": true,
    "lib": ["DOM", "DOM.Iterable", "ES2020"],
    "allowJs": false,
    "skipLibCheck": true,
    "esModuleInterop": true,
    "allowSyntheticDefaultImports": true,
    "strict": true,
    "forceConsistentCasingInFileNames": true,
    "module": "ESNext",
    "moduleResolution": "Node",
    "resolveJsonModule": true,
    "isolatedModules": true,
    "noEmit": true,
    "jsx": "react-jsx"
  },
  "include": ["src"]
}
```

- [ ] **Step 3: Add shared frontend types**

```ts
export type StageStatusValue = 'healthy' | 'warning' | 'critical' | 'idle';

export interface StageStatus {
  stageId: string;
  label: string;
  status: StageStatusValue;
  inEps: number;
  outEps: number;
  latencyP50Ms: number;
  latencyP95Ms: number;
  watermarkLagMs: number;
  dlqCount: number;
  summary: string;
  updatedAt: string;
}

export interface SourceSummary {
  source: 'chr' | 'mr' | 'cm';
  status: StageStatusValue;
  eps: number;
  kafkaLag: number;
  eventDelayMs: number;
  summary: string;
  updatedAt: string;
}

export interface MovedVBucket {
  vbucket: number;
  fromSubtask: number;
  toSubtask: number;
  estimatedEps: number;
  hotKeys: string[];
}

export interface MigrationEvent {
  migrationId: string;
  routingVersionBefore: number;
  routingVersionAfter: number;
  reason: string;
  status: string;
  startedAt: string;
  completedAt: string;
  movedVbuckets: MovedVBucket[];
}

export interface SinkSummary {
  sink: 'mysql' | 'hive' | 'iceberg';
  window: string;
  status: StageStatusValue;
  rowsWritten: number;
  writeLatencyP95Ms: number;
  summary: string;
  updatedAt: string;
}
```

- [ ] **Step 4: Add minimal React entry**

Create `frontend/src/main.tsx`:

```tsx
import React from 'react';
import ReactDOM from 'react-dom/client';
import { ConfigProvider } from 'antd';
import zhCN from 'antd/locale/zh_CN';
import App from './App';
import 'antd/dist/reset.css';

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <ConfigProvider locale={zhCN}>
      <App />
    </ConfigProvider>
  </React.StrictMode>
);
```

Create `frontend/src/App.tsx`:

```tsx
import { Layout, Menu } from 'antd';
import FlowOverview from './pages/FlowOverview';

export default function App() {
  return (
    <Layout style={{ minHeight: '100vh' }}>
      <Layout.Sider theme="light" width={220}>
        <Menu
          mode="inline"
          selectedKeys={['flow']}
          items={[
            { key: 'flow', label: '流处理总览' },
            { key: 'migrations', label: '负载迁移' },
            { key: 'metrics', label: '指标面板' }
          ]}
        />
      </Layout.Sider>
      <Layout.Content style={{ padding: 24 }}>
        <FlowOverview />
      </Layout.Content>
    </Layout>
  );
}
```

- [ ] **Step 5: Install and build**

Run:

```bash
cd frontend
npm install
npm run build
```

Expected: Vite build succeeds.

- [ ] **Step 6: Commit**

```bash
git add frontend
git commit -m "feat: scaffold observability frontend"
```

### Task 5: Implement Frontend API Client And Overview Page

**Files:**
- Create: `frontend/src/api/client.ts`
- Create: `frontend/src/components/SourceLatencyCard.tsx`
- Create: `frontend/src/components/StageStatusPanel.tsx`
- Create: `frontend/src/pages/FlowOverview.tsx`

- [ ] **Step 1: Create API client**

```ts
import type { MigrationEvent, SinkSummary, SourceSummary, StageStatus } from '../types/observability';

async function getJson<T>(path: string): Promise<T> {
  const response = await fetch(path);
  if (!response.ok) {
    throw new Error(`Request failed: ${response.status} ${path}`);
  }
  return response.json() as Promise<T>;
}

export function fetchStageStatuses() {
  return getJson<StageStatus[]>('/api/flow/status');
}

export function fetchSourceSummaries() {
  return getJson<SourceSummary[]>('/api/flow/sources');
}

export function fetchMigrationEvents() {
  return getJson<MigrationEvent[]>('/api/flow/migrations');
}

export function fetchSinkSummaries() {
  return getJson<SinkSummary[]>('/api/flow/sinks');
}
```

- [ ] **Step 2: Create `SourceLatencyCard`**

```tsx
import { Card, Statistic, Tag, Typography } from 'antd';
import type { SourceSummary } from '../types/observability';

export default function SourceLatencyCard({ source }: { source: SourceSummary }) {
  return (
    <Card size="small" title={source.source.toUpperCase()} extra={<Tag color="green">{source.status}</Tag>}>
      <Statistic title="EPS" value={source.eps} precision={1} />
      <Typography.Text type="secondary">Kafka lag: {source.kafkaLag}</Typography.Text>
      <br />
      <Typography.Text type="secondary">事件延迟: {source.eventDelayMs} ms</Typography.Text>
      <Typography.Paragraph style={{ marginTop: 8, marginBottom: 0 }}>{source.summary}</Typography.Paragraph>
    </Card>
  );
}
```

- [ ] **Step 3: Create `StageStatusPanel`**

```tsx
import { Badge, Table } from 'antd';
import type { ColumnsType } from 'antd/es/table';
import type { StageStatus } from '../types/observability';

const statusMap = {
  healthy: 'success',
  warning: 'warning',
  critical: 'error',
  idle: 'default'
} as const;

const columns: ColumnsType<StageStatus> = [
  {
    title: '阶段',
    dataIndex: 'label'
  },
  {
    title: '状态',
    dataIndex: 'status',
    render: (_, row) => <Badge status={statusMap[row.status]} text={row.status} />
  },
  {
    title: 'In EPS',
    dataIndex: 'inEps',
    render: (value: number) => value.toFixed(1)
  },
  {
    title: 'Out EPS',
    dataIndex: 'outEps',
    render: (value: number) => value.toFixed(1)
  },
  {
    title: 'P95',
    dataIndex: 'latencyP95Ms',
    render: (value: number) => `${value} ms`
  },
  {
    title: '摘要',
    dataIndex: 'summary'
  }
];

export default function StageStatusPanel({ stages }: { stages: StageStatus[] }) {
  return <Table rowKey="stageId" size="small" pagination={false} columns={columns} dataSource={stages} />;
}
```

- [ ] **Step 4: Create `FlowOverview`**

```tsx
import { Alert, Col, Row, Space, Typography } from 'antd';
import { useEffect, useState } from 'react';
import { fetchSourceSummaries, fetchStageStatuses } from '../api/client';
import SourceLatencyCard from '../components/SourceLatencyCard';
import StageStatusPanel from '../components/StageStatusPanel';
import StreamingFlowGraph from '../components/StreamingFlowGraph';
import type { SourceSummary, StageStatus } from '../types/observability';

export default function FlowOverview() {
  const [sources, setSources] = useState<SourceSummary[]>([]);
  const [stages, setStages] = useState<StageStatus[]>([]);
  const [error, setError] = useState<string>();

  useEffect(() => {
    Promise.all([fetchSourceSummaries(), fetchStageStatuses()])
      .then(([nextSources, nextStages]) => {
        setSources(nextSources);
        setStages(nextStages);
      })
      .catch((err: Error) => setError(err.message));
  }, []);

  return (
    <Space direction="vertical" size={16} style={{ width: '100%' }}>
      <Typography.Title level={3} style={{ margin: 0 }}>实时流处理总览</Typography.Title>
      {error ? <Alert type="error" message="观测 API 请求失败" description={error} /> : null}
      <Row gutter={12}>
        {sources.map((source) => (
          <Col span={8} key={source.source}>
            <SourceLatencyCard source={source} />
          </Col>
        ))}
      </Row>
      <StreamingFlowGraph stages={stages} />
      <StageStatusPanel stages={stages} />
    </Space>
  );
}
```

- [ ] **Step 5: Run frontend build**

Run:

```bash
cd frontend
npm run build
```

Expected: build succeeds.

- [ ] **Step 6: Commit**

```bash
git add frontend/src/api frontend/src/components/SourceLatencyCard.tsx frontend/src/components/StageStatusPanel.tsx frontend/src/pages/FlowOverview.tsx
git commit -m "feat: add observability overview data panels"
```

### Task 6: Implement G6 Flow Graph

**Files:**
- Create: `frontend/src/components/StreamingFlowGraph.tsx`

- [ ] **Step 1: Implement graph component**

```tsx
import { Graph } from '@antv/g6';
import { Card } from 'antd';
import { useEffect, useRef } from 'react';
import type { StageStatus } from '../types/observability';

const statusColor = {
  healthy: '#16a34a',
  warning: '#f59e0b',
  critical: '#dc2626',
  idle: '#94a3b8'
};

const edges = [
  ['chr-source', 'kafka'],
  ['mr-source', 'kafka'],
  ['cm-source', 'kafka'],
  ['kafka', 'assigner'],
  ['assigner', 'enrichment'],
  ['assigner', 'load-coordinator'],
  ['load-coordinator', 'assigner'],
  ['enrichment', 'mysql-sink'],
  ['enrichment', 'hive-sink'],
  ['enrichment', 'iceberg-sink']
];

export default function StreamingFlowGraph({ stages }: { stages: StageStatus[] }) {
  const containerRef = useRef<HTMLDivElement>(null);
  const graphRef = useRef<Graph>();

  useEffect(() => {
    if (!containerRef.current || stages.length === 0) {
      return;
    }

    graphRef.current?.destroy();
    const graph = new Graph({
      container: containerRef.current,
      width: containerRef.current.clientWidth,
      height: 360,
      data: {
        nodes: stages.map((stage) => ({
          id: stage.stageId,
          data: stage,
          style: {
            labelText: `${stage.label}\n${stage.outEps.toFixed(0)} EPS\np95 ${stage.latencyP95Ms}ms`,
            fill: '#ffffff',
            stroke: statusColor[stage.status],
            lineWidth: 3,
            radius: 8
          }
        })),
        edges: edges.map(([source, target]) => ({ source, target }))
      },
      layout: {
        type: 'dagre',
        rankdir: 'LR',
        nodesep: 24,
        ranksep: 56
      },
      behaviors: ['drag-canvas', 'zoom-canvas', 'drag-element']
    });
    graph.render();
    graphRef.current = graph;

    return () => graph.destroy();
  }, [stages]);

  return (
    <Card size="small" title="流处理阶段流程图">
      <div ref={containerRef} style={{ width: '100%', height: 360 }} />
    </Card>
  );
}
```

- [ ] **Step 2: Run frontend build**

Run:

```bash
cd frontend
npm run build
```

Expected: build succeeds and TypeScript accepts G6 usage.

- [ ] **Step 3: Commit**

```bash
git add frontend/src/components/StreamingFlowGraph.tsx
git commit -m "feat: render streaming flow graph"
```

### Task 7: Add Migration And Metrics Pages

**Files:**
- Create: `frontend/src/components/MigrationDiffPanel.tsx`
- Create: `frontend/src/components/GrafanaEmbedPanel.tsx`
- Create: `frontend/src/pages/MigrationTimeline.tsx`
- Create: `frontend/src/pages/MetricsDashboard.tsx`
- Modify: `frontend/src/App.tsx`

- [ ] **Step 1: Create migration panel**

```tsx
import { Card, Descriptions, Table, Timeline } from 'antd';
import type { MigrationEvent, MovedVBucket } from '../types/observability';

export default function MigrationDiffPanel({ event }: { event: MigrationEvent }) {
  return (
    <Card title={event.migrationId} size="small">
      <Descriptions size="small" column={2}>
        <Descriptions.Item label="原因">{event.reason}</Descriptions.Item>
        <Descriptions.Item label="状态">{event.status}</Descriptions.Item>
        <Descriptions.Item label="路由版本">
          {event.routingVersionBefore} → {event.routingVersionAfter}
        </Descriptions.Item>
        <Descriptions.Item label="耗时">
          {event.startedAt} / {event.completedAt}
        </Descriptions.Item>
      </Descriptions>
      <Timeline
        style={{ marginTop: 16 }}
        items={[
          { children: '检测到负载不均衡' },
          { children: '生成 vbucket 迁移计划' },
          { children: '应用新路由版本' },
          { children: '迁移后负载稳定' }
        ]}
      />
      <Table<MovedVBucket>
        rowKey="vbucket"
        size="small"
        pagination={false}
        dataSource={event.movedVbuckets}
        columns={[
          { title: 'VBucket', dataIndex: 'vbucket' },
          { title: 'From', dataIndex: 'fromSubtask' },
          { title: 'To', dataIndex: 'toSubtask' },
          { title: '预计 EPS', dataIndex: 'estimatedEps' },
          { title: '热点 Key', dataIndex: 'hotKeys', render: (keys: string[]) => keys.join(', ') }
        ]}
      />
    </Card>
  );
}
```

- [ ] **Step 2: Create migration page**

```tsx
import { Alert, Space, Typography } from 'antd';
import { useEffect, useState } from 'react';
import { fetchMigrationEvents } from '../api/client';
import MigrationDiffPanel from '../components/MigrationDiffPanel';
import type { MigrationEvent } from '../types/observability';

export default function MigrationTimeline() {
  const [events, setEvents] = useState<MigrationEvent[]>([]);
  const [error, setError] = useState<string>();

  useEffect(() => {
    fetchMigrationEvents().then(setEvents).catch((err: Error) => setError(err.message));
  }, []);

  return (
    <Space direction="vertical" size={16} style={{ width: '100%' }}>
      <Typography.Title level={3} style={{ margin: 0 }}>负载迁移时间线</Typography.Title>
      {error ? <Alert type="error" message="迁移事件请求失败" description={error} /> : null}
      {events.map((event) => <MigrationDiffPanel key={event.migrationId} event={event} />)}
    </Space>
  );
}
```

- [ ] **Step 3: Create Grafana panel and metrics page**

```tsx
import { Card } from 'antd';

export default function GrafanaEmbedPanel() {
  const src = import.meta.env.VITE_GRAFANA_URL || 'http://localhost:3000';
  return (
    <Card size="small" title="Grafana 详细指标">
      <iframe
        title="Grafana"
        src={src}
        style={{ width: '100%', height: 720, border: 0 }}
      />
    </Card>
  );
}
```

```tsx
import { Space, Typography } from 'antd';
import GrafanaEmbedPanel from '../components/GrafanaEmbedPanel';

export default function MetricsDashboard() {
  return (
    <Space direction="vertical" size={16} style={{ width: '100%' }}>
      <Typography.Title level={3} style={{ margin: 0 }}>指标面板</Typography.Title>
      <GrafanaEmbedPanel />
    </Space>
  );
}
```

- [ ] **Step 4: Update `App.tsx` with simple route state**

```tsx
import { Layout, Menu } from 'antd';
import { useState } from 'react';
import FlowOverview from './pages/FlowOverview';
import MetricsDashboard from './pages/MetricsDashboard';
import MigrationTimeline from './pages/MigrationTimeline';

export default function App() {
  const [page, setPage] = useState('flow');
  return (
    <Layout style={{ minHeight: '100vh' }}>
      <Layout.Sider theme="light" width={220}>
        <Menu
          mode="inline"
          selectedKeys={[page]}
          onClick={(item) => setPage(item.key)}
          items={[
            { key: 'flow', label: '流处理总览' },
            { key: 'migrations', label: '负载迁移' },
            { key: 'metrics', label: '指标面板' }
          ]}
        />
      </Layout.Sider>
      <Layout.Content style={{ padding: 24 }}>
        {page === 'flow' ? <FlowOverview /> : null}
        {page === 'migrations' ? <MigrationTimeline /> : null}
        {page === 'metrics' ? <MetricsDashboard /> : null}
      </Layout.Content>
    </Layout>
  );
}
```

- [ ] **Step 5: Run frontend build**

Run:

```bash
cd frontend
npm run build
```

Expected: build succeeds.

- [ ] **Step 6: Commit**

```bash
git add frontend/src/components/MigrationDiffPanel.tsx frontend/src/components/GrafanaEmbedPanel.tsx frontend/src/pages/MigrationTimeline.tsx frontend/src/pages/MetricsDashboard.tsx frontend/src/App.tsx
git commit -m "feat: add migration and metrics pages"
```

### Task 8: Add Prometheus And Grafana Config

**Files:**
- Create: `docker/prometheus/prometheus.yml`
- Create: `docker/grafana/provisioning/datasources/prometheus.yml`
- Create: `docker/grafana/provisioning/dashboards/dashboards.yml`
- Create: `docs/grafana/streaming-observability-dashboard.json`

- [ ] **Step 1: Add Prometheus scrape config**

```yaml
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: "observability-api"
    metrics_path: "/metrics"
    static_configs:
      - targets: ["observability-api:18080"]
  - job_name: "flink-jobmanager"
    metrics_path: "/metrics"
    static_configs:
      - targets: ["jobmanager:9249"]
  - job_name: "flink-taskmanager"
    metrics_path: "/metrics"
    static_configs:
      - targets: ["taskmanager:9249"]
```

- [ ] **Step 2: Add Grafana datasource provisioning**

```yaml
apiVersion: 1

datasources:
  - name: Prometheus
    type: prometheus
    access: proxy
    url: http://prometheus:9090
    isDefault: true
```

- [ ] **Step 3: Add Grafana dashboard provisioning**

```yaml
apiVersion: 1

providers:
  - name: fdb
    orgId: 1
    folder: Flink Data Balance
    type: file
    disableDeletion: false
    updateIntervalSeconds: 30
    options:
      path: /var/lib/grafana/dashboards
```

- [ ] **Step 4: Add minimal dashboard JSON**

```json
{
  "uid": "fdb-streaming-observability",
  "title": "Flink Data Balance Streaming Observability",
  "schemaVersion": 39,
  "version": 1,
  "panels": [
    {
      "type": "timeseries",
      "title": "Source EPS",
      "gridPos": { "x": 0, "y": 0, "w": 12, "h": 8 },
      "targets": [
        { "expr": "fdb_source_eps", "legendFormat": "{{source}}" }
      ]
    },
    {
      "type": "timeseries",
      "title": "Sink Write Latency P95",
      "gridPos": { "x": 12, "y": 0, "w": 12, "h": 8 },
      "targets": [
        { "expr": "fdb_sink_write_latency_ms", "legendFormat": "{{sink}} {{window}}" }
      ]
    },
    {
      "type": "timeseries",
      "title": "Imbalance Ratio",
      "gridPos": { "x": 0, "y": 8, "w": 12, "h": 8 },
      "targets": [
        { "expr": "fdb_subtask_imbalance_ratio", "legendFormat": "imbalance" }
      ]
    },
    {
      "type": "timeseries",
      "title": "Rebalance Total",
      "gridPos": { "x": 12, "y": 8, "w": 12, "h": 8 },
      "targets": [
        { "expr": "fdb_rebalance_total", "legendFormat": "rebalance" }
      ]
    }
  ]
}
```

- [ ] **Step 5: Commit**

```bash
git add docker/prometheus docker/grafana docs/grafana/streaming-observability-dashboard.json
git commit -m "feat: add prometheus and grafana observability config"
```

### Task 9: Wire Docker Compose Services

**Files:**
- Modify: `docker/docker-compose.yml`

- [ ] **Step 1: Add services**

Add these services to `docker/docker-compose.yml`:

```yaml
  observability-api:
    image: eclipse-temurin:17-jre
    working_dir: /app
    command: ["java", "-jar", "/app/observability-api.jar"]
    environment:
      FDB_OBSERVABILITY_PORT: "18080"
    ports:
      - "18080:18080"
    volumes:
      - ../observability-api/target/observability-api-0.1.0-SNAPSHOT.jar:/app/observability-api.jar:ro

  frontend:
    image: node:20-alpine
    working_dir: /app
    command: ["sh", "-c", "npm install && npm run dev"]
    environment:
      VITE_GRAFANA_URL: "http://localhost:3000"
    ports:
      - "5173:5173"
    volumes:
      - ../frontend:/app
    depends_on:
      - observability-api

  prometheus:
    image: prom/prometheus:v2.52.0
    ports:
      - "9090:9090"
    volumes:
      - ./prometheus/prometheus.yml:/etc/prometheus/prometheus.yml:ro
    depends_on:
      - observability-api

  grafana:
    image: grafana/grafana:11.0.0
    environment:
      GF_SECURITY_ADMIN_USER: admin
      GF_SECURITY_ADMIN_PASSWORD: admin
      GF_AUTH_ANONYMOUS_ENABLED: "true"
      GF_AUTH_ANONYMOUS_ORG_ROLE: Viewer
      GF_SECURITY_ALLOW_EMBEDDING: "true"
    ports:
      - "3000:3000"
    volumes:
      - ./grafana/provisioning:/etc/grafana/provisioning:ro
      - ../docs/grafana:/var/lib/grafana/dashboards:ro
    depends_on:
      - prometheus
```

- [ ] **Step 2: Validate Compose**

Run:

```bash
docker compose -f docker/docker-compose.yml config
```

Expected: command exits 0 and includes services `frontend`, `observability-api`, `prometheus`, `grafana`.

- [ ] **Step 3: Commit**

```bash
git add docker/docker-compose.yml
git commit -m "feat: wire observability services into compose"
```

### Task 10: Add API Metrics Endpoint

**Files:**
- Modify: `observability-api/src/main/java/com/fdb/observability/ObservabilityApiMain.java`
- Create: `observability-api/src/test/java/com/fdb/observability/PrometheusMetricsTest.java`

- [ ] **Step 1: Write metric format test**

```java
package com.fdb.observability;

import static org.assertj.core.api.Assertions.assertThat;

import com.fdb.observability.service.ObservabilitySnapshotService;
import org.junit.jupiter.api.Test;

class PrometheusMetricsTest {

  @Test
  void rendersSourceAndSinkMetrics() {
    String metrics = ObservabilityApiMain.toPrometheusMetrics(new ObservabilitySnapshotService());
    assertThat(metrics).contains("fdb_source_eps{source=\"chr\"}");
    assertThat(metrics).contains("fdb_sink_write_latency_ms{sink=\"iceberg\",window=\"1m\"}");
    assertThat(metrics).contains("fdb_rebalance_total");
  }
}
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
mvn -pl observability-api test
```

Expected: FAIL because `toPrometheusMetrics` does not exist.

- [ ] **Step 3: Implement metrics renderer and route**

Add a server route:

```java
server.createContext("/metrics", exchange -> writeText(exchange, toPrometheusMetrics(service)));
```

Add these methods:

```java
static String toPrometheusMetrics(ObservabilitySnapshotService service) {
  StringBuilder out = new StringBuilder();
  service.sourceSummaries().forEach(source -> {
    out.append("fdb_source_eps{source=\"").append(source.source()).append("\"} ").append(source.eps()).append('\n');
    out.append("fdb_source_lag_ms{source=\"").append(source.source()).append("\"} ").append(source.eventDelayMs()).append('\n');
  });
  service.stageStatuses().forEach(stage -> {
    out.append("fdb_stage_in_eps{stage=\"").append(stage.stageId()).append("\"} ").append(stage.inEps()).append('\n');
    out.append("fdb_stage_out_eps{stage=\"").append(stage.stageId()).append("\"} ").append(stage.outEps()).append('\n');
    out.append("fdb_stage_latency_ms{stage=\"").append(stage.stageId()).append("\",quantile=\"p95\"} ").append(stage.latencyP95Ms()).append('\n');
    out.append("fdb_stage_watermark_lag_ms{stage=\"").append(stage.stageId()).append("\"} ").append(stage.watermarkLagMs()).append('\n');
  });
  service.sinkSummaries().forEach(sink -> {
    out.append("fdb_sink_write_rows_total{sink=\"").append(sink.sink()).append("\",window=\"").append(sink.window()).append("\"} ").append(sink.rowsWritten()).append('\n');
    out.append("fdb_sink_write_latency_ms{sink=\"").append(sink.sink()).append("\",window=\"").append(sink.window()).append("\"} ").append(sink.writeLatencyP95Ms()).append('\n');
  });
  out.append("fdb_subtask_imbalance_ratio 2.4\n");
  out.append("fdb_rebalance_total ").append(service.migrationEvents().size()).append('\n');
  return out.toString();
}

private static void writeText(HttpExchange exchange, String body) throws IOException {
  byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
  exchange.getResponseHeaders().add("Content-Type", "text/plain; version=0.0.4; charset=utf-8");
  exchange.getResponseHeaders().add("Access-Control-Allow-Origin", "*");
  exchange.sendResponseHeaders(200, bytes.length);
  try (OutputStream output = exchange.getResponseBody()) {
    output.write(bytes);
  }
}
```

- [ ] **Step 4: Run tests**

Run:

```bash
mvn -pl observability-api test
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add observability-api/src/main/java/com/fdb/observability/ObservabilityApiMain.java observability-api/src/test/java/com/fdb/observability/PrometheusMetricsTest.java
git commit -m "feat: expose prometheus metrics from observability api"
```

### Task 11: Documentation And Existing Spec Cross-Link

**Files:**
- Modify: `README.md`
- Modify: `AGENTS.md`
- Modify: `docs/superpowers/specs/2026-04-29-flink-data-balance-design.md`

- [ ] **Step 1: Add README section**

Add:

````markdown
## 实时观测控制台

本项目包含嵌入式前端观测控制台：

- Frontend: http://localhost:5173
- Observability API: http://localhost:18080
- Prometheus: http://localhost:9090
- Grafana: http://localhost:3000

启动前先构建 Java 模块：

```bash
mvn -pl observability-api package
docker compose -f docker/docker-compose.yml up -d observability-api frontend prometheus grafana
```

控制台展示 CHR/MR/CM 数据源延迟、流处理阶段状态、负载迁移事件，以及 MySQL/Hive/Iceberg Sink 写入性能摘要。
````

- [ ] **Step 2: Update AGENTS.md**

Add:

```markdown
### 实时观测控制台

- 前端模块位于 `frontend/`，技术栈为 React 18 + TypeScript + Vite + Ant Design + AntV G6。
- 观测 API 模块位于 `observability-api/`，使用 Java 17 提供 REST、SSE 与 Prometheus `/metrics`。
- Grafana dashboard 位于 `docs/grafana/`，Prometheus/Grafana 配置位于 `docker/prometheus/` 与 `docker/grafana/`。
- 修改控制台时需至少运行 `mvn -pl observability-api test`、`cd frontend && npm run build`、`docker compose -f docker/docker-compose.yml config`。
```

- [ ] **Step 3: Keep the main spec authoritative**

Ensure `docs/superpowers/specs/2026-04-29-flink-data-balance-design.md` contains the full realtime observability console design under `## 12. 可观测性与实时控制台`; do not link to a separate incremental spec.

- [ ] **Step 4: Commit**

```bash
git add README.md AGENTS.md docs/superpowers/specs/2026-04-29-flink-data-balance-design.md
git commit -m "docs: document streaming observability console"
```

### Task 12: Final Verification

**Files:**
- No source edits unless verification exposes a defect.

- [ ] **Step 1: Run Java tests**

Run:

```bash
mvn test
```

Expected: all Java module tests pass.

- [ ] **Step 2: Run frontend build**

Run:

```bash
cd frontend
npm run build
```

Expected: TypeScript and Vite build pass.

- [ ] **Step 3: Validate Docker Compose**

Run:

```bash
docker compose -f docker/docker-compose.yml config
```

Expected: config renders successfully.

- [ ] **Step 4: Smoke test API**

Run:

```bash
mvn -pl observability-api package
java -jar observability-api/target/observability-api-0.1.0-SNAPSHOT.jar
```

In a second terminal:

```bash
curl -fsS http://localhost:18080/api/flow/status
curl -fsS http://localhost:18080/metrics
```

Expected: status JSON includes `iceberg-sink`; metrics include `fdb_source_eps` and `fdb_sink_write_latency_ms`.

- [ ] **Step 5: Commit verification fixes if needed**

If verification required code changes, commit them:

```bash
git add .
git commit -m "fix: stabilize observability console verification"
```

If no changes were needed, do not create an empty commit.

---

## Self-Review

- Spec coverage: The plan covers the embedded frontend module, G6 flow graph, data source latency cards, migration visualization, Grafana embed, Prometheus scrape config, API/SSE surface, Docker Compose integration, and docs cross-link.
- Placeholder scan: 未发现占位式步骤或缺少实际内容的执行项。
- Type consistency: Java record fields match TypeScript interfaces: `stageId`, `source`, `migrationId`, `movedVbuckets`, `rowsWritten`, `writeLatencyP95Ms`.

# Flink Data Balance — Foundation 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 搭建 `flink-data-balance` 工程的基础设施（parent POM、common 模块的 Avro schema 与工具类、本地 Docker 依赖、Kafka topic 与 MySQL DDL 脚本），让后续模块（topology-service / simulator / flink-job）有一个稳固、可验证的起点。

**Architecture:** 单仓库多模块 Maven 工程；`common` 模块汇总跨模块共享的 Avro schema、Kafka serde、Geohash、稳定哈希、三层合并配置加载；本地依赖（Kafka / MySQL / Hive Metastore + Postgres）通过 docker-compose 起；Kafka topic 与 MySQL 表通过 shell/SQL 脚本初始化。

**Tech Stack:** Java 21, Maven 3.9+, Apache Avro 1.11.3, Apache Flink 1.20.0（仅引入 BOM 依赖管理）, Guava, SnakeYAML 2.2, JUnit 5.10.2, AssertJ 3.25.3, Confluent Platform 7.5.0 (Kafka), MySQL 8.0, Apache Hive 4.0.0 (standalone metastore), Postgres 14, Docker Desktop on Windows + Git Bash.

**前置准备：**
- Windows 11 + Git Bash + Docker Desktop（WSL2 backend）已就绪
- 安装 JDK 21（推荐 Temurin/Zulu/Microsoft 21）
- 安装 Maven 3.9+
- 工作目录：`D:/agent-code/flink-data-balance/`（路径全部使用正斜杠）

---

## 任务概览

| # | 任务 | 关键产物 |
|---|---|---|
| 1 | 项目骨架 | `pom.xml` + `common/pom.xml` + `.gitignore` |
| 2 | `ChrEvent.avsc` | CHR 事件 Avro schema |
| 3 | `MrStat.avsc` | 话统 Avro schema |
| 4 | `CmConfig.avsc` | 配置 Avro schema |
| 5 | `AnomalyEvent.avsc` | 异常事件 Avro schema |
| 6 | `CellKpi.avsc` | 小区 KPI Avro schema |
| 7 | `Geohash` 工具 | `Geohash.encode(lat, lon, precision)` |
| 8 | `Hashes` 工具 | `Hashes.toVBucket(siteId, n)` |
| 9 | Kafka Avro serde | `AvroSerde.serializer/deserializer` |
| 10 | 三层 ConfigLoader | 默认 yaml / 文件覆盖 / 环境变量覆盖 |
| 11 | docker-compose | Kafka/MySQL/HMS/Postgres + dev-up/down |
| 12 | Kafka topic 脚本 | 创建 spec 中所有 topic |
| 13 | MySQL DDL | `anomaly_events` / `cell_kpi` |
| 14 | 端到端冒烟 | 启动全套依赖 + `mvn install` 通过 |

---

## Task 1: 项目骨架（parent POM + common 模块 + .gitignore）

**Files:**
- Create: `D:/agent-code/flink-data-balance/pom.xml`
- Create: `D:/agent-code/flink-data-balance/.gitignore`
- Create: `D:/agent-code/flink-data-balance/common/pom.xml`
- Create: `D:/agent-code/flink-data-balance/common/src/main/java/com/fdb/common/.gitkeep`
- Create: `D:/agent-code/flink-data-balance/common/src/main/avro/.gitkeep`
- Create: `D:/agent-code/flink-data-balance/common/src/test/java/com/fdb/common/SmokeTest.java`

- [ ] **Step 1: 写一个会失败的冒烟测试（确认测试基础设施可跑）**

文件：`common/src/test/java/com/fdb/common/SmokeTest.java`

```java
package com.fdb.common;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class SmokeTest {

    @Test
    void java_runtime_is_21_or_higher() {
        int major = Runtime.version().feature();
        assertThat(major).isGreaterThanOrEqualTo(21);
    }
}
```

- [ ] **Step 2: 此时未配置 pom.xml，运行测试应失败**

```bash
cd D:/agent-code/flink-data-balance
mvn -pl common -am test
```

预期：失败，因为 `pom.xml` 不存在或 module 未声明。

- [ ] **Step 3: 写 parent `pom.xml`**

文件：`D:/agent-code/flink-data-balance/pom.xml`

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>com.fdb</groupId>
    <artifactId>flink-data-balance-parent</artifactId>
    <version>0.1.0-SNAPSHOT</version>
    <packaging>pom</packaging>
    <name>flink-data-balance-parent</name>

    <modules>
        <module>common</module>
    </modules>

    <properties>
        <maven.compiler.release>21</maven.compiler.release>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
        <project.reporting.outputEncoding>UTF-8</project.reporting.outputEncoding>

        <flink.version>1.20.0</flink.version>
        <avro.version>1.11.3</avro.version>
        <guava.version>33.0.0-jre</guava.version>
        <snakeyaml.version>2.2</snakeyaml.version>
        <slf4j.version>2.0.13</slf4j.version>
        <logback.version>1.5.6</logback.version>

        <junit.version>5.10.2</junit.version>
        <assertj.version>3.25.3</assertj.version>

        <maven.compiler.plugin.version>3.13.0</maven.compiler.plugin.version>
        <maven.surefire.plugin.version>3.2.5</maven.surefire.plugin.version>
        <avro.plugin.version>1.11.3</avro.plugin.version>
    </properties>

    <dependencyManagement>
        <dependencies>
            <dependency>
                <groupId>org.apache.flink</groupId>
                <artifactId>flink-bom</artifactId>
                <version>${flink.version}</version>
                <type>pom</type>
                <scope>import</scope>
            </dependency>
            <dependency>
                <groupId>org.apache.avro</groupId>
                <artifactId>avro</artifactId>
                <version>${avro.version}</version>
            </dependency>
            <dependency>
                <groupId>com.google.guava</groupId>
                <artifactId>guava</artifactId>
                <version>${guava.version}</version>
            </dependency>
            <dependency>
                <groupId>org.yaml</groupId>
                <artifactId>snakeyaml</artifactId>
                <version>${snakeyaml.version}</version>
            </dependency>
            <dependency>
                <groupId>org.slf4j</groupId>
                <artifactId>slf4j-api</artifactId>
                <version>${slf4j.version}</version>
            </dependency>
            <dependency>
                <groupId>ch.qos.logback</groupId>
                <artifactId>logback-classic</artifactId>
                <version>${logback.version}</version>
            </dependency>

            <dependency>
                <groupId>org.junit.jupiter</groupId>
                <artifactId>junit-jupiter</artifactId>
                <version>${junit.version}</version>
            </dependency>
            <dependency>
                <groupId>org.assertj</groupId>
                <artifactId>assertj-core</artifactId>
                <version>${assertj.version}</version>
            </dependency>
        </dependencies>
    </dependencyManagement>

    <build>
        <pluginManagement>
            <plugins>
                <plugin>
                    <groupId>org.apache.maven.plugins</groupId>
                    <artifactId>maven-compiler-plugin</artifactId>
                    <version>${maven.compiler.plugin.version}</version>
                </plugin>
                <plugin>
                    <groupId>org.apache.maven.plugins</groupId>
                    <artifactId>maven-surefire-plugin</artifactId>
                    <version>${maven.surefire.plugin.version}</version>
                </plugin>
                <plugin>
                    <groupId>org.apache.avro</groupId>
                    <artifactId>avro-maven-plugin</artifactId>
                    <version>${avro.plugin.version}</version>
                </plugin>
            </plugins>
        </pluginManagement>
    </build>
</project>
```

- [ ] **Step 4: 写 `common/pom.xml`**

文件：`D:/agent-code/flink-data-balance/common/pom.xml`

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

    <artifactId>common</artifactId>
    <name>flink-data-balance-common</name>

    <dependencies>
        <dependency>
            <groupId>org.apache.avro</groupId>
            <artifactId>avro</artifactId>
        </dependency>
        <dependency>
            <groupId>com.google.guava</groupId>
            <artifactId>guava</artifactId>
        </dependency>
        <dependency>
            <groupId>org.yaml</groupId>
            <artifactId>snakeyaml</artifactId>
        </dependency>
        <dependency>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-api</artifactId>
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
        <dependency>
            <groupId>ch.qos.logback</groupId>
            <artifactId>logback-classic</artifactId>
            <scope>test</scope>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.apache.avro</groupId>
                <artifactId>avro-maven-plugin</artifactId>
                <executions>
                    <execution>
                        <id>schemas</id>
                        <phase>generate-sources</phase>
                        <goals>
                            <goal>schema</goal>
                        </goals>
                        <configuration>
                            <sourceDirectory>${project.basedir}/src/main/avro/</sourceDirectory>
                            <outputDirectory>${project.basedir}/target/generated-sources/avro/</outputDirectory>
                            <stringType>String</stringType>
                            <createSetters>false</createSetters>
                            <enableDecimalLogicalType>false</enableDecimalLogicalType>
                        </configuration>
                    </execution>
                </executions>
            </plugin>
        </plugins>
    </build>
</project>
```

- [ ] **Step 5: 写 `.gitignore` 与占位文件**

文件：`D:/agent-code/flink-data-balance/.gitignore`

```
# Maven
target/
*.iml

# IDE
.idea/
.vscode/
*.swp

# Logs
*.log
logs/

# OS
Thumbs.db
.DS_Store

# Local docker data
docker/data/

# Local config overrides
*.local.yaml
```

文件：`D:/agent-code/flink-data-balance/common/src/main/avro/.gitkeep` （空文件）
文件：`D:/agent-code/flink-data-balance/common/src/main/java/com/fdb/common/.gitkeep` （空文件）

- [ ] **Step 6: 运行测试，确认通过**

```bash
cd D:/agent-code/flink-data-balance
mvn -pl common -am test
```

预期：`SmokeTest.java_runtime_is_21_or_higher` PASS。`BUILD SUCCESS`。

- [ ] **Step 7: 提交**

```bash
cd D:/agent-code/flink-data-balance
git init
git add pom.xml .gitignore common/pom.xml common/src
git commit -m "chore: bootstrap parent POM and common module skeleton"
```

---

## Task 2: ChrEvent Avro Schema

**Files:**
- Create: `D:/agent-code/flink-data-balance/common/src/main/avro/ChrEvent.avsc`
- Create: `D:/agent-code/flink-data-balance/common/src/test/java/com/fdb/common/avro/ChrEventSchemaTest.java`

- [ ] **Step 1: 写失败测试（构造 ChrEvent → 二进制序列化 → 反序列化 → equals）**

文件：`common/src/test/java/com/fdb/common/avro/ChrEventSchemaTest.java`

```java
package com.fdb.common.avro;

import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import static org.assertj.core.api.Assertions.assertThat;

class ChrEventSchemaTest {

    @Test
    void roundtrip_chr_event_preserves_all_fields() throws Exception {
        ChrEvent original = ChrEvent.newBuilder()
            .setChrId("chr-1")
            .setEventTs(1714387200000L)
            .setImsi("460001234567890")
            .setSiteId("SITE-001")
            .setCellId("CELL-001-1")
            .setEventType(ChrEventType.DATA_SESSION)
            .setRatType(RatType.NR_SA)
            .setPci(123)
            .setTac(40001)
            .setEci(1234567890L)
            .setMcc("460")
            .setMnc("00")
            .setResultCode(0)
            .setLatitude(39.9042)
            .setLongitude(116.4074)
            .setRsrp(-95.5f)
            .setSinr(12.0f)
            .build();

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DatumWriter<ChrEvent> writer = new SpecificDatumWriter<>(ChrEvent.class);
        var encoder = EncoderFactory.get().binaryEncoder(out, null);
        writer.write(original, encoder);
        encoder.flush();

        DatumReader<ChrEvent> reader = new SpecificDatumReader<>(ChrEvent.class);
        ChrEvent decoded = reader.read(null,
            DecoderFactory.get().binaryDecoder(new ByteArrayInputStream(out.toByteArray()), null));

        assertThat(decoded).isEqualTo(original);
        assertThat(decoded.getEventType()).isEqualTo(ChrEventType.DATA_SESSION);
        assertThat(decoded.getRatType()).isEqualTo(RatType.NR_SA);
        assertThat(decoded.getRsrp()).isEqualTo(-95.5f);
    }
}
```

- [ ] **Step 2: 运行测试，确认失败（ChrEvent / 枚举类未生成）**

```bash
cd D:/agent-code/flink-data-balance
mvn -pl common -am test -Dtest=ChrEventSchemaTest
```

预期：编译错误，`ChrEvent`、`ChrEventType`、`RatType` 不存在。

- [ ] **Step 3: 写 `ChrEvent.avsc`**

文件：`common/src/main/avro/ChrEvent.avsc`

```json
{
  "namespace": "com.fdb.common.avro",
  "type": "record",
  "name": "ChrEvent",
  "doc": "用户级 CHR 事件主流",
  "fields": [
    { "name": "chrId",         "type": "string", "doc": "事件 UUID" },
    { "name": "eventTs",       "type": "long",   "doc": "事件时间, epoch ms" },
    { "name": "imsi",          "type": "string" },
    { "name": "imei",          "type": ["null", "string"], "default": null },
    { "name": "siteId",        "type": "string" },
    { "name": "cellId",        "type": "string" },
    { "name": "eventType",     "type": { "type": "enum", "name": "ChrEventType",
       "symbols": ["ATTACH","DETACH","HANDOVER","DATA_SESSION","VOICE_CALL","RRC_SETUP_FAIL","SERVICE_REQUEST","PAGING"] } },
    { "name": "ratType",       "type": { "type": "enum", "name": "RatType",
       "symbols": ["LTE","NR_NSA","NR_SA","ENDC"] } },
    { "name": "pci",           "type": "int" },
    { "name": "qci",           "type": ["null", "int"], "default": null },
    { "name": "tac",           "type": "int" },
    { "name": "eci",           "type": "long" },
    { "name": "mcc",           "type": "string" },
    { "name": "mnc",           "type": "string" },
    { "name": "arfcn",         "type": ["null", "int"], "default": null },
    { "name": "nssaiSst",      "type": ["null", "int"], "default": null },
    { "name": "nssaiSd",       "type": ["null", "string"], "default": null },
    { "name": "bearerType",    "type": ["null", { "type": "enum", "name": "BearerType",
       "symbols": ["DEFAULT","DEDICATED"] }], "default": null },
    { "name": "durationMs",    "type": ["null", "long"],  "default": null },
    { "name": "bytesUp",       "type": ["null", "long"],  "default": null },
    { "name": "bytesDown",     "type": ["null", "long"],  "default": null },
    { "name": "latencyMs",     "type": ["null", "float"], "default": null },
    { "name": "rsrp",          "type": ["null", "float"], "default": null },
    { "name": "rsrq",          "type": ["null", "float"], "default": null },
    { "name": "sinr",          "type": ["null", "float"], "default": null },
    { "name": "cqi",           "type": ["null", "int"],   "default": null },
    { "name": "mcs",           "type": ["null", "int"],   "default": null },
    { "name": "bler",          "type": ["null", "float"], "default": null },
    { "name": "timingAdvance", "type": ["null", "int"],   "default": null },
    { "name": "resultCode",    "type": "int", "doc": "0=成功，非零=失败原因" },
    { "name": "latitude",      "type": "double" },
    { "name": "longitude",     "type": "double" },
    { "name": "gridId",        "type": ["null", "string"], "default": null }
  ]
}
```

- [ ] **Step 4: 运行测试，确认通过**

```bash
cd D:/agent-code/flink-data-balance
mvn -pl common -am test -Dtest=ChrEventSchemaTest
```

预期：`ChrEventSchemaTest.roundtrip_chr_event_preserves_all_fields` PASS。
若 IDE 报红，运行 `mvn -pl common generate-sources` 让 Avro 插件生成 `target/generated-sources/avro/com/fdb/common/avro/*.java`。

- [ ] **Step 5: 提交**

```bash
cd D:/agent-code/flink-data-balance
git add common/src/main/avro/ChrEvent.avsc common/src/test/java/com/fdb/common/avro/ChrEventSchemaTest.java
git commit -m "feat(common): add ChrEvent Avro schema with roundtrip test"
```

---

## Task 3: MrStat Avro Schema

**Files:**
- Create: `common/src/main/avro/MrStat.avsc`
- Create: `common/src/test/java/com/fdb/common/avro/MrStatSchemaTest.java`

- [ ] **Step 1: 写失败测试**

文件：`common/src/test/java/com/fdb/common/avro/MrStatSchemaTest.java`

```java
package com.fdb.common.avro;

import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import static org.assertj.core.api.Assertions.assertThat;

class MrStatSchemaTest {

    @Test
    void roundtrip_mr_stat_preserves_fields() throws Exception {
        MrStat original = MrStat.newBuilder()
            .setSiteId("SITE-002")
            .setCellId("CELL-002-3")
            .setWindowStartTs(1714387200000L)
            .setWindowEndTs(1714387210000L)
            .setPrbUsageDl(0.78f)
            .setPrbUsageUl(0.42f)
            .setActiveUsers(150)
            .setAvgRsrp(-92.0f)
            .setAvgRsrq(-10.5f)
            .setAvgSinr(8.5f)
            .setAvgCqi(11.0f)
            .setAvgMcs(20.0f)
            .setAvgBler(0.05f)
            .setThroughputDlMbps(123.5f)
            .setThroughputUlMbps(34.2f)
            .setDroppedConnections(2)
            .setHandoverSuccess(45)
            .setHandoverFailure(3)
            .setPrachAttempt(120)
            .setPrachFailure(5)
            .setRrcEstabAttempt(80)
            .setRrcEstabSuccess(78)
            .setAvgLatencyMs(12.0f)
            .setPacketLossRate(0.001f)
            .build();

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        var encoder = EncoderFactory.get().binaryEncoder(out, null);
        new SpecificDatumWriter<>(MrStat.class).write(original, encoder);
        encoder.flush();

        MrStat decoded = new SpecificDatumReader<>(MrStat.class).read(null,
            DecoderFactory.get().binaryDecoder(new ByteArrayInputStream(out.toByteArray()), null));

        assertThat(decoded).isEqualTo(original);
        assertThat(decoded.getPrbUsageDl()).isEqualTo(0.78f);
        assertThat(decoded.getActiveUsers()).isEqualTo(150);
    }
}
```

- [ ] **Step 2: 运行测试，确认失败**

```bash
mvn -pl common -am test -Dtest=MrStatSchemaTest
```

预期：编译错误，`MrStat` 类不存在。

- [ ] **Step 3: 写 `MrStat.avsc`**

文件：`common/src/main/avro/MrStat.avsc`

```json
{
  "namespace": "com.fdb.common.avro",
  "type": "record",
  "name": "MrStat",
  "doc": "话统记录, 每小区每 10 秒一条",
  "fields": [
    { "name": "siteId",              "type": "string" },
    { "name": "cellId",              "type": "string" },
    { "name": "windowStartTs",       "type": "long" },
    { "name": "windowEndTs",         "type": "long" },
    { "name": "prbUsageDl",          "type": "float" },
    { "name": "prbUsageUl",          "type": "float" },
    { "name": "activeUsers",         "type": "int" },
    { "name": "avgRsrp",             "type": "float" },
    { "name": "avgRsrq",             "type": "float" },
    { "name": "avgSinr",             "type": "float" },
    { "name": "avgCqi",              "type": "float" },
    { "name": "avgMcs",              "type": "float" },
    { "name": "avgBler",             "type": "float" },
    { "name": "throughputDlMbps",    "type": "float" },
    { "name": "throughputUlMbps",    "type": "float" },
    { "name": "droppedConnections",  "type": "int" },
    { "name": "handoverSuccess",     "type": "int" },
    { "name": "handoverFailure",     "type": "int" },
    { "name": "prachAttempt",        "type": "int" },
    { "name": "prachFailure",        "type": "int" },
    { "name": "rrcEstabAttempt",     "type": "int" },
    { "name": "rrcEstabSuccess",     "type": "int" },
    { "name": "avgLatencyMs",        "type": "float" },
    { "name": "packetLossRate",      "type": "float" },
    { "name": "numerology",          "type": ["null", "int"], "default": null }
  ]
}
```

- [ ] **Step 4: 运行测试，确认通过**

```bash
mvn -pl common -am test -Dtest=MrStatSchemaTest
```

预期：PASS。

- [ ] **Step 5: 提交**

```bash
git add common/src/main/avro/MrStat.avsc common/src/test/java/com/fdb/common/avro/MrStatSchemaTest.java
git commit -m "feat(common): add MrStat Avro schema with roundtrip test"
```

---

## Task 4: CmConfig Avro Schema

**Files:**
- Create: `common/src/main/avro/CmConfig.avsc`
- Create: `common/src/test/java/com/fdb/common/avro/CmConfigSchemaTest.java`

- [ ] **Step 1: 写失败测试**

文件：`common/src/test/java/com/fdb/common/avro/CmConfigSchemaTest.java`

```java
package com.fdb.common.avro;

import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class CmConfigSchemaTest {

    @Test
    void roundtrip_cm_config_preserves_nested_nssai_and_neighbors() throws Exception {
        CmConfig original = CmConfig.newBuilder()
            .setSiteId("SITE-003")
            .setCellId("CELL-003-1")
            .setEffectiveTs(1714387200000L)
            .setVersion(7L)
            .setCellType(CellType.NR_SA)
            .setBandwidthMhz(100)
            .setFrequencyBand("n78")
            .setArfcn(632448)
            .setMaxPowerDbm(49.0f)
            .setAzimuth(120)
            .setCenterLat(39.9100)
            .setCenterLon(116.4100)
            .setCoverageRadiusM(800)
            .setPci(456)
            .setTac(40002)
            .setEci(2345678901L)
            .setMcc("460")
            .setMnc("00")
            .setNumerology(1)
            .setMimoMode(MimoMode.MIMO_4x4)
            .setAntennaPorts(4)
            .setNssai(List.of(NssaiEntry.newBuilder().setSst(1).setSd("000001").build()))
            .setNeighborCells(List.of("CELL-003-2", "CELL-004-1"))
            .setTombstone(false)
            .build();

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        var encoder = EncoderFactory.get().binaryEncoder(out, null);
        new SpecificDatumWriter<>(CmConfig.class).write(original, encoder);
        encoder.flush();

        CmConfig decoded = new SpecificDatumReader<>(CmConfig.class).read(null,
            DecoderFactory.get().binaryDecoder(new ByteArrayInputStream(out.toByteArray()), null));

        assertThat(decoded).isEqualTo(original);
        assertThat(decoded.getNssai()).hasSize(1);
        assertThat(decoded.getNssai().get(0).getSst()).isEqualTo(1);
        assertThat(decoded.getNeighborCells()).containsExactly("CELL-003-2", "CELL-004-1");
        assertThat(decoded.getTombstone()).isFalse();
    }
}
```

- [ ] **Step 2: 运行测试，确认失败**

```bash
mvn -pl common -am test -Dtest=CmConfigSchemaTest
```

预期：编译错误。

- [ ] **Step 3: 写 `CmConfig.avsc`**

文件：`common/src/main/avro/CmConfig.avsc`

```json
{
  "namespace": "com.fdb.common.avro",
  "type": "record",
  "name": "CmConfig",
  "doc": "小区配置数据, 按需更新, compact topic",
  "fields": [
    { "name": "siteId",                "type": "string" },
    { "name": "cellId",                "type": "string" },
    { "name": "effectiveTs",           "type": "long" },
    { "name": "version",               "type": "long",  "doc": "单调递增" },
    { "name": "cellType",              "type": { "type": "enum", "name": "CellType",
       "symbols": ["LTE","NR_NSA","NR_SA"] } },
    { "name": "bandwidthMhz",          "type": "int" },
    { "name": "frequencyBand",         "type": "string" },
    { "name": "arfcn",                 "type": "int" },
    { "name": "maxPowerDbm",           "type": "float" },
    { "name": "azimuth",               "type": "int", "doc": "0-359 度" },
    { "name": "centerLat",             "type": "double" },
    { "name": "centerLon",             "type": "double" },
    { "name": "coverageRadiusM",       "type": "int" },
    { "name": "pci",                   "type": "int" },
    { "name": "tac",                   "type": "int" },
    { "name": "eci",                   "type": "long" },
    { "name": "mcc",                   "type": "string" },
    { "name": "mnc",                   "type": "string" },
    { "name": "numerology",            "type": ["null", "int"], "default": null },
    { "name": "mimoMode",              "type": ["null", { "type": "enum", "name": "MimoMode",
       "symbols": ["SISO","MIMO_2x2","MIMO_4x4","MIMO_8x8"] }], "default": null },
    { "name": "antennaPorts",          "type": "int" },
    { "name": "nssai", "type": { "type": "array", "items": {
        "type": "record", "name": "NssaiEntry",
        "fields": [
          { "name": "sst", "type": "int" },
          { "name": "sd",  "type": "string" }
        ] } } },
    { "name": "tddSubFrameAssignment", "type": ["null", "int"],   "default": null },
    { "name": "referenceSignalPower",  "type": ["null", "float"], "default": null },
    { "name": "neighborCells",         "type": { "type": "array", "items": "string" } },
    { "name": "tombstone",             "type": "boolean", "default": false }
  ]
}
```

- [ ] **Step 4: 运行测试，确认通过**

```bash
mvn -pl common -am test -Dtest=CmConfigSchemaTest
```

预期：PASS。

- [ ] **Step 5: 提交**

```bash
git add common/src/main/avro/CmConfig.avsc common/src/test/java/com/fdb/common/avro/CmConfigSchemaTest.java
git commit -m "feat(common): add CmConfig Avro schema with NSSAI and neighbors"
```

---

## Task 5: AnomalyEvent Avro Schema

**Files:**
- Create: `common/src/main/avro/AnomalyEvent.avsc`
- Create: `common/src/test/java/com/fdb/common/avro/AnomalyEventSchemaTest.java`

- [ ] **Step 1: 写失败测试**

文件：`common/src/test/java/com/fdb/common/avro/AnomalyEventSchemaTest.java`

```java
package com.fdb.common.avro;

import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import static org.assertj.core.api.Assertions.assertThat;

class AnomalyEventSchemaTest {

    @Test
    void roundtrip_anomaly_event() throws Exception {
        AnomalyEvent original = AnomalyEvent.newBuilder()
            .setDetectionTs(1714387210000L)
            .setEventTs(1714387200000L)
            .setImsi("460001234567890")
            .setSiteId("SITE-001")
            .setCellId("CELL-001-1")
            .setGridId("wx4g0ec")
            .setLatitude(39.9042)
            .setLongitude(116.4074)
            .setAnomalyType(AnomalyType.LOW_SIGNAL)
            .setSeverity(Severity.LOW)
            .setRuleVersion("v1.0")
            .setContextJson("{\"rsrp\":-115}")
            .build();

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        var encoder = EncoderFactory.get().binaryEncoder(out, null);
        new SpecificDatumWriter<>(AnomalyEvent.class).write(original, encoder);
        encoder.flush();

        AnomalyEvent decoded = new SpecificDatumReader<>(AnomalyEvent.class).read(null,
            DecoderFactory.get().binaryDecoder(new ByteArrayInputStream(out.toByteArray()), null));

        assertThat(decoded).isEqualTo(original);
        assertThat(decoded.getAnomalyType()).isEqualTo(AnomalyType.LOW_SIGNAL);
        assertThat(decoded.getSeverity()).isEqualTo(Severity.LOW);
    }
}
```

- [ ] **Step 2: 运行测试，确认失败**

```bash
mvn -pl common -am test -Dtest=AnomalyEventSchemaTest
```

预期：编译错误。

- [ ] **Step 3: 写 `AnomalyEvent.avsc`**

文件：`common/src/main/avro/AnomalyEvent.avsc`

```json
{
  "namespace": "com.fdb.common.avro",
  "type": "record",
  "name": "AnomalyEvent",
  "doc": "用户级异常事件输出",
  "fields": [
    { "name": "detectionTs",  "type": "long" },
    { "name": "eventTs",      "type": "long" },
    { "name": "imsi",         "type": "string" },
    { "name": "siteId",       "type": "string" },
    { "name": "cellId",       "type": "string" },
    { "name": "gridId",       "type": "string" },
    { "name": "latitude",     "type": "double" },
    { "name": "longitude",    "type": "double" },
    { "name": "anomalyType",  "type": { "type": "enum", "name": "AnomalyType",
       "symbols": ["LOW_SIGNAL","ATTACH_FAILURE_BURST","HANDOVER_FAIL_PATTERN",
                   "CONFIG_MISMATCH","COVERAGE_HOLE"] } },
    { "name": "severity",     "type": { "type": "enum", "name": "Severity",
       "symbols": ["LOW","MEDIUM","HIGH"] } },
    { "name": "ruleVersion",  "type": "string" },
    { "name": "contextJson",  "type": "string" }
  ]
}
```

- [ ] **Step 4: 运行测试，确认通过**

```bash
mvn -pl common -am test -Dtest=AnomalyEventSchemaTest
```

预期：PASS。

- [ ] **Step 5: 提交**

```bash
git add common/src/main/avro/AnomalyEvent.avsc common/src/test/java/com/fdb/common/avro/AnomalyEventSchemaTest.java
git commit -m "feat(common): add AnomalyEvent Avro schema"
```

---

## Task 6: CellKpi Avro Schema

**Files:**
- Create: `common/src/main/avro/CellKpi.avsc`
- Create: `common/src/test/java/com/fdb/common/avro/CellKpiSchemaTest.java`

- [ ] **Step 1: 写失败测试**

文件：`common/src/test/java/com/fdb/common/avro/CellKpiSchemaTest.java`

```java
package com.fdb.common.avro;

import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import static org.assertj.core.api.Assertions.assertThat;

class CellKpiSchemaTest {

    @Test
    void roundtrip_cell_kpi_preserves_window_kind() throws Exception {
        CellKpi original = CellKpi.newBuilder()
            .setWindowStartTs(1714387200000L)
            .setWindowEndTs(1714387260000L)
            .setWindowKind(WindowKind.MIN_1)
            .setSiteId("SITE-001")
            .setCellId("CELL-001-1")
            .setGridId("wx4g0ec")
            .setNumChrEvents(1234L)
            .setNumUsers(456L)
            .setAvgRsrp(-93.5f)
            .setAvgSinr(10.2f)
            .setAvgPrbUsageDl(0.65f)
            .setThroughputDlMbpsAvg(120.0f)
            .setDropRate(0.02f)
            .setHoSuccessRate(0.97f)
            .setAttachSuccessRate(0.99f)
            .build();

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        var encoder = EncoderFactory.get().binaryEncoder(out, null);
        new SpecificDatumWriter<>(CellKpi.class).write(original, encoder);
        encoder.flush();

        CellKpi decoded = new SpecificDatumReader<>(CellKpi.class).read(null,
            DecoderFactory.get().binaryDecoder(new ByteArrayInputStream(out.toByteArray()), null));

        assertThat(decoded).isEqualTo(original);
        assertThat(decoded.getWindowKind()).isEqualTo(WindowKind.MIN_1);
        assertThat(decoded.getNumChrEvents()).isEqualTo(1234L);
    }
}
```

- [ ] **Step 2: 运行测试，确认失败**

```bash
mvn -pl common -am test -Dtest=CellKpiSchemaTest
```

预期：编译错误。

- [ ] **Step 3: 写 `CellKpi.avsc`**

文件：`common/src/main/avro/CellKpi.avsc`

```json
{
  "namespace": "com.fdb.common.avro",
  "type": "record",
  "name": "CellKpi",
  "doc": "小区级 KPI 输出, 多窗口参数化",
  "fields": [
    { "name": "windowStartTs",          "type": "long" },
    { "name": "windowEndTs",            "type": "long" },
    { "name": "windowKind",             "type": { "type": "enum", "name": "WindowKind",
       "symbols": ["MIN_1","MIN_5","MIN_15","HOUR_1"] } },
    { "name": "siteId",                 "type": "string" },
    { "name": "cellId",                 "type": "string" },
    { "name": "gridId",                 "type": "string" },
    { "name": "numChrEvents",           "type": "long" },
    { "name": "numUsers",               "type": "long",  "doc": "HLL 近似" },
    { "name": "avgRsrp",                "type": "float" },
    { "name": "avgSinr",                "type": "float" },
    { "name": "avgPrbUsageDl",          "type": "float" },
    { "name": "throughputDlMbpsAvg",    "type": "float" },
    { "name": "dropRate",               "type": "float" },
    { "name": "hoSuccessRate",          "type": "float" },
    { "name": "attachSuccessRate",      "type": "float" }
  ]
}
```

- [ ] **Step 4: 运行测试，确认通过**

```bash
mvn -pl common -am test -Dtest=CellKpiSchemaTest
```

预期：PASS。

- [ ] **Step 5: 提交**

```bash
git add common/src/main/avro/CellKpi.avsc common/src/test/java/com/fdb/common/avro/CellKpiSchemaTest.java
git commit -m "feat(common): add CellKpi Avro schema"
```

---

## Task 7: Geohash 工具

> 说明：实现简化版的 base32 Geohash encode (Niemeyer 算法)，足够用于派生 `gridId`。算法参考 https://en.wikipedia.org/wiki/Geohash 的标准位交错 + base32 编码。

**Files:**
- Create: `common/src/main/java/com/fdb/common/geo/Geohash.java`
- Create: `common/src/test/java/com/fdb/common/geo/GeohashTest.java`

- [ ] **Step 1: 写失败测试**

文件：`common/src/test/java/com/fdb/common/geo/GeohashTest.java`

```java
package com.fdb.common.geo;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class GeohashTest {

    @Test
    void encode_returns_string_of_requested_length() {
        String hash = Geohash.encode(39.9042, 116.4074, 7);
        assertThat(hash).hasSize(7);
    }

    @Test
    void encode_is_deterministic() {
        String a = Geohash.encode(39.9042, 116.4074, 7);
        String b = Geohash.encode(39.9042, 116.4074, 7);
        assertThat(a).isEqualTo(b);
    }

    @Test
    void encode_known_value_for_origin() {
        // 经典参考: (0, 0) at length 5 -> "s0000"
        assertThat(Geohash.encode(0.0, 0.0, 5)).isEqualTo("s0000");
    }

    @Test
    void encode_known_value_for_san_francisco() {
        // (37.7749, -122.4194) at length 5 -> "9q8yy"
        assertThat(Geohash.encode(37.7749, -122.4194, 5)).isEqualTo("9q8yy");
    }

    @Test
    void encode_rejects_invalid_precision() {
        assertThatThrownBy(() -> Geohash.encode(0.0, 0.0, 0))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Geohash.encode(0.0, 0.0, 13))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void encode_rejects_invalid_lat_lon() {
        assertThatThrownBy(() -> Geohash.encode(91.0, 0.0, 7))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Geohash.encode(0.0, 181.0, 7))
            .isInstanceOf(IllegalArgumentException.class);
    }
}
```

- [ ] **Step 2: 运行测试，确认失败**

```bash
mvn -pl common -am test -Dtest=GeohashTest
```

预期：编译错误，`Geohash` 类不存在。

- [ ] **Step 3: 写 `Geohash.java`**

文件：`common/src/main/java/com/fdb/common/geo/Geohash.java`

```java
package com.fdb.common.geo;

public final class Geohash {

    private static final char[] BASE32 = "0123456789bcdefghjkmnpqrstuvwxyz".toCharArray();

    private Geohash() {}

    public static String encode(double lat, double lon, int precision) {
        if (precision < 1 || precision > 12) {
            throw new IllegalArgumentException("precision must be in [1, 12], got " + precision);
        }
        if (lat < -90.0 || lat > 90.0) {
            throw new IllegalArgumentException("lat out of range: " + lat);
        }
        if (lon < -180.0 || lon > 180.0) {
            throw new IllegalArgumentException("lon out of range: " + lon);
        }

        double latLo = -90.0, latHi = 90.0;
        double lonLo = -180.0, lonHi = 180.0;

        StringBuilder sb = new StringBuilder(precision);
        boolean evenBit = true;       // even bits encode longitude
        int bit = 0;
        int ch = 0;

        while (sb.length() < precision) {
            if (evenBit) {
                double mid = (lonLo + lonHi) / 2.0;
                if (lon >= mid) {
                    ch = (ch << 1) | 1;
                    lonLo = mid;
                } else {
                    ch = ch << 1;
                    lonHi = mid;
                }
            } else {
                double mid = (latLo + latHi) / 2.0;
                if (lat >= mid) {
                    ch = (ch << 1) | 1;
                    latLo = mid;
                } else {
                    ch = ch << 1;
                    latHi = mid;
                }
            }
            evenBit = !evenBit;
            bit++;
            if (bit == 5) {
                sb.append(BASE32[ch]);
                bit = 0;
                ch = 0;
            }
        }
        return sb.toString();
    }
}
```

- [ ] **Step 4: 运行测试，确认通过**

```bash
mvn -pl common -am test -Dtest=GeohashTest
```

预期：所有 6 个测试 PASS。

- [ ] **Step 5: 提交**

```bash
git add common/src/main/java/com/fdb/common/geo/Geohash.java common/src/test/java/com/fdb/common/geo/GeohashTest.java
git commit -m "feat(common): add Geohash encoder utility"
```

---

## Task 8: Hashes 工具（含 toVBucket）

**Files:**
- Create: `common/src/main/java/com/fdb/common/hash/Hashes.java`
- Create: `common/src/test/java/com/fdb/common/hash/HashesTest.java`

- [ ] **Step 1: 写失败测试**

文件：`common/src/test/java/com/fdb/common/hash/HashesTest.java`

```java
package com.fdb.common.hash;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class HashesTest {

    @Test
    void to_vbucket_is_deterministic() {
        int a = Hashes.toVBucket("SITE-000123", 1024);
        int b = Hashes.toVBucket("SITE-000123", 1024);
        assertThat(a).isEqualTo(b);
    }

    @Test
    void to_vbucket_is_in_range() {
        for (int i = 0; i < 5000; i++) {
            int v = Hashes.toVBucket("SITE-" + i, 1024);
            assertThat(v).isBetween(0, 1023);
        }
    }

    @Test
    void to_vbucket_distributes_across_buckets() {
        Map<Integer, Integer> counts = new HashMap<>();
        for (int i = 0; i < 10_000; i++) {
            int v = Hashes.toVBucket("SITE-" + i, 1024);
            counts.merge(v, 1, Integer::sum);
        }
        // 期望覆盖度: 至少 80% 的 bucket 被命中
        assertThat(counts.size()).isGreaterThan(820);
    }

    @Test
    void to_vbucket_rejects_non_positive_n() {
        assertThatThrownBy(() -> Hashes.toVBucket("x", 0))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Hashes.toVBucket("x", -1))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void to_vbucket_with_shift_xor() {
        int base = Hashes.toVBucket("SITE-001", 1024);
        int shifted = Hashes.toVBucketWithShift("SITE-001", 1024, 17);
        assertThat(shifted).isEqualTo((base ^ 17) & 1023);
    }
}
```

- [ ] **Step 2: 运行测试，确认失败**

```bash
mvn -pl common -am test -Dtest=HashesTest
```

预期：编译错误，`Hashes` 类不存在。

- [ ] **Step 3: 写 `Hashes.java`**

文件：`common/src/main/java/com/fdb/common/hash/Hashes.java`

```java
package com.fdb.common.hash;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import java.nio.charset.StandardCharsets;

public final class Hashes {

    private static final HashFunction MURMUR3_128 = Hashing.murmur3_128(0xC0FFEE);

    private Hashes() {}

    public static long murmur3(String key) {
        return MURMUR3_128.hashString(key, StandardCharsets.UTF_8).asLong();
    }

    public static int toVBucket(String key, int numVBuckets) {
        if (numVBuckets <= 0) {
            throw new IllegalArgumentException("numVBuckets must be positive, got " + numVBuckets);
        }
        long h = murmur3(key);
        int v = (int) Math.floorMod(h, numVBuckets);
        return v;
    }

    public static int toVBucketWithShift(String key, int numVBuckets, int slotShift) {
        if (numVBuckets <= 0) {
            throw new IllegalArgumentException("numVBuckets must be positive, got " + numVBuckets);
        }
        if (Integer.bitCount(numVBuckets) != 1) {
            throw new IllegalArgumentException("numVBuckets must be a power of 2 for XOR shift, got " + numVBuckets);
        }
        int base = toVBucket(key, numVBuckets);
        return (base ^ slotShift) & (numVBuckets - 1);
    }
}
```

- [ ] **Step 4: 运行测试，确认通过**

```bash
mvn -pl common -am test -Dtest=HashesTest
```

预期：所有 5 个测试 PASS。

- [ ] **Step 5: 提交**

```bash
git add common/src/main/java/com/fdb/common/hash/Hashes.java common/src/test/java/com/fdb/common/hash/HashesTest.java
git commit -m "feat(common): add Hashes util with toVBucket and slot-shift"
```

---

## Task 9: Kafka Avro Serde

**Files:**
- Create: `common/src/main/java/com/fdb/common/kafka/AvroSerde.java`
- Create: `common/src/test/java/com/fdb/common/kafka/AvroSerdeTest.java`

- [ ] **Step 1: 写失败测试**

文件：`common/src/test/java/com/fdb/common/kafka/AvroSerdeTest.java`

```java
package com.fdb.common.kafka;

import com.fdb.common.avro.ChrEvent;
import com.fdb.common.avro.ChrEventType;
import com.fdb.common.avro.RatType;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class AvroSerdeTest {

    private static ChrEvent sample() {
        return ChrEvent.newBuilder()
            .setChrId("c-1").setEventTs(123L).setImsi("imsi-1")
            .setSiteId("S1").setCellId("C1")
            .setEventType(ChrEventType.ATTACH).setRatType(RatType.NR_SA)
            .setPci(1).setTac(1).setEci(1L).setMcc("460").setMnc("00")
            .setResultCode(0).setLatitude(0.0).setLongitude(0.0)
            .build();
    }

    @Test
    void serializer_then_deserializer_roundtrip() {
        Serializer<ChrEvent> ser = AvroSerde.serializer(ChrEvent.class);
        Deserializer<ChrEvent> de  = AvroSerde.deserializer(ChrEvent.class);

        ChrEvent original = sample();
        byte[] bytes = ser.serialize("topic", original);
        ChrEvent decoded = de.deserialize("topic", bytes);

        assertThat(decoded).isEqualTo(original);
    }

    @Test
    void serializer_returns_null_for_null_input() {
        Serializer<ChrEvent> ser = AvroSerde.serializer(ChrEvent.class);
        assertThat(ser.serialize("topic", null)).isNull();
    }

    @Test
    void deserializer_returns_null_for_null_bytes() {
        Deserializer<ChrEvent> de = AvroSerde.deserializer(ChrEvent.class);
        assertThat(de.deserialize("topic", null)).isNull();
    }
}
```

注意：此测试需要 `org.apache.kafka:kafka-clients` 依赖。下一步 `pom.xml` 修改加入。

- [ ] **Step 2: 修改 parent `pom.xml` 加入 Kafka clients 依赖管理**

修改文件：`pom.xml`，在 `<dependencyManagement><dependencies>` 块中追加：

```xml
            <dependency>
                <groupId>org.apache.kafka</groupId>
                <artifactId>kafka-clients</artifactId>
                <version>3.7.0</version>
            </dependency>
```

修改文件：`common/pom.xml`，在 `<dependencies>` 块中追加（紧挨着 snakeyaml 之后）：

```xml
        <dependency>
            <groupId>org.apache.kafka</groupId>
            <artifactId>kafka-clients</artifactId>
        </dependency>
```

- [ ] **Step 3: 运行测试，确认失败**

```bash
mvn -pl common -am test -Dtest=AvroSerdeTest
```

预期：编译错误，`AvroSerde` 类不存在。

- [ ] **Step 4: 写 `AvroSerde.java`**

文件：`common/src/main/java/com/fdb/common/kafka/AvroSerde.java`

```java
package com.fdb.common.kafka;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

public final class AvroSerde {

    private AvroSerde() {}

    public static <T extends SpecificRecord> Serializer<T> serializer(Class<T> type) {
        DatumWriter<T> writer = new SpecificDatumWriter<>(type);
        return new Serializer<T>() {
            @Override
            public byte[] serialize(String topic, T record) {
                if (record == null) return null;
                try {
                    ByteArrayOutputStream out = new ByteArrayOutputStream(256);
                    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
                    writer.write(record, encoder);
                    encoder.flush();
                    return out.toByteArray();
                } catch (Exception e) {
                    throw new SerializationException("Avro serialize failed for " + type.getName(), e);
                }
            }
        };
    }

    public static <T extends SpecificRecord> Deserializer<T> deserializer(Class<T> type) {
        DatumReader<T> reader = new SpecificDatumReader<>(type);
        return new Deserializer<T>() {
            @Override
            public T deserialize(String topic, byte[] data) {
                if (data == null) return null;
                try {
                    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(new ByteArrayInputStream(data), null);
                    return reader.read(null, decoder);
                } catch (Exception e) {
                    throw new SerializationException("Avro deserialize failed for " + type.getName(), e);
                }
            }
        };
    }
}
```

- [ ] **Step 5: 运行测试，确认通过**

```bash
mvn -pl common -am test -Dtest=AvroSerdeTest
```

预期：3 个测试 PASS。

- [ ] **Step 6: 提交**

```bash
git add pom.xml common/pom.xml common/src/main/java/com/fdb/common/kafka/AvroSerde.java common/src/test/java/com/fdb/common/kafka/AvroSerdeTest.java
git commit -m "feat(common): add Kafka Avro Serializer/Deserializer"
```

---

## Task 10: 三层 ConfigLoader（默认 / 文件 / 环境变量）

**Files:**
- Create: `common/src/main/java/com/fdb/common/config/ConfigLoader.java`
- Create: `common/src/test/resources/test-default.yaml`
- Create: `common/src/test/resources/test-overlay.yaml`
- Create: `common/src/test/java/com/fdb/common/config/ConfigLoaderTest.java`

- [ ] **Step 1: 写测试 yaml fixture**

文件：`common/src/test/resources/test-default.yaml`

```yaml
kafka:
  bootstrap: default-broker:9092
  topic: chr-events
mysql:
  url: jdbc:mysql://default:3306/fdb
  user: default-user
nested:
  deeply:
    value: original
```

文件：`common/src/test/resources/test-overlay.yaml`

```yaml
kafka:
  bootstrap: file-broker:9092
nested:
  deeply:
    value: from-file
```

- [ ] **Step 2: 写失败测试**

文件：`common/src/test/java/com/fdb/common/config/ConfigLoaderTest.java`

```java
package com.fdb.common.config;

import org.junit.jupiter.api.Test;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ConfigLoaderTest {

    private static Path resource(String name) throws URISyntaxException {
        return Paths.get(ConfigLoaderTest.class.getClassLoader().getResource(name).toURI());
    }

    @Test
    void defaults_only_when_no_overlay_no_env() throws Exception {
        Config cfg = ConfigLoader.builder()
            .defaultResource("test-default.yaml")
            .envPrefix("FDB_TEST_")
            .envSource(Map.of())
            .build()
            .load();

        assertThat(cfg.getString("kafka.bootstrap")).isEqualTo("default-broker:9092");
        assertThat(cfg.getString("kafka.topic")).isEqualTo("chr-events");
        assertThat(cfg.getString("nested.deeply.value")).isEqualTo("original");
    }

    @Test
    void file_overlay_overrides_default() throws Exception {
        Config cfg = ConfigLoader.builder()
            .defaultResource("test-default.yaml")
            .overlayFile(resource("test-overlay.yaml"))
            .envPrefix("FDB_TEST_")
            .envSource(Map.of())
            .build()
            .load();

        assertThat(cfg.getString("kafka.bootstrap")).isEqualTo("file-broker:9092");
        // 文件未覆盖的字段保持默认
        assertThat(cfg.getString("kafka.topic")).isEqualTo("chr-events");
        assertThat(cfg.getString("nested.deeply.value")).isEqualTo("from-file");
    }

    @Test
    void env_var_overrides_file_and_default() throws Exception {
        Config cfg = ConfigLoader.builder()
            .defaultResource("test-default.yaml")
            .overlayFile(resource("test-overlay.yaml"))
            .envPrefix("FDB_TEST_")
            .envSource(Map.of(
                "FDB_TEST_KAFKA_BOOTSTRAP", "env-broker:9092",
                "FDB_TEST_NESTED_DEEPLY_VALUE", "from-env",
                "OTHER_VAR", "ignored"
            ))
            .build()
            .load();

        assertThat(cfg.getString("kafka.bootstrap")).isEqualTo("env-broker:9092");
        assertThat(cfg.getString("nested.deeply.value")).isEqualTo("from-env");
        assertThat(cfg.getString("kafka.topic")).isEqualTo("chr-events");
    }

    @Test
    void missing_key_returns_null_and_required_throws() throws Exception {
        Config cfg = ConfigLoader.builder()
            .defaultResource("test-default.yaml")
            .envPrefix("FDB_TEST_")
            .envSource(Map.of())
            .build()
            .load();

        assertThat(cfg.getStringOrNull("nope")).isNull();
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> cfg.getString("nope"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("nope");
    }
}
```

- [ ] **Step 3: 运行测试，确认失败**

```bash
mvn -pl common -am test -Dtest=ConfigLoaderTest
```

预期：编译错误，`ConfigLoader`、`Config` 类不存在。

- [ ] **Step 4: 写 `ConfigLoader.java`（含内嵌 `Config` 类）**

文件：`common/src/main/java/com/fdb/common/config/ConfigLoader.java`

```java
package com.fdb.common.config;

import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public final class ConfigLoader {

    private final String defaultResource;
    private final Path overlayFile;
    private final String envPrefix;
    private final Map<String, String> envSource;

    private ConfigLoader(Builder b) {
        this.defaultResource = b.defaultResource;
        this.overlayFile = b.overlayFile;
        this.envPrefix = b.envPrefix == null ? "FDB_" : b.envPrefix;
        this.envSource = b.envSource == null ? System.getenv() : b.envSource;
    }

    public static Builder builder() { return new Builder(); }

    public Config load() throws IOException {
        Map<String, Object> merged = new LinkedHashMap<>();

        if (defaultResource != null) {
            try (InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream(defaultResource)) {
                if (in == null) {
                    throw new IOException("default resource not on classpath: " + defaultResource);
                }
                Object loaded = new Yaml().load(in);
                if (loaded instanceof Map<?, ?> map) {
                    deepMerge(merged, castToStringKeyedMap(map));
                }
            }
        }

        if (overlayFile != null) {
            try (InputStream in = Files.newInputStream(overlayFile)) {
                Object loaded = new Yaml().load(in);
                if (loaded instanceof Map<?, ?> map) {
                    deepMerge(merged, castToStringKeyedMap(map));
                }
            }
        }

        applyEnv(merged, envPrefix, envSource);

        return new Config(merged);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> castToStringKeyedMap(Map<?, ?> raw) {
        Map<String, Object> out = new LinkedHashMap<>();
        for (Map.Entry<?, ?> e : raw.entrySet()) {
            String k = String.valueOf(e.getKey());
            Object v = e.getValue();
            if (v instanceof Map<?, ?> nested) {
                out.put(k, castToStringKeyedMap(nested));
            } else {
                out.put(k, v);
            }
        }
        return out;
    }

    @SuppressWarnings("unchecked")
    private static void deepMerge(Map<String, Object> base, Map<String, Object> overlay) {
        for (Map.Entry<String, Object> e : overlay.entrySet()) {
            String k = e.getKey();
            Object oVal = e.getValue();
            Object bVal = base.get(k);
            if (bVal instanceof Map && oVal instanceof Map) {
                deepMerge((Map<String, Object>) bVal, (Map<String, Object>) oVal);
            } else {
                base.put(k, oVal);
            }
        }
    }

    private static void applyEnv(Map<String, Object> base, String prefix, Map<String, String> env) {
        for (Map.Entry<String, String> e : env.entrySet()) {
            String envKey = e.getKey();
            if (!envKey.startsWith(prefix)) continue;
            String dotted = envKey.substring(prefix.length()).toLowerCase().replace('_', '.');
            setByPath(base, dotted, e.getValue());
        }
    }

    @SuppressWarnings("unchecked")
    private static void setByPath(Map<String, Object> base, String dottedPath, Object value) {
        String[] parts = dottedPath.split("\\.");
        Map<String, Object> cur = base;
        for (int i = 0; i < parts.length - 1; i++) {
            Object next = cur.get(parts[i]);
            if (!(next instanceof Map)) {
                Map<String, Object> fresh = new LinkedHashMap<>();
                cur.put(parts[i], fresh);
                next = fresh;
            }
            cur = (Map<String, Object>) next;
        }
        cur.put(parts[parts.length - 1], value);
    }

    public static final class Builder {
        private String defaultResource;
        private Path overlayFile;
        private String envPrefix;
        private Map<String, String> envSource;

        public Builder defaultResource(String resource) { this.defaultResource = resource; return this; }
        public Builder overlayFile(Path file)           { this.overlayFile = file;        return this; }
        public Builder envPrefix(String prefix)          { this.envPrefix = prefix;        return this; }
        public Builder envSource(Map<String, String> env){ this.envSource = env;           return this; }

        public ConfigLoader build() { return new ConfigLoader(this); }
    }

    public static final class Config {
        private final Map<String, Object> root;

        Config(Map<String, Object> root) {
            this.root = Collections.unmodifiableMap(root);
        }

        @SuppressWarnings("unchecked")
        private Object resolve(String dottedPath) {
            String[] parts = dottedPath.split("\\.");
            Object cur = root;
            for (String p : parts) {
                if (cur instanceof Map<?, ?> m) {
                    cur = ((Map<String, Object>) m).get(p);
                } else {
                    return null;
                }
            }
            return cur;
        }

        public String getStringOrNull(String dottedPath) {
            Object v = resolve(dottedPath);
            return v == null ? null : String.valueOf(v);
        }

        public String getString(String dottedPath) {
            String v = getStringOrNull(dottedPath);
            if (v == null) {
                throw new IllegalArgumentException("missing required config key: " + dottedPath);
            }
            return v;
        }

        public Map<String, Object> raw() { return root; }
    }
}
```

- [ ] **Step 5: 运行测试，确认通过**

```bash
mvn -pl common -am test -Dtest=ConfigLoaderTest
```

预期：4 个测试 PASS。

- [ ] **Step 6: 提交**

```bash
git add common/src/main/java/com/fdb/common/config/ConfigLoader.java common/src/test/resources common/src/test/java/com/fdb/common/config/ConfigLoaderTest.java
git commit -m "feat(common): add layered ConfigLoader (default/file/env)"
```

---

## Task 11: docker-compose.yml + dev-up/down 脚本

> 说明：Windows + Docker Desktop（WSL2 backend）下，`docker compose` 命令可用；volume 路径用 `./data/*` 形式（相对 compose 文件目录）。

**Files:**
- Create: `docker/docker-compose.yml`
- Create: `scripts/dev-up.sh`
- Create: `scripts/dev-down.sh`

- [ ] **Step 1: 写 `docker/docker-compose.yml`**

文件：`docker/docker-compose.yml`

```yaml
name: fdb

services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    container_name: fdb-zookeeper
    ports:
      - "2181:2181"
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000

  kafka:
    image: confluentinc/cp-kafka:7.5.0
    container_name: fdb-kafka
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"

  kafka-ui:
    image: provectuslabs/kafka-ui:v0.7.2
    container_name: fdb-kafka-ui
    depends_on:
      - kafka
    ports:
      - "8080:8080"
    environment:
      KAFKA_CLUSTERS_0_NAME: local
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:29092

  mysql:
    image: mysql:8.0
    container_name: fdb-mysql
    ports:
      - "3306:3306"
    environment:
      MYSQL_ROOT_PASSWORD: rootpwd
      MYSQL_DATABASE: fdb
      MYSQL_USER: fdb
      MYSQL_PASSWORD: fdbpwd
    command: ["--default-authentication-plugin=mysql_native_password", "--character-set-server=utf8mb4"]
    volumes:
      - ./data/mysql:/var/lib/mysql

  hms-postgres:
    image: postgres:14
    container_name: fdb-hms-postgres
    ports:
      - "5432:5432"
    environment:
      POSTGRES_USER: hive
      POSTGRES_PASSWORD: hivepwd
      POSTGRES_DB: metastore
    volumes:
      - ./data/postgres:/var/lib/postgresql/data

  hive-metastore:
    image: apache/hive:4.0.0
    container_name: fdb-hive-metastore
    depends_on:
      - hms-postgres
    ports:
      - "9083:9083"
    environment:
      SERVICE_NAME: metastore
      DB_DRIVER: postgres
      SERVICE_OPTS: >-
        -Djavax.jdo.option.ConnectionDriverName=org.postgresql.Driver
        -Djavax.jdo.option.ConnectionURL=jdbc:postgresql://hms-postgres:5432/metastore
        -Djavax.jdo.option.ConnectionUserName=hive
        -Djavax.jdo.option.ConnectionPassword=hivepwd
```

- [ ] **Step 2: 写 `scripts/dev-up.sh`**

文件：`scripts/dev-up.sh`

```bash
#!/usr/bin/env bash
set -euo pipefail

# 用法: ./scripts/dev-up.sh
# 在仓库根目录执行
cd "$(dirname "$0")/.."

echo "[dev-up] 启动本地依赖容器 (Kafka / MySQL / HMS / Postgres)..."
docker compose -f docker/docker-compose.yml up -d

echo "[dev-up] 等待 Kafka 就绪 (最多 60s)..."
for i in {1..30}; do
  if docker compose -f docker/docker-compose.yml exec -T kafka \
     kafka-broker-api-versions --bootstrap-server kafka:29092 >/dev/null 2>&1; then
    echo "[dev-up] Kafka OK"
    break
  fi
  sleep 2
done

echo "[dev-up] 等待 MySQL 就绪 (最多 60s)..."
for i in {1..30}; do
  if docker compose -f docker/docker-compose.yml exec -T mysql \
     mysqladmin ping -h localhost -ufdb -pfdbpwd --silent >/dev/null 2>&1; then
    echo "[dev-up] MySQL OK"
    break
  fi
  sleep 2
done

echo "[dev-up] 全部依赖就绪。kafka-ui: http://localhost:8080"
docker compose -f docker/docker-compose.yml ps
```

- [ ] **Step 3: 写 `scripts/dev-down.sh`**

文件：`scripts/dev-down.sh`

```bash
#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

if [[ "${1:-}" == "--clean" ]]; then
  echo "[dev-down] 停止并删除容器 + 数据卷 (./docker/data)..."
  docker compose -f docker/docker-compose.yml down -v
  rm -rf docker/data
else
  echo "[dev-down] 停止容器 (保留 ./docker/data)..."
  docker compose -f docker/docker-compose.yml down
fi
```

- [ ] **Step 4: 给脚本可执行权限并启动**

```bash
cd D:/agent-code/flink-data-balance
chmod +x scripts/dev-up.sh scripts/dev-down.sh
./scripts/dev-up.sh
```

预期输出（节选）：
```
[dev-up] 启动本地依赖容器 ...
 ✔ Container fdb-zookeeper       Started
 ✔ Container fdb-kafka           Started
 ...
[dev-up] Kafka OK
[dev-up] MySQL OK
[dev-up] 全部依赖就绪。
NAME                IMAGE                                 STATUS
fdb-hive-metastore  apache/hive:4.0.0                     running
fdb-hms-postgres    postgres:14                           running
fdb-kafka           confluentinc/cp-kafka:7.5.0           running
fdb-kafka-ui        provectuslabs/kafka-ui:v0.7.2         running
fdb-mysql           mysql:8.0                             running
fdb-zookeeper       confluentinc/cp-zookeeper:7.5.0       running
```

- [ ] **Step 5: 验证可用性**

```bash
# Kafka
docker exec fdb-kafka kafka-broker-api-versions --bootstrap-server kafka:29092 | head -3

# MySQL
docker exec fdb-mysql mysql -ufdb -pfdbpwd -e "SELECT VERSION();" fdb

# kafka-ui (浏览器访问)
echo "http://localhost:8080"
```

预期：Kafka 输出版本协议，MySQL 输出版本号 `8.0.x`。

- [ ] **Step 6: 提交**

```bash
git add docker/docker-compose.yml scripts/dev-up.sh scripts/dev-down.sh
git commit -m "feat(infra): docker-compose for kafka/mysql/hms + dev up/down scripts"
```

---

## Task 12: Kafka Topic 创建脚本

**Files:**
- Create: `scripts/create-kafka-topics.sh`

- [ ] **Step 1: 写脚本**

文件：`scripts/create-kafka-topics.sh`

```bash
#!/usr/bin/env bash
set -euo pipefail

# 在 fdb-kafka 容器内通过内部监听端口创建 spec 中定义的全部 topic
# 用法: ./scripts/create-kafka-topics.sh

KAFKA_CONTAINER=${FDB_KAFKA_CONTAINER:-fdb-kafka}
INTERNAL_BOOTSTRAP=${FDB_KAFKA_INTERNAL_BOOTSTRAP:-kafka:29092}

create_topic() {
  local name=$1
  local partitions=$2
  local cleanup=$3      # delete | compact
  local retention_ms=${4:-}

  local extra=""
  if [[ -n "$retention_ms" && "$cleanup" == "delete" ]]; then
    extra="--config retention.ms=$retention_ms"
  fi

  echo "[create] $name partitions=$partitions cleanup=$cleanup retention=${retention_ms:-default}"
  docker exec "$KAFKA_CONTAINER" kafka-topics \
    --bootstrap-server "$INTERNAL_BOOTSTRAP" \
    --create --if-not-exists \
    --topic "$name" \
    --partitions "$partitions" \
    --replication-factor 1 \
    --config "cleanup.policy=$cleanup" \
    $extra
}

# 业务 topic
create_topic chr-events       64 delete  604800000     # 7d
create_topic mr-stats         16 delete  259200000     # 3d
create_topic cm-config         8 compact
create_topic topology          4 compact

# 负载均衡控制流
create_topic lb-heartbeat      1 delete  3600000       # 1h
create_topic lb-routing        1 compact

# Flink 输出
create_topic anomaly-events   16 delete  604800000     # 7d
create_topic cell-kpi-1m       8 delete  259200000     # 3d
create_topic cell-kpi-5m       8 delete  604800000     # 7d

# 死信 / 迟到
create_topic chr-dlq           4 delete  604800000
create_topic mr-dlq            4 delete  604800000
create_topic cm-dlq            4 delete  604800000
create_topic enrichment-late   4 delete  604800000

echo
echo "[done] 当前 topic 列表："
docker exec "$KAFKA_CONTAINER" kafka-topics \
  --bootstrap-server "$INTERNAL_BOOTSTRAP" --list | sort
```

- [ ] **Step 2: 给可执行权限并执行**

```bash
chmod +x scripts/create-kafka-topics.sh
./scripts/create-kafka-topics.sh
```

预期输出末尾：
```
[done] 当前 topic 列表：
anomaly-events
cell-kpi-1m
cell-kpi-5m
chr-dlq
chr-events
cm-config
cm-dlq
enrichment-late
lb-heartbeat
lb-routing
mr-dlq
mr-stats
topology
```

（共 13 个，含 `__consumer_offsets` 等内部 topic 时数量更多）

- [ ] **Step 3: 验证 cleanup.policy 配置正确**

```bash
docker exec fdb-kafka kafka-configs \
  --bootstrap-server kafka:29092 \
  --describe --entity-type topics --entity-name cm-config | grep cleanup.policy
```

预期：`cleanup.policy=compact`。

```bash
docker exec fdb-kafka kafka-configs \
  --bootstrap-server kafka:29092 \
  --describe --entity-type topics --entity-name chr-events | grep cleanup.policy
```

预期：`cleanup.policy=delete`。

- [ ] **Step 4: 提交**

```bash
git add scripts/create-kafka-topics.sh
git commit -m "feat(infra): script to create all spec-defined Kafka topics"
```

---

## Task 13: MySQL DDL 脚本

**Files:**
- Create: `scripts/init-mysql.sql`

- [ ] **Step 1: 写 DDL**

文件：`scripts/init-mysql.sql`

```sql
-- 数据均衡处理工程 — MySQL 数仓 sink 表
-- 在 docker mysql 容器中以 `fdb` 用户执行：
--   docker exec -i fdb-mysql mysql -ufdb -pfdbpwd fdb < scripts/init-mysql.sql

USE fdb;

DROP TABLE IF EXISTS anomaly_events;
CREATE TABLE anomaly_events (
  id            BIGINT AUTO_INCREMENT PRIMARY KEY,
  detection_ts  BIGINT       NOT NULL,
  event_ts      BIGINT       NOT NULL,
  imsi          VARCHAR(32)  NOT NULL,
  site_id       VARCHAR(64)  NOT NULL,
  cell_id       VARCHAR(64)  NOT NULL,
  grid_id       VARCHAR(16),
  latitude      DOUBLE,
  longitude     DOUBLE,
  anomaly_type  VARCHAR(32)  NOT NULL,
  severity      VARCHAR(8),
  rule_version  VARCHAR(32),
  context_json  TEXT,
  UNIQUE KEY uk_anomaly (detection_ts, site_id, cell_id, anomaly_type, imsi),
  KEY idx_event_ts (event_ts),
  KEY idx_cell    (cell_id, detection_ts)
) ENGINE=InnoDB CHARSET=utf8mb4;

DROP TABLE IF EXISTS cell_kpi;
CREATE TABLE cell_kpi (
  id                       BIGINT AUTO_INCREMENT PRIMARY KEY,
  window_start_ts          BIGINT       NOT NULL,
  window_end_ts            BIGINT       NOT NULL,
  window_kind              VARCHAR(8)   NOT NULL,
  site_id                  VARCHAR(64)  NOT NULL,
  cell_id                  VARCHAR(64)  NOT NULL,
  grid_id                  VARCHAR(16),
  num_chr_events           BIGINT,
  num_users                BIGINT,
  avg_rsrp                 FLOAT,
  avg_sinr                 FLOAT,
  avg_prb_usage_dl         FLOAT,
  throughput_dl_mbps_avg   FLOAT,
  drop_rate                FLOAT,
  ho_success_rate          FLOAT,
  attach_success_rate      FLOAT,
  UNIQUE KEY uk_kpi       (window_start_ts, window_kind, site_id, cell_id),
  KEY idx_cell_window     (cell_id, window_kind, window_start_ts)
) ENGINE=InnoDB CHARSET=utf8mb4;
```

- [ ] **Step 2: 执行 DDL**

```bash
docker exec -i fdb-mysql mysql -ufdb -pfdbpwd fdb < scripts/init-mysql.sql
```

预期：无输出，无错误。

- [ ] **Step 3: 验证表已创建**

```bash
docker exec fdb-mysql mysql -ufdb -pfdbpwd fdb -e "SHOW TABLES;"
```

预期：
```
+---------------+
| Tables_in_fdb |
+---------------+
| anomaly_events|
| cell_kpi      |
+---------------+
```

```bash
docker exec fdb-mysql mysql -ufdb -pfdbpwd fdb -e "SHOW INDEX FROM anomaly_events;"
```

预期：可见 `PRIMARY`、`uk_anomaly`、`idx_event_ts`、`idx_cell`。

- [ ] **Step 4: 提交**

```bash
git add scripts/init-mysql.sql
git commit -m "feat(infra): MySQL DDL for anomaly_events and cell_kpi"
```

---

## Task 14: 端到端冒烟（验证 foundation 全套联通）

**Files:**
- 无新增；本任务通过运行命令组合验证整个 foundation 计划完成。

- [ ] **Step 1: 清理后从零启动**

```bash
cd D:/agent-code/flink-data-balance
./scripts/dev-down.sh --clean
./scripts/dev-up.sh
```

预期：所有 6 个容器 `running`。

- [ ] **Step 2: 创建 Kafka topic**

```bash
./scripts/create-kafka-topics.sh
```

预期：13 个业务/控制 topic 创建成功，列表无报错。

- [ ] **Step 3: 创建 MySQL 表**

```bash
docker exec -i fdb-mysql mysql -ufdb -pfdbpwd fdb < scripts/init-mysql.sql
docker exec fdb-mysql mysql -ufdb -pfdbpwd fdb -e "SHOW TABLES;"
```

预期：`anomaly_events`、`cell_kpi` 两张表存在。

- [ ] **Step 4: 全量构建 + 测试**

```bash
mvn -DskipTests=false clean install
```

预期：
- Avro schema 全部生成（5 个 record + 多个 enum）
- 所有单测通过：`SmokeTest`、`ChrEventSchemaTest`、`MrStatSchemaTest`、`CmConfigSchemaTest`、`AnomalyEventSchemaTest`、`CellKpiSchemaTest`、`GeohashTest`、`HashesTest`、`AvroSerdeTest`、`ConfigLoaderTest`
- `BUILD SUCCESS`

测试数应 ≥ 25（粗略：6 个 schema × 1 测试 + 6 + 5 + 3 + 4 + 1 = 25）。

- [ ] **Step 5: 验证手工往 chr-events 发一条 Avro 消息再读出（可选烟雾）**

```bash
# 用 console-producer + console-consumer 简单触摸 topic（值不必正确 Avro，只验证 broker 通）
docker exec -i fdb-kafka bash -c \
  "echo 'hello' | kafka-console-producer --bootstrap-server kafka:29092 --topic chr-events"

docker exec fdb-kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 --topic chr-events --from-beginning --max-messages 1 --timeout-ms 5000
```

预期：消费到 `hello`。

- [ ] **Step 6: 收尾提交（如有未提交的微小修复）**

```bash
git status
# 如果有未跟踪/未提交的修复:
# git add ...
# git commit -m "chore: foundation smoke test fixes"
```

- [ ] **Step 7: 标记 foundation 完成**

```bash
git tag foundation-v0.1.0
git log --oneline -20
```

预期：能看到 14 个任务对应的提交，从 `chore: bootstrap parent POM and common module skeleton` 到 `feat(infra): MySQL DDL ...`。

---

## 自检（Self-Review）

### 1. Spec 覆盖度

| Spec 章节 | 对应任务 | 状态 |
|---|---|---|
| § 3.1 ChrEvent | Task 2 | ✓ |
| § 3.2 MrStat | Task 3 | ✓ |
| § 3.3 CmConfig | Task 4 | ✓ |
| § 3.4 AnomalyEvent | Task 5 | ✓ |
| § 3.5 CellKpi | Task 6 | ✓ |
| § 3.6 兼容性约束（long ts / 无 map / union null + default null） | 已落到所有 .avsc | ✓ |
| § 4 Kafka topic 全景 | Task 12 | ✓ |
| § 7.2 ConsistentHash + slotShift | Task 8 (`Hashes.toVBucketWithShift`) | ✓ |
| § 8 MySQL Schema | Task 13 | ✓ |
| § 10 三层配置合并 | Task 10 | ✓ |
| § 14.2 docker-compose 服务清单 | Task 11 | ✓ |
| Geohash level 7（§ 3.4 / § 7.4） | Task 7 | ✓ |
| Avro Kafka serde（贯穿 simulator + flink-job） | Task 9 | ✓ |

不在本计划范围（属于后续计划）：拓扑服务、模拟器、Flink 作业、Hive 表 DDL（由 Flink HiveCatalog 自动注册）、StarRocks DDL、Grafana dashboard、Avro schema 6 → 用户上下文等。

### 2. Placeholder 扫描

- 所有代码块均含完整可编译内容
- 所有命令均给出预期输出
- 无 "TBD" / "类似 Task N" / "添加适当处理" 等占位
- Task 2 Step 1 的早期草稿（含 `.also` 错误）已在文中明确"删除/简化"，最终代码块独立完整

### 3. 类型/方法名一致性

- `ChrEvent` / `ChrEventType` / `RatType` / `BearerType` — 在 schema 与测试中一致
- `Hashes.toVBucket(String, int)`、`Hashes.toVBucketWithShift(String, int, int)`、`Hashes.murmur3(String)` — 三方法签名固定
- `AvroSerde.serializer(Class<T>)` / `AvroSerde.deserializer(Class<T>)` — 一致
- `ConfigLoader.builder().defaultResource(String).overlayFile(Path).envPrefix(String).envSource(Map).build().load()` — Fluent API 一致
- `Config.getString` / `Config.getStringOrNull` — 测试与实现一致
- `Geohash.encode(double, double, int)` — 测试与实现一致

---

## 执行交接

**Plan complete and saved to `docs/superpowers/plans/2026-05-06-flink-data-balance-foundation.md`. 两种执行选项：**

**1. Subagent-Driven（推荐）** — 每个任务派发一个全新的 subagent，任务间复审，迭代快。
**2. Inline Execution** — 在当前 session 用 executing-plans 批量执行 + checkpoint 复审。

**请选择：1 或 2？**

- 选 **1**：进入 `superpowers:subagent-driven-development` 子技能（fresh subagent per task + 两阶段 review）
- 选 **2**：进入 `superpowers:executing-plans` 子技能（带 checkpoint 的批量执行）

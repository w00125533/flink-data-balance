# AGENTS.md

## 公共基础设施约束

- 新增或修改 Docker Compose 基础设施前，必须先检查 `../shared-data-infra` 是否已经定义同类服务或 profile。
- 如果 `../shared-data-infra` 已定义 HDFS、Hive Metastore、HiveServer2、Spark、YARN、Kafka、ZooKeeper、StarRocks、Prometheus、Grafana 等能力，不要在本工程重复新增；通过 external network、环境变量和项目级命名空间复用。
- 当前迁移边界：HDFS、Hive Metastore/HMS Postgres、HiveServer2、Kafka、ZooKeeper 使用 `../shared-data-infra`；本工程只保留项目级 MySQL、Flink runtime、observability-api、frontend 和 Prometheus。
- 修改基础设施后，至少运行 `docker compose -f docker/docker-compose.yml --profile e2e config`。

## GitNexus 约束

- 本工程已由 GitNexus 索引为 `flink-data-balance`。
- 修改 Java 函数、类或方法前，必须先运行 GitNexus impact analysis，并关注 HIGH/CRITICAL 风险。
- 提交前必须运行 GitNexus detect_changes，确认影响范围符合预期。
- 本次仅改 compose、脚本或文档时，不需要对 Java 符号做 impact analysis。

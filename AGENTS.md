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

<!-- gitnexus:start -->
# GitNexus — Code Intelligence

This project is indexed by GitNexus as **flink-data-balance** (2946 symbols, 7208 relationships, 251 execution flows). Use the GitNexus MCP tools to understand code, assess impact, and navigate safely.

> If any GitNexus tool warns the index is stale, run `npx gitnexus analyze` in terminal first.

## Always Do

- **MUST run impact analysis before editing any symbol.** Before modifying a function, class, or method, run `gitnexus_impact({target: "symbolName", direction: "upstream"})` and report the blast radius (direct callers, affected processes, risk level) to the user.
- **MUST run `gitnexus_detect_changes()` before committing** to verify your changes only affect expected symbols and execution flows.
- **MUST warn the user** if impact analysis returns HIGH or CRITICAL risk before proceeding with edits.
- When exploring unfamiliar code, use `gitnexus_query({query: "concept"})` to find execution flows instead of grepping. It returns process-grouped results ranked by relevance.
- When you need full context on a specific symbol — callers, callees, which execution flows it participates in — use `gitnexus_context({name: "symbolName"})`.

## Never Do

- NEVER edit a function, class, or method without first running `gitnexus_impact` on it.
- NEVER ignore HIGH or CRITICAL risk warnings from impact analysis.
- NEVER rename symbols with find-and-replace — use `gitnexus_rename` which understands the call graph.
- NEVER commit changes without running `gitnexus_detect_changes()` to check affected scope.

## Resources

| Resource | Use for |
|----------|---------|
| `gitnexus://repo/flink-data-balance/context` | Codebase overview, check index freshness |
| `gitnexus://repo/flink-data-balance/clusters` | All functional areas |
| `gitnexus://repo/flink-data-balance/processes` | All execution flows |
| `gitnexus://repo/flink-data-balance/process/{name}` | Step-by-step execution trace |

## CLI

| Task | Read this skill file |
|------|---------------------|
| Understand architecture / "How does X work?" | `.claude/skills/gitnexus/gitnexus-exploring/SKILL.md` |
| Blast radius / "What breaks if I change X?" | `.claude/skills/gitnexus/gitnexus-impact-analysis/SKILL.md` |
| Trace bugs / "Why is X failing?" | `.claude/skills/gitnexus/gitnexus-debugging/SKILL.md` |
| Rename / extract / split / refactor | `.claude/skills/gitnexus/gitnexus-refactoring/SKILL.md` |
| Tools, resources, schema reference | `.claude/skills/gitnexus/gitnexus-guide/SKILL.md` |
| Index, status, clean, wiki CLI commands | `.claude/skills/gitnexus/gitnexus-cli/SKILL.md` |

<!-- gitnexus:end -->

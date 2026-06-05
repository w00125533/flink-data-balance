<!-- gitnexus:start -->
# GitNexus — Code Intelligence

This project is indexed by GitNexus as **flink-data-balance** (2014 symbols, 4491 relationships, 169 execution flows). Use the GitNexus MCP tools to understand code, assess impact, and navigate safely.

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
| Work in the Job area (152 symbols) | `.claude/skills/generated/job/SKILL.md` |
| Work in the Topology area (45 symbols) | `.claude/skills/generated/topology/SKILL.md` |
| Work in the Simulator area (35 symbols) | `.claude/skills/generated/simulator/SKILL.md` |
| Work in the Service area (27 symbols) | `.claude/skills/generated/service/SKILL.md` |
| Work in the Config area (25 symbols) | `.claude/skills/generated/config/SKILL.md` |
| Work in the Coordinator area (23 symbols) | `.claude/skills/generated/coordinator/SKILL.md` |
| Work in the Observability area (22 symbols) | `.claude/skills/generated/observability/SKILL.md` |
| Work in the Api area (14 symbols) | `.claude/skills/generated/api/SKILL.md` |
| Work in the Components area (11 symbols) | `.claude/skills/generated/components/SKILL.md` |
| Work in the Hash area (10 symbols) | `.claude/skills/generated/hash/SKILL.md` |
| Work in the Geo area (9 symbols) | `.claude/skills/generated/geo/SKILL.md` |
| Work in the Kafka area (8 symbols) | `.claude/skills/generated/kafka/SKILL.md` |
| Work in the Summary area (6 symbols) | `.claude/skills/generated/summary/SKILL.md` |
| Work in the Metrics area (5 symbols) | `.claude/skills/generated/metrics/SKILL.md` |

<!-- gitnexus:end -->

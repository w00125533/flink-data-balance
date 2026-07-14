import { describe, expect, it } from 'vitest';
import { resolveFlowEdges } from './flowEdges';

describe('resolveFlowEdges', () => {
  const currentSinkStageIds = [
    'kafka-kpi-1m',
    'kafka-kpi-5m',
    'starrocks-kpi-1m',
    'starrocks-kpi-5m',
    'hive-kpi-1m',
    'hive-kpi-5m',
    'iceberg-kpi-1m',
    'iceberg-kpi-5m',
    'kafka-cell-anomaly',
    'kafka-grid-anomaly',
    'starrocks-cell-anomaly',
    'starrocks-grid-anomaly'
  ];

  it('uses direct routing when dynamic balancing stages are absent', () => {
    expect(resolveFlowEdges(['chr-source', 'pm-source', 'cfg-source', 'kafka', 'enrichment']))
      .toContainEqual(['kafka', 'enrichment']);
    expect(resolveFlowEdges(['chr-source', 'pm-source', 'cfg-source', 'kafka', 'enrichment']))
      .not.toContainEqual(['kafka', 'assigner']);
  });

  it('uses dynamic routing when balancing stages are present', () => {
    const edges = resolveFlowEdges(['kafka', 'assigner', 'enrichment', 'load-coordinator']);

    expect(edges).toContainEqual(['kafka', 'assigner']);
    expect(edges).toContainEqual(['assigner', 'enrichment']);
    expect(edges).toContainEqual(['load-coordinator', 'assigner']);
    expect(edges).not.toContainEqual(['kafka', 'enrichment']);
  });

  it('connects enrichment to every current sink stage id', () => {
    const edges = resolveFlowEdges(['kafka', 'enrichment', ...currentSinkStageIds]);

    currentSinkStageIds.forEach((sinkStageId) => {
      expect(edges).toContainEqual(['enrichment', sinkStageId]);
    });
    expect(edges).not.toContainEqual(['enrichment', 'cell-anomaly-kafka-sink']);
    expect(edges).not.toContainEqual(['enrichment', 'grid-anomaly-kafka-sink']);
    expect(edges).not.toContainEqual(['enrichment', 'hive-sink']);
    expect(edges).not.toContainEqual(['enrichment', 'iceberg-sink']);
  });
});

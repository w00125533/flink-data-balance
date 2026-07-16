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
    'kafka-user-anomaly',
    'kafka-grid-anomaly',
    'starrocks-cell-anomaly',
    'starrocks-user-anomaly',
    'starrocks-grid-anomaly',
    'hive-cell-anomaly',
    'hive-user-anomaly',
    'hive-grid-anomaly',
    'iceberg-cell-anomaly',
    'iceberg-user-anomaly',
    'iceberg-grid-anomaly'
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

  it('connects KPI and anomaly stages from their producing stages', () => {
    const edges = resolveFlowEdges(['kafka', 'enrichment', 'kpi-1m', 'kpi-5m', ...currentSinkStageIds]);

    expect(edges).toContainEqual(['kafka', 'kpi-1m']);
    expect(edges).toContainEqual(['kpi-1m', 'kpi-5m']);
    ['kafka', 'starrocks', 'hive', 'iceberg'].forEach((sink) => {
      expect(edges).toContainEqual(['kpi-1m', `${sink}-kpi-1m`]);
      expect(edges).toContainEqual(['kpi-5m', `${sink}-kpi-5m`]);
      expect(edges).toContainEqual(['kpi-1m', `${sink}-cell-anomaly`]);
      expect(edges).toContainEqual(['enrichment', `${sink}-user-anomaly`]);
      expect(edges).toContainEqual(['enrichment', `${sink}-grid-anomaly`]);
    });
    expect(edges).not.toContainEqual(['enrichment', 'cell-anomaly-kafka-sink']);
    expect(edges).not.toContainEqual(['enrichment', 'grid-anomaly-kafka-sink']);
    expect(edges).not.toContainEqual(['enrichment', 'hive-sink']);
    expect(edges).not.toContainEqual(['enrichment', 'iceberg-sink']);
  });

  it('only connects active starrocks sink nodes when only starrocks sinks are active', () => {
    const edges = resolveFlowEdges([
      'chr-source',
      'pm-source',
      'cfg-source',
      'kafka',
      'enrichment',
      'kpi-1m',
      'kpi-5m',
      'starrocks-kpi-1m',
      'starrocks-kpi-5m',
      'starrocks-cell-anomaly',
      'starrocks-user-anomaly',
      'starrocks-grid-anomaly'
    ]);

    expect(edges).toContainEqual(['kpi-1m', 'starrocks-kpi-1m']);
    expect(edges).toContainEqual(['kpi-5m', 'starrocks-kpi-5m']);
    expect(edges).toContainEqual(['kpi-1m', 'starrocks-cell-anomaly']);
    expect(edges).toContainEqual(['enrichment', 'starrocks-user-anomaly']);
    expect(edges).toContainEqual(['enrichment', 'starrocks-grid-anomaly']);
    expect(edges).not.toContainEqual(['kpi-1m', 'kafka-kpi-1m']);
    expect(edges).not.toContainEqual(['kpi-5m', 'kafka-kpi-5m']);
    expect(edges).not.toContainEqual(['enrichment', 'kafka-cell-anomaly']);
    expect(edges).not.toContainEqual(['enrichment', 'kafka-user-anomaly']);
    expect(edges).not.toContainEqual(['enrichment', 'kafka-grid-anomaly']);
    expect(edges).not.toContainEqual(['kpi-1m', 'hive-kpi-1m']);
    expect(edges).not.toContainEqual(['kpi-5m', 'hive-kpi-5m']);
    expect(edges).not.toContainEqual(['enrichment', 'hive-cell-anomaly']);
    expect(edges).not.toContainEqual(['enrichment', 'hive-user-anomaly']);
    expect(edges).not.toContainEqual(['enrichment', 'hive-grid-anomaly']);
    expect(edges).not.toContainEqual(['kpi-1m', 'iceberg-kpi-1m']);
    expect(edges).not.toContainEqual(['kpi-5m', 'iceberg-kpi-5m']);
    expect(edges).not.toContainEqual(['enrichment', 'iceberg-cell-anomaly']);
    expect(edges).not.toContainEqual(['enrichment', 'iceberg-user-anomaly']);
    expect(edges).not.toContainEqual(['enrichment', 'iceberg-grid-anomaly']);
  });

  it('keeps only source, kafka, enrichment, and KPI compute edges in none mode', () => {
    expect(resolveFlowEdges(['chr-source', 'pm-source', 'cfg-source', 'kafka', 'enrichment', 'kpi-1m', 'kpi-5m'])).toEqual([
      ['chr-source', 'kafka'],
      ['pm-source', 'kafka'],
      ['cfg-source', 'kafka'],
      ['kafka', 'enrichment'],
      ['kafka', 'kpi-1m'],
      ['kpi-1m', 'kpi-5m']
    ]);
  });
});

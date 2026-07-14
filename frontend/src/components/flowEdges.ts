export type FlowEdge = readonly [string, string];

const sourceEdges: FlowEdge[] = [
  ['chr-source', 'kafka'],
  ['pm-source', 'kafka'],
  ['cfg-source', 'kafka']
];

const dynamicRoutingEdges: FlowEdge[] = [
  ['kafka', 'assigner'],
  ['assigner', 'enrichment'],
  ['assigner', 'load-coordinator'],
  ['load-coordinator', 'assigner']
];

const directRoutingEdges: FlowEdge[] = [
  ['kafka', 'enrichment']
];

const possibleSinkEdges: FlowEdge[] = [
  ['enrichment', 'kafka-kpi-1m'],
  ['enrichment', 'kafka-kpi-5m'],
  ['enrichment', 'starrocks-kpi-1m'],
  ['enrichment', 'starrocks-kpi-5m'],
  ['enrichment', 'hive-kpi-1m'],
  ['enrichment', 'hive-kpi-5m'],
  ['enrichment', 'iceberg-kpi-1m'],
  ['enrichment', 'iceberg-kpi-5m'],
  ['enrichment', 'kafka-cell-anomaly'],
  ['enrichment', 'kafka-grid-anomaly'],
  ['enrichment', 'starrocks-cell-anomaly'],
  ['enrichment', 'starrocks-grid-anomaly'],
  ['enrichment', 'hive-cell-anomaly'],
  ['enrichment', 'hive-grid-anomaly'],
  ['enrichment', 'iceberg-cell-anomaly'],
  ['enrichment', 'iceberg-grid-anomaly']
];

export function resolveFlowEdges(stageIds: Iterable<string>): FlowEdge[] {
  const ids = new Set(stageIds);
  const dynamicBalancingEnabled = ids.has('assigner') || ids.has('load-coordinator');

  return [
    ...sourceEdges,
    ...(dynamicBalancingEnabled ? dynamicRoutingEdges : directRoutingEdges),
    ...possibleSinkEdges.filter(([source, target]) => ids.has(source) && ids.has(target))
  ];
}

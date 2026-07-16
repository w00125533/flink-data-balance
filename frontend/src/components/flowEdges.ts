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

const computeEdges: FlowEdge[] = [
  ['kafka', 'kpi-1m'],
  ['kpi-1m', 'kpi-5m']
];

const possibleSinkEdges: FlowEdge[] = [
  ['kpi-1m', 'kafka-kpi-1m'],
  ['kpi-5m', 'kafka-kpi-5m'],
  ['kpi-1m', 'starrocks-kpi-1m'],
  ['kpi-5m', 'starrocks-kpi-5m'],
  ['kpi-1m', 'hive-kpi-1m'],
  ['kpi-5m', 'hive-kpi-5m'],
  ['kpi-1m', 'iceberg-kpi-1m'],
  ['kpi-5m', 'iceberg-kpi-5m'],
  ['kpi-1m', 'kafka-cell-anomaly'],
  ['enrichment', 'kafka-user-anomaly'],
  ['enrichment', 'kafka-grid-anomaly'],
  ['kpi-1m', 'starrocks-cell-anomaly'],
  ['enrichment', 'starrocks-user-anomaly'],
  ['enrichment', 'starrocks-grid-anomaly'],
  ['kpi-1m', 'hive-cell-anomaly'],
  ['enrichment', 'hive-user-anomaly'],
  ['enrichment', 'hive-grid-anomaly'],
  ['kpi-1m', 'iceberg-cell-anomaly'],
  ['enrichment', 'iceberg-user-anomaly'],
  ['enrichment', 'iceberg-grid-anomaly']
];

export function resolveFlowEdges(stageIds: Iterable<string>): FlowEdge[] {
  const ids = new Set(stageIds);
  const dynamicBalancingEnabled = ids.has('assigner') || ids.has('load-coordinator');

  return [
    ...sourceEdges,
    ...(dynamicBalancingEnabled ? dynamicRoutingEdges : directRoutingEdges),
    ...computeEdges.filter(([source, target]) => ids.has(source) && ids.has(target)),
    ...possibleSinkEdges.filter(([source, target]) => ids.has(source) && ids.has(target))
  ];
}

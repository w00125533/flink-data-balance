export type StageStatusValue = 'healthy' | 'warning' | 'critical' | 'idle' | 'unknown';

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
  source: 'chr' | 'pm' | 'cfg';
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
  sink: string;
  window: string;
  status: StageStatusValue;
  rowsWritten: number;
  writeLatencyP95Ms: number;
  summary: string;
  updatedAt: string;
}

export interface ExecutionRunSummary {
  runId: string;
  status: string;
  startedAt?: string;
  completedAt?: string;
  metricCount: number;
  summary: string;
}

export interface ExecutionMetric {
  section: string;
  metric: string;
  value: string;
}

export interface ExecutionRunDetail {
  runId: string;
  status: string;
  startedAt?: string;
  completedAt?: string;
  metrics: ExecutionMetric[];
  rawLines: string[];
}

export interface ResultQueryParams {
  startTs?: number | string;
  endTs?: number | string;
  siteId?: string;
  cellId?: string;
  gridId?: string;
  joinQuality?: string;
  limit?: number | string;
}

export interface AnomalyQueryParams {
  startTs?: number | string;
  endTs?: number | string;
  siteId?: string;
  cellId?: string;
  gridId?: string;
  severity?: string;
  anomalyType?: string;
  limit?: number | string;
}

export interface KpiResultRow {
  windowStartTs: number | null;
  windowEndTs: number | null;
  windowKind: string | null;
  joinQuality: string | null;
  siteId: string | null;
  cellId: string | null;
  gridId: string | null;
  numChrEvents: number | null;
  numUsers: number | null;
  avgRsrp: number | null;
  avgSinr: number | null;
  avgPrbUsageDl: number | null;
  throughputDlMbpsAvg: number | null;
  dropRate: number | null;
  hoSuccessRate: number | null;
  attachSuccessRate: number | null;
}

export interface AnomalyResultRow {
  detectionTs: number | null;
  eventTs: number | null;
  siteId: string | null;
  cellId: string | null;
  gridId: string | null;
  anomalyType: string | null;
  severity: string | null;
  contextJson: string | null;
  latitude: number | null;
  longitude: number | null;
  ruleVersion: string | null;
}

export interface SinkLatencySummary {
  sinkName: string;
  sinkType: string;
  dataset: string;
  windowKind: string;
  records: number;
  bytes: number;
  durationMs: number;
  p50Ms: number;
  p95Ms: number;
  p99Ms: number;
  failureCount: number;
  lastError?: string;
  checkpointId: number;
  updatedAt: string;
}

export interface RuntimeConfig {
  dynamicBalancingEnabled: boolean;
  resultQueryLayer: string;
  kpiStorage: string;
  anomalyStorage: string;
}

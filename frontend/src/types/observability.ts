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

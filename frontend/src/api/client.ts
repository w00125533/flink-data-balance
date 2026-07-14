import type {
  AnomalyQueryParams,
  AnomalyResultRow,
  ExecutionRunDetail,
  ExecutionRunSummary,
  KpiResultRow,
  MigrationEvent,
  ResultQueryParams,
  RuntimeConfig,
  SinkLatencySummary,
  SinkSummary,
  SourceSummary,
  StageStatus
} from '../types/observability';

async function getJson<T>(path: string): Promise<T> {
  const response = await fetch(path);
  if (!response.ok) {
    throw new Error(`Request failed: ${response.status} ${path}`);
  }
  return response.json() as Promise<T>;
}

function withQuery(path: string, params: object) {
  const query = new URLSearchParams();
  Object.entries(params).forEach(([key, value]) => {
    if ((typeof value !== 'string' && typeof value !== 'number') || value === '') {
      return;
    }
    query.set(key, String(value));
  });
  const queryString = query.toString();
  return queryString ? `${path}?${queryString}` : path;
}

export function fetchStageStatuses() {
  return getJson<StageStatus[]>('/api/flow/status');
}

export function fetchSourceSummaries() {
  return getJson<SourceSummary[]>('/api/flow/sources');
}

export function fetchMigrationEvents() {
  return getJson<MigrationEvent[]>('/api/flow/migrations');
}

export function fetchSinkSummaries() {
  return getJson<SinkSummary[]>('/api/flow/sinks');
}

export function fetchExecutionRuns() {
  return getJson<ExecutionRunSummary[]>('/api/runs');
}

export function fetchExecutionRunDetail(runId: string) {
  return getJson<ExecutionRunDetail>(`/api/runs/${encodeURIComponent(runId)}`);
}

export function fetchKpiResults(windowKind: '1m' | '5m', params: ResultQueryParams): Promise<KpiResultRow[]> {
  return getJson<KpiResultRow[]>(withQuery(`/api/results/kpi/${windowKind}`, params));
}

export function fetchCellAnomalies(params: AnomalyQueryParams): Promise<AnomalyResultRow[]> {
  return getJson<AnomalyResultRow[]>(withQuery('/api/results/anomalies/cell', params));
}

export function fetchGridAnomalies(params: AnomalyQueryParams): Promise<AnomalyResultRow[]> {
  return getJson<AnomalyResultRow[]>(withQuery('/api/results/anomalies/grid', params));
}

export function fetchSinkLatency(): Promise<SinkLatencySummary[]> {
  return getJson<SinkLatencySummary[]>('/api/results/sink-latency');
}

export function fetchRuntimeConfig(): Promise<RuntimeConfig> {
  return getJson<RuntimeConfig>('/api/runtime/config');
}

export function subscribeObservabilityEvents(handlers: {
  onStageStatus?: (stages: StageStatus[]) => void;
  onMigrationEvent?: (events: MigrationEvent[]) => void;
  onError?: () => void;
}) {
  const source = new EventSource('/api/events/stream');
  source.addEventListener('stage_status', (event) => {
    handlers.onStageStatus?.(JSON.parse(event.data) as StageStatus[]);
  });
  source.addEventListener('migration_event', (event) => {
    handlers.onMigrationEvent?.(JSON.parse(event.data) as MigrationEvent[]);
  });
  source.onerror = () => {
    handlers.onError?.();
  };
  return () => source.close();
}

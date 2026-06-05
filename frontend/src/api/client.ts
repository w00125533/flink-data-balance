import type {
  ExecutionRunDetail,
  ExecutionRunSummary,
  MigrationEvent,
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

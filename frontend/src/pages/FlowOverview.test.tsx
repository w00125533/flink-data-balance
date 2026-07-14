import '@testing-library/jest-dom/vitest';
import { render, screen } from '@testing-library/react';
import type { ReactNode } from 'react';
import { beforeEach, expect, test, vi } from 'vitest';
import type { RuntimeConfig, SinkSummary, SourceSummary, StageStatus } from '../types/observability';
import FlowOverview from './FlowOverview';

const client = vi.hoisted(() => ({
  fetchRuntimeConfig: vi.fn(),
  fetchSinkSummaries: vi.fn(),
  fetchSourceSummaries: vi.fn(),
  fetchStageStatuses: vi.fn(),
  subscribeObservabilityEvents: vi.fn()
}));

vi.mock('../api/client', () => ({
  fetchRuntimeConfig: client.fetchRuntimeConfig,
  fetchSinkSummaries: client.fetchSinkSummaries,
  fetchSourceSummaries: client.fetchSourceSummaries,
  fetchStageStatuses: client.fetchStageStatuses,
  subscribeObservabilityEvents: client.subscribeObservabilityEvents
}));

vi.mock('../components/SourceLatencyCard', () => ({
  default: ({ source }: { source: SourceSummary }) => <div>source:{source.source}</div>
}));

vi.mock('../components/StreamingFlowGraph', () => ({
  default: ({ stages }: { stages: StageStatus[] }) => (
    <div data-testid="graph-stage-ids">{stages.map((stage) => stage.stageId).join(',')}</div>
  )
}));

vi.mock('../components/StageStatusPanel', () => ({
  default: ({ stages }: { stages: StageStatus[] }) => (
    <div data-testid="stage-panel-ids">{stages.map((stage) => stage.stageId).join(',')}</div>
  )
}));

vi.mock('../components/SinkSummaryPanel', () => ({
  default: ({ sinks }: { sinks: SinkSummary[] }) => (
    <div data-testid="sink-ids">{sinks.map((sink) => sink.sink).join(',')}</div>
  )
}));

vi.mock('antd', () => {
  function Shell({ children }: { children?: ReactNode }) {
    return <div>{children}</div>;
  }

  return {
    Alert: ({ message, description }: { message?: ReactNode; description?: ReactNode }) => (
      <div role="alert">
        {message}
        {description}
      </div>
    ),
    Card: ({ children, title }: { children?: ReactNode; title?: ReactNode }) => (
      <section>
        {title ? <h2>{title}</h2> : null}
        {children}
      </section>
    ),
    Col: Shell,
    Row: Shell,
    Space: Shell,
    Tag: ({ children }: { children?: ReactNode }) => <span>{children}</span>,
    Typography: {
      Text: ({ children }: { children: ReactNode }) => <span>{children}</span>,
      Title: ({ children }: { children: ReactNode }) => <h1>{children}</h1>
    }
  };
});

beforeEach(() => {
  vi.clearAllMocks();
  client.fetchSourceSummaries.mockResolvedValue([source('chr')]);
  client.fetchStageStatuses.mockResolvedValue([stage('enrichment')]);
  client.fetchSinkSummaries.mockResolvedValue([]);
  client.fetchRuntimeConfig.mockResolvedValue(runtime());
  client.subscribeObservabilityEvents.mockReturnValue(vi.fn());
});

test('updates flow summaries when runtime config fails', async () => {
  client.fetchRuntimeConfig.mockRejectedValue(new Error('runtime unavailable'));
  client.fetchStageStatuses.mockResolvedValue([stage('enrichment'), stage('starrocks-kpi-1m')]);
  client.fetchSinkSummaries.mockResolvedValue([sink('starrocks-kpi-1m')]);

  render(<FlowOverview />);

  expect(await screen.findByText('source:chr')).toBeInTheDocument();
  expect(screen.getByTestId('graph-stage-ids')).toHaveTextContent('enrichment,starrocks-kpi-1m');
  expect(screen.getByTestId('stage-panel-ids')).toHaveTextContent('enrichment,starrocks-kpi-1m');
  expect(screen.getByTestId('sink-ids')).toHaveTextContent('starrocks-kpi-1m');
  expect(screen.getByRole('alert')).toHaveTextContent('runtime unavailable');
});

test('keeps known sink data visible when runtime result sink is unknown', async () => {
  client.fetchRuntimeConfig.mockResolvedValue(runtime({ resultSink: 'typo-sink' }));
  client.fetchStageStatuses.mockResolvedValue([
    stage('enrichment'),
    stage('starrocks-kpi-1m'),
    stage('hive-kpi-1m'),
    stage('iceberg-kpi-1m')
  ]);
  client.fetchSinkSummaries.mockResolvedValue([
    sink('starrocks-kpi-1m'),
    sink('hive-kpi-1m'),
    sink('iceberg-kpi-1m')
  ]);

  render(<FlowOverview />);

  expect(await screen.findByTestId('graph-stage-ids')).toHaveTextContent(
    'enrichment,starrocks-kpi-1m,hive-kpi-1m,iceberg-kpi-1m'
  );
  expect(screen.getByTestId('stage-panel-ids')).toHaveTextContent(
    'enrichment,starrocks-kpi-1m,hive-kpi-1m,iceberg-kpi-1m'
  );
  expect(screen.getByTestId('sink-ids')).toHaveTextContent('starrocks-kpi-1m,hive-kpi-1m,iceberg-kpi-1m');
});

function runtime(overrides: Partial<RuntimeConfig> = {}): RuntimeConfig {
  return {
    dynamicBalancingEnabled: false,
    resultSink: 'starrocks',
    dlqEnabled: true,
    metricsEnabled: true,
    metricsHistoryEnabled: true,
    runId: 'run-test',
    runLabel: '',
    parallelism: 4,
    checkpointIntervalMs: 30_000,
    jobStatus: 'running',
    reportStatus: 'collecting',
    ...overrides
  };
}

function source(sourceName: SourceSummary['source']): SourceSummary {
  return {
    source: sourceName,
    status: 'healthy',
    eps: 1,
    kafkaLag: 0,
    eventDelayMs: 0,
    summary: 'ok',
    updatedAt: '2026-07-14T00:00:00Z'
  };
}

function stage(stageId: string): StageStatus {
  return {
    stageId,
    label: stageId,
    status: 'healthy',
    inEps: 1,
    outEps: 1,
    latencyP50Ms: 10,
    latencyP95Ms: 20,
    watermarkLagMs: 0,
    dlqCount: 0,
    summary: 'ok',
    updatedAt: '2026-07-14T00:00:00Z'
  };
}

function sink(sinkName: string): SinkSummary {
  return {
    sink: sinkName,
    window: 'MIN_1',
    status: 'healthy',
    rowsWritten: 1,
    writeLatencyP95Ms: 20,
    summary: 'ok',
    updatedAt: '2026-07-14T00:00:00Z'
  };
}

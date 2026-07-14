import { Alert, Card, Col, Row, Space, Tag, Typography } from 'antd';
import { useEffect, useMemo, useState } from 'react';
import {
  fetchRuntimeConfig,
  fetchSinkSummaries,
  fetchSourceSummaries,
  fetchStageStatuses,
  subscribeObservabilityEvents
} from '../api/client';
import SinkSummaryPanel from '../components/SinkSummaryPanel';
import SourceLatencyCard from '../components/SourceLatencyCard';
import StageStatusPanel from '../components/StageStatusPanel';
import StreamingFlowGraph from '../components/StreamingFlowGraph';
import type { RuntimeConfig, SinkSummary, SourceSummary, StageStatus } from '../types/observability';

export default function FlowOverview() {
  const [sources, setSources] = useState<SourceSummary[]>([]);
  const [stages, setStages] = useState<StageStatus[]>([]);
  const [sinks, setSinks] = useState<SinkSummary[]>([]);
  const [runtime, setRuntime] = useState<RuntimeConfig>();
  const [error, setError] = useState<string>();
  const graphStages = useMemo(() => filterFlowStages(stages, runtime?.resultSink), [stages, runtime?.resultSink]);
  const activeSinks = useMemo(() => filterActiveSinks(sinks, runtime?.resultSink), [sinks, runtime?.resultSink]);
  const bottleneckCandidates = useMemo(
    () => resolveBottleneckCandidates(graphStages, activeSinks),
    [graphStages, activeSinks]
  );

  useEffect(() => {
    let active = true;
    const refreshSummary = () => {
      const summariesPromise = Promise.all([
        fetchSourceSummaries(),
        fetchStageStatuses(),
        fetchSinkSummaries()
      ]);
      const runtimePromise = fetchRuntimeConfig();
      return Promise.allSettled([summariesPromise, runtimePromise] as const).then(([summaryResult, runtimeResult]) => {
        if (!active) {
          return;
        }
        const errors: string[] = [];
        if (summaryResult.status === 'fulfilled') {
          const [nextSources, nextStages, nextSinks] = summaryResult.value;
          setSources(nextSources);
          setStages(nextStages);
          setSinks(nextSinks);
        } else {
          errors.push(formatErrorMessage(summaryResult.reason));
        }
        if (runtimeResult.status === 'fulfilled') {
          setRuntime(runtimeResult.value);
        } else {
          setRuntime(undefined);
          errors.push(formatErrorMessage(runtimeResult.reason));
        }
        setError(errors.length > 0 ? errors.join('; ') : undefined);
      });
    };
    void refreshSummary();

    const closeEvents = subscribeObservabilityEvents({
      onStageStatus: (nextStages) => {
        if (active) {
          setStages(nextStages);
        }
      }
    });
    const summaryTimer = window.setInterval(() => {
      void refreshSummary();
    }, 10_000);

    return () => {
      active = false;
      closeEvents();
      window.clearInterval(summaryTimer);
    };
  }, []);

  return (
    <Space direction="vertical" size={16} style={{ width: '100%' }}>
      <Typography.Title level={3} style={{ margin: 0 }}>
        实时流处理总览
      </Typography.Title>
      {error ? <Alert type="error" message="观测 API 请求失败" description={error} /> : null}
      {runtime ? <RuntimeInfoBar runtime={runtime} /> : null}
      <Row gutter={[12, 12]}>
        {sources.map((source) => (
          <Col xs={24} md={8} key={source.source}>
            <SourceLatencyCard source={source} />
          </Col>
        ))}
      </Row>
      <StreamingFlowGraph stages={graphStages} />
      <BottleneckSummary candidates={bottleneckCandidates} />
      <StageStatusPanel stages={graphStages} />
      <SinkSummaryPanel sinks={activeSinks} />
    </Space>
  );
}

type RuntimeInfoItem = {
  label: string;
  value: string;
  color?: string;
};

type BottleneckCandidate = {
  key: string;
  label: string;
  color: string;
};

const knownSinkStagePrefixes = ['kafka-', 'starrocks-', 'hive-', 'iceberg-'] as const;
const knownResultSinkValues = new Set(['starrocks', 'iceberg', 'hive', 'kafka', 'none']);
const stageLatencyThresholdMs = 1_000;
const watermarkLagThresholdMs = 60_000;
const maxBottleneckCandidates = 8;

function RuntimeInfoBar({ runtime }: { runtime: RuntimeConfig }) {
  const runValue = runtime.runLabel ? `${runtime.runId} (${runtime.runLabel})` : runtime.runId;
  const metricsValue = runtime.metricsEnabled
    ? runtime.metricsHistoryEnabled ? 'on / history' : 'on'
    : 'off';
  const items: RuntimeInfoItem[] = [
    { label: 'Run ID', value: runValue || '-' },
    { label: 'Result Sink', value: runtime.resultSink || 'none', color: 'blue' },
    { label: 'Metrics', value: metricsValue, color: runtime.metricsEnabled ? 'green' : 'default' },
    { label: 'DLQ', value: formatEnabled(runtime.dlqEnabled), color: runtime.dlqEnabled ? 'green' : 'default' },
    { label: 'Parallelism', value: String(runtime.parallelism) },
    { label: 'Checkpoint', value: formatDurationMs(runtime.checkpointIntervalMs) },
    { label: 'Job', value: runtime.jobStatus || 'unknown', color: statusTagColor(runtime.jobStatus) },
    { label: 'Report', value: runtime.reportStatus || 'unknown', color: statusTagColor(runtime.reportStatus) }
  ];

  return (
    <Card size="small">
      <Space wrap size={[8, 8]}>
        {items.map((item) => (
          <span key={item.label} style={{ display: 'inline-flex', alignItems: 'center', gap: 4 }}>
            <Typography.Text type="secondary" style={{ fontSize: 12 }}>
              {item.label}
            </Typography.Text>
            <Tag
              color={item.color}
              style={{
                marginInlineEnd: 0,
                maxWidth: 260,
                overflow: 'hidden',
                textOverflow: 'ellipsis',
                whiteSpace: 'nowrap'
              }}
            >
              {item.value}
            </Tag>
          </span>
        ))}
      </Space>
    </Card>
  );
}

function BottleneckSummary({ candidates }: { candidates: BottleneckCandidate[] }) {
  return (
    <Card size="small" title="Bottleneck Candidates">
      {candidates.length > 0 ? (
        <Space wrap size={[8, 8]}>
          {candidates.map((candidate) => (
            <Tag key={candidate.key} color={candidate.color} style={{ marginInlineEnd: 0 }}>
              {candidate.label}
            </Tag>
          ))}
        </Space>
      ) : (
        <Typography.Text type="secondary">No current candidates</Typography.Text>
      )}
    </Card>
  );
}

function filterFlowStages(stages: StageStatus[], resultSink?: RuntimeConfig['resultSink']) {
  const normalizedSink = normalizeKnownResultSink(resultSink);
  if (!normalizedSink) {
    return stages;
  }
  if (normalizedSink === 'none') {
    return stages.filter((stage) => !isKnownSinkStage(stage.stageId));
  }
  const activeSinkPrefix = `${normalizedSink}-`;
  return stages.filter((stage) => {
    const stageId = stage.stageId.toLowerCase();
    return !isKnownSinkStage(stageId) || stageId.startsWith(activeSinkPrefix);
  });
}

function filterActiveSinks(sinks: SinkSummary[], resultSink?: RuntimeConfig['resultSink']) {
  const normalizedSink = normalizeKnownResultSink(resultSink);
  if (!normalizedSink) {
    return sinks;
  }
  if (normalizedSink === 'none') {
    return sinks.filter((sink) => !isKnownSinkStage(sink.sink));
  }
  const activeSinkPrefix = `${normalizedSink}-`;
  return sinks.filter((sink) => {
    const sinkId = sink.sink.toLowerCase();
    return !isKnownSinkStage(sinkId) || sinkId.startsWith(activeSinkPrefix);
  });
}

function isKnownSinkStage(stageId: string) {
  const normalized = stageId.toLowerCase();
  return knownSinkStagePrefixes.some((prefix) => normalized.startsWith(prefix));
}

function normalizeKnownResultSink(resultSink?: RuntimeConfig['resultSink']) {
  const normalized = String(resultSink ?? '').trim().toLowerCase();
  return knownResultSinkValues.has(normalized) ? normalized : undefined;
}

function resolveBottleneckCandidates(stages: StageStatus[], sinks: SinkSummary[]) {
  const candidates: BottleneckCandidate[] = [];
  const addCandidate = (candidate: BottleneckCandidate) => {
    if (!candidates.some((existing) => existing.key === candidate.key)) {
      candidates.push(candidate);
    }
  };

  stages.forEach((stage) => {
    const label = stage.label || stage.stageId;
    if (stage.status === 'critical' || stage.status === 'warning') {
      addCandidate({
        key: `stage-status-${stage.stageId}`,
        label: `${label} ${stage.status}`,
        color: stage.status === 'critical' ? 'red' : 'orange'
      });
    }
    if (stage.latencyP95Ms >= stageLatencyThresholdMs) {
      addCandidate({
        key: `stage-p95-${stage.stageId}`,
        label: `${label} P95 ${formatDurationMs(stage.latencyP95Ms)}`,
        color: 'orange'
      });
    }
    if (stage.watermarkLagMs >= watermarkLagThresholdMs) {
      addCandidate({
        key: `stage-watermark-${stage.stageId}`,
        label: `${label} WM ${formatDurationMs(stage.watermarkLagMs)}`,
        color: 'gold'
      });
    }
    if (stage.dlqCount > 0) {
      addCandidate({
        key: `stage-dlq-${stage.stageId}`,
        label: `${label} DLQ ${stage.dlqCount.toLocaleString()}`,
        color: 'red'
      });
    }
  });

  sinks.forEach((sink) => {
    const label = `${sink.sink} ${sink.window}`;
    if (sink.status === 'critical' || sink.status === 'warning') {
      addCandidate({
        key: `sink-status-${sink.sink}-${sink.window}`,
        label: `${label} ${sink.status}`,
        color: sink.status === 'critical' ? 'red' : 'orange'
      });
    }
    if (sink.writeLatencyP95Ms >= stageLatencyThresholdMs) {
      addCandidate({
        key: `sink-p95-${sink.sink}-${sink.window}`,
        label: `${label} write P95 ${formatDurationMs(sink.writeLatencyP95Ms)}`,
        color: 'orange'
      });
    }
  });

  return candidates.slice(0, maxBottleneckCandidates);
}

function formatEnabled(enabled: boolean) {
  return enabled ? 'on' : 'off';
}

function formatErrorMessage(reason: unknown) {
  return reason instanceof Error ? reason.message : String(reason);
}

function formatDurationMs(value: number) {
  if (!Number.isFinite(value)) {
    return '-';
  }
  if (value >= 60_000) {
    return `${(value / 60_000).toLocaleString(undefined, { maximumFractionDigits: 1 })}m`;
  }
  if (value >= 1_000) {
    return `${(value / 1_000).toLocaleString(undefined, { maximumFractionDigits: 1 })}s`;
  }
  return `${value.toLocaleString(undefined, { maximumFractionDigits: 0 })}ms`;
}

function statusTagColor(status: string) {
  switch (status?.toLowerCase()) {
    case 'ready':
    case 'running':
    case 'healthy':
    case 'success':
      return 'green';
    case 'failed':
    case 'error':
    case 'critical':
      return 'red';
    case 'warning':
      return 'orange';
    default:
      return 'default';
  }
}

import { Alert, Col, Row, Space, Typography } from 'antd';
import { useEffect, useState } from 'react';
import { fetchSinkSummaries, fetchSourceSummaries, fetchStageStatuses, subscribeObservabilityEvents } from '../api/client';
import SinkSummaryPanel from '../components/SinkSummaryPanel';
import SourceLatencyCard from '../components/SourceLatencyCard';
import StageStatusPanel from '../components/StageStatusPanel';
import StreamingFlowGraph from '../components/StreamingFlowGraph';
import type { SinkSummary, SourceSummary, StageStatus } from '../types/observability';

export default function FlowOverview() {
  const [sources, setSources] = useState<SourceSummary[]>([]);
  const [stages, setStages] = useState<StageStatus[]>([]);
  const [sinks, setSinks] = useState<SinkSummary[]>([]);
  const [error, setError] = useState<string>();

  useEffect(() => {
    let active = true;
    const refreshSummary = () => Promise.all([fetchSourceSummaries(), fetchStageStatuses(), fetchSinkSummaries()])
      .then(([nextSources, nextStages, nextSinks]) => {
        if (!active) {
          return;
        }
        setSources(nextSources);
        setStages(nextStages);
        setSinks(nextSinks);
        setError(undefined);
      })
      .catch((err: Error) => {
        if (active) {
          setError(err.message);
        }
      });
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
      <Row gutter={[12, 12]}>
        {sources.map((source) => (
          <Col xs={24} md={8} key={source.source}>
            <SourceLatencyCard source={source} />
          </Col>
        ))}
      </Row>
      <StreamingFlowGraph stages={stages} />
      <StageStatusPanel stages={stages} />
      <SinkSummaryPanel sinks={sinks} />
    </Space>
  );
}

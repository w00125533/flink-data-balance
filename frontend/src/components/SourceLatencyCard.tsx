import { Card, Statistic, Tag, Typography } from 'antd';
import type { SourceSummary } from '../types/observability';

const statusColor = {
  healthy: 'green',
  warning: 'orange',
  critical: 'red',
  idle: 'default',
  unknown: 'default'
} as const;

export default function SourceLatencyCard({ source }: { source: SourceSummary }) {
  return (
    <Card size="small" title={source.source.toUpperCase()} extra={<Tag color={statusColor[source.status] ?? 'default'}>{source.status}</Tag>}>
      <Statistic title="EPS" value={source.eps} precision={1} />
      <Typography.Text type="secondary">Kafka lag: {source.kafkaLag}</Typography.Text>
      <br />
      <Typography.Text type="secondary">事件延迟: {source.eventDelayMs} ms</Typography.Text>
      <Typography.Paragraph style={{ marginTop: 8, marginBottom: 0 }}>{source.summary}</Typography.Paragraph>
    </Card>
  );
}

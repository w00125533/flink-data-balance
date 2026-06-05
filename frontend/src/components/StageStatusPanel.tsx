import { Badge, Card, Table } from 'antd';
import type { ColumnsType } from 'antd/es/table';
import type { StageStatus } from '../types/observability';

const statusMap = {
  healthy: 'success',
  warning: 'warning',
  critical: 'error',
  idle: 'default',
  unknown: 'default'
} as const;

const columns: ColumnsType<StageStatus> = [
  {
    title: '阶段',
    dataIndex: 'label'
  },
  {
    title: '状态',
    dataIndex: 'status',
    render: (_, row) => <Badge status={statusMap[row.status] ?? 'default'} text={row.status} />
  },
  {
    title: 'In EPS',
    dataIndex: 'inEps',
    render: (value: number) => value.toFixed(1)
  },
  {
    title: 'Out EPS',
    dataIndex: 'outEps',
    render: (value: number) => value.toFixed(1)
  },
  {
    title: 'P50',
    dataIndex: 'latencyP50Ms',
    render: (value: number) => `${value} ms`
  },
  {
    title: 'P95',
    dataIndex: 'latencyP95Ms',
    render: (value: number) => `${value} ms`
  },
  {
    title: 'Watermark Lag',
    dataIndex: 'watermarkLagMs',
    render: (value: number) => `${value} ms`
  },
  {
    title: 'DLQ',
    dataIndex: 'dlqCount'
  },
  {
    title: '摘要',
    dataIndex: 'summary'
  }
];

export default function StageStatusPanel({ stages }: { stages: StageStatus[] }) {
  return (
    <Card size="small" title="阶段状态">
      <Table rowKey="stageId" size="small" pagination={false} columns={columns} dataSource={stages} />
    </Card>
  );
}

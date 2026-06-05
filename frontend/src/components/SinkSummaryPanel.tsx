import { Badge, Card, Table } from 'antd';
import type { ColumnsType } from 'antd/es/table';
import type { SinkSummary } from '../types/observability';

const statusMap = {
  healthy: 'success',
  warning: 'warning',
  critical: 'error',
  idle: 'default',
  unknown: 'default'
} as const;

const columns: ColumnsType<SinkSummary> = [
  {
    title: 'Sink',
    dataIndex: 'sink',
    render: (value: string) => value.toUpperCase()
  },
  {
    title: '窗口',
    dataIndex: 'window'
  },
  {
    title: '状态',
    dataIndex: 'status',
    render: (_, row) => <Badge status={statusMap[row.status] ?? 'default'} text={row.status} />
  },
  {
    title: '写入行数',
    dataIndex: 'rowsWritten'
  },
  {
    title: '写入 P95',
    dataIndex: 'writeLatencyP95Ms',
    render: (value: number) => `${value} ms`
  },
  {
    title: '摘要',
    dataIndex: 'summary'
  }
];

export default function SinkSummaryPanel({ sinks }: { sinks: SinkSummary[] }) {
  return (
    <Card size="small" title="Sink 写入性能">
      <Table rowKey={(row) => `${row.sink}-${row.window}`} size="small" pagination={false} columns={columns} dataSource={sinks} />
    </Card>
  );
}

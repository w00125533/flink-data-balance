import { Alert, Space, Table, Typography } from 'antd';
import type { ColumnsType } from 'antd/es/table';
import { useEffect, useState } from 'react';
import { fetchSinkLatency } from '../api/client';
import type { SinkLatencySummary } from '../types/observability';

const columns: ColumnsType<SinkLatencySummary> = [
  { title: 'sinkName', dataIndex: 'sinkName', width: 190 },
  { title: 'sinkType', dataIndex: 'sinkType', width: 110 },
  { title: 'dataset', dataIndex: 'dataset', width: 160 },
  { title: 'windowKind', dataIndex: 'windowKind', width: 120 },
  { title: 'records', dataIndex: 'records', align: 'right', width: 110 },
  { title: 'bytes', dataIndex: 'bytes', align: 'right', width: 110 },
  { title: 'durationMs', dataIndex: 'durationMs', align: 'right', width: 120 },
  { title: 'p50Ms', dataIndex: 'p50Ms', align: 'right', width: 100 },
  { title: 'p95Ms', dataIndex: 'p95Ms', align: 'right', width: 100 },
  { title: 'p99Ms', dataIndex: 'p99Ms', align: 'right', width: 100 },
  { title: 'failureCount', dataIndex: 'failureCount', align: 'right', width: 130 },
  { title: 'checkpointId', dataIndex: 'checkpointId', align: 'right', width: 130 },
  { title: 'updatedAt', dataIndex: 'updatedAt', width: 180 }
];

export default function SinkLatency() {
  const [rows, setRows] = useState<SinkLatencySummary[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string>();

  useEffect(() => {
    let active = true;
    setLoading(true);
    fetchSinkLatency()
      .then((nextRows) => {
        if (active) {
          setRows(nextRows);
          setError(undefined);
        }
      })
      .catch((err: Error) => {
        if (active) {
          setError(err.message);
        }
      })
      .finally(() => {
        if (active) {
          setLoading(false);
        }
      });
    return () => {
      active = false;
    };
  }, []);

  return (
    <Space direction="vertical" size={12} style={{ width: '100%' }}>
      <Typography.Title level={3} style={{ margin: 0 }}>
        Sink 耗时
      </Typography.Title>
      {error ? <Alert type="error" message="Sink 耗时查询失败" description={error} /> : null}
      <Table<SinkLatencySummary>
        size="small"
        rowKey={(record) => `${record.sinkName}-${record.dataset}-${record.windowKind}`}
        loading={loading}
        dataSource={rows}
        columns={columns}
        scroll={{ x: 'max-content' }}
        pagination={{ pageSize: 20, showSizeChanger: true }}
      />
    </Space>
  );
}

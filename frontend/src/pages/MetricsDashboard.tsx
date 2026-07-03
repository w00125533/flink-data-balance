import { Alert, Space, Table, Tag, Typography } from 'antd';
import { useEffect, useState } from 'react';
import { fetchSinkSummaries } from '../api/client';
import type { SinkSummary, StageStatusValue } from '../types/observability';

function statusColor(status: StageStatusValue) {
  if (status === 'healthy') {
    return 'green';
  }
  if (status === 'warning') {
    return 'orange';
  }
  if (status === 'critical') {
    return 'red';
  }
  return 'default';
}

export default function MetricsDashboard() {
  const [sinks, setSinks] = useState<SinkSummary[]>([]);
  const [error, setError] = useState<string>();

  useEffect(() => {
    let active = true;
    const refresh = () => fetchSinkSummaries()
      .then((nextSinks) => {
        if (active) {
          setSinks(nextSinks);
          setError(undefined);
        }
      })
      .catch((err: Error) => {
        if (active) {
          setError(err.message);
        }
      });
    void refresh();
    const timer = window.setInterval(() => {
      void refresh();
    }, 10_000);
    return () => {
      active = false;
      window.clearInterval(timer);
    };
  }, []);

  return (
    <Space direction="vertical" size={16} style={{ width: '100%' }}>
      <Typography.Title level={3} style={{ margin: 0 }}>
        指标面板
      </Typography.Title>
      {error ? <Alert type="error" message="Sink 指标请求失败" description={error} /> : null}
      <Table<SinkSummary>
        size="small"
        rowKey={(record) => `${record.sink}-${record.window}`}
        dataSource={sinks}
        pagination={false}
        columns={[
          {
            title: 'Sink',
            dataIndex: 'sink'
          },
          {
            title: '窗口',
            dataIndex: 'window'
          },
          {
            title: '状态',
            dataIndex: 'status',
            render: (status: StageStatusValue) => <Tag color={statusColor(status)}>{status}</Tag>
          },
          {
            title: '写入行数',
            dataIndex: 'rowsWritten',
            align: 'right',
            render: (value: number) => value.toLocaleString()
          },
          {
            title: 'P95 写入延迟',
            dataIndex: 'writeLatencyP95Ms',
            align: 'right',
            render: (value: number) => `${value.toLocaleString()} ms`
          },
          {
            title: '摘要',
            dataIndex: 'summary'
          },
          {
            title: '更新时间',
            dataIndex: 'updatedAt',
            render: (value: string) => new Date(value).toLocaleString()
          }
        ]}
      />
    </Space>
  );
}

import { Alert, Badge, Card, Col, Descriptions, Empty, List, Row, Space, Table, Typography } from 'antd';
import type { ColumnsType } from 'antd/es/table';
import { useEffect, useMemo, useState } from 'react';
import { fetchExecutionRunDetail, fetchExecutionRuns } from '../api/client';
import type { ExecutionMetric, ExecutionRunDetail, ExecutionRunSummary } from '../types/observability';

const statusMap: Record<string, 'success' | 'processing' | 'error' | 'default'> = {
  success: 'success',
  running: 'processing',
  failed: 'error',
  unknown: 'default'
};

const columns: ColumnsType<ExecutionMetric> = [
  {
    title: '分组',
    dataIndex: 'section',
    width: 180
  },
  {
    title: '指标',
    dataIndex: 'metric',
    width: 260
  },
  {
    title: '值',
    dataIndex: 'value',
    render: (value: string) => <Typography.Text copyable={{ text: value }}>{value}</Typography.Text>
  }
];

export default function ExecutionHistory() {
  const [runs, setRuns] = useState<ExecutionRunSummary[]>([]);
  const [selectedRunId, setSelectedRunId] = useState<string>();
  const [detail, setDetail] = useState<ExecutionRunDetail>();
  const [error, setError] = useState<string>();

  useEffect(() => {
    let active = true;
    fetchExecutionRuns()
      .then((nextRuns) => {
        if (!active) {
          return;
        }
        setRuns(nextRuns);
        setSelectedRunId((current) => current ?? nextRuns[0]?.runId);
        setError(undefined);
      })
      .catch((err: Error) => {
        if (active) {
          setError(err.message);
        }
      });
    return () => {
      active = false;
    };
  }, []);

  useEffect(() => {
    if (!selectedRunId) {
      setDetail(undefined);
      return;
    }
    let active = true;
    fetchExecutionRunDetail(selectedRunId)
      .then((nextDetail) => {
        if (active) {
          setDetail(nextDetail);
          setError(undefined);
        }
      })
      .catch((err: Error) => {
        if (active) {
          setError(err.message);
        }
      });
    return () => {
      active = false;
    };
  }, [selectedRunId]);

  const selectedRun = useMemo(
    () => runs.find((run) => run.runId === selectedRunId),
    [runs, selectedRunId]
  );

  return (
    <Space direction="vertical" size={16} style={{ width: '100%' }}>
      <Typography.Title level={3} style={{ margin: 0 }}>
        执行历史
      </Typography.Title>
      {error ? <Alert type="error" message="执行历史请求失败" description={error} /> : null}
      <Row gutter={[16, 16]}>
        <Col xs={24} lg={7} xl={6}>
          <Card size="small" title="执行列表" styles={{ body: { padding: 0 } }}>
            {runs.length === 0 ? (
              <Empty description="暂无执行记录" style={{ padding: 24 }} />
            ) : (
              <List
                dataSource={runs}
                renderItem={(run) => (
                  <List.Item
                    onClick={() => setSelectedRunId(run.runId)}
                    style={{
                      cursor: 'pointer',
                      padding: '12px 16px',
                      background: run.runId === selectedRunId ? '#eef6ff' : undefined
                    }}
                  >
                    <List.Item.Meta
                      title={
                        <Space>
                          <Badge status={statusMap[run.status] ?? 'default'} />
                          <Typography.Text strong>{run.runId}</Typography.Text>
                        </Space>
                      }
                      description={
                        <Space direction="vertical" size={2}>
                          <Typography.Text type="secondary">{formatTime(run.startedAt)}</Typography.Text>
                          <Typography.Text type="secondary">{run.metricCount} metrics</Typography.Text>
                        </Space>
                      }
                    />
                  </List.Item>
                )}
              />
            )}
          </Card>
        </Col>
        <Col xs={24} lg={17} xl={18}>
          <Card size="small" title={selectedRun ? `执行详情: ${selectedRun.runId}` : '执行详情'}>
            {!detail ? (
              <Empty description="请选择一次执行" />
            ) : (
              <Space direction="vertical" size={16} style={{ width: '100%' }}>
                <Descriptions size="small" column={2} bordered>
                  <Descriptions.Item label="状态">
                    <Badge status={statusMap[detail.status] ?? 'default'} text={detail.status} />
                  </Descriptions.Item>
                  <Descriptions.Item label="指标数">{detail.metrics.length}</Descriptions.Item>
                  <Descriptions.Item label="开始时间">{formatTime(detail.startedAt)}</Descriptions.Item>
                  <Descriptions.Item label="结束时间">{formatTime(detail.completedAt)}</Descriptions.Item>
                </Descriptions>
                <Table
                  rowKey={(row, index) => `${row.section}-${row.metric}-${index}`}
                  size="small"
                  pagination={{ pageSize: 20 }}
                  columns={columns}
                  dataSource={detail.metrics}
                />
              </Space>
            )}
          </Card>
        </Col>
      </Row>
    </Space>
  );
}

function formatTime(value?: string) {
  if (!value) {
    return '-';
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  return date.toLocaleString();
}

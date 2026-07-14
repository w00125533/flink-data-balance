import { Alert, Button, Card, Form, Input, InputNumber, Select, Space, Table, Typography } from 'antd';
import type { ColumnsType } from 'antd/es/table';
import { useEffect, useState } from 'react';
import { fetchCellAnomalies } from '../api/client';
import type { AnomalyQueryParams, AnomalyResultRow } from '../types/observability';

const NULL_KEY_PART = '<null>';

const columns: ColumnsType<AnomalyResultRow> = [
  { title: 'detectionTs', dataIndex: 'detectionTs', width: 150 },
  { title: 'eventTs', dataIndex: 'eventTs', width: 150 },
  { title: 'siteId', dataIndex: 'siteId', width: 130 },
  { title: 'cellId', dataIndex: 'cellId', width: 140 },
  { title: 'gridId', dataIndex: 'gridId', width: 130 },
  { title: 'anomalyType', dataIndex: 'anomalyType', width: 160 },
  { title: 'severity', dataIndex: 'severity', width: 110 },
  { title: 'latitude', dataIndex: 'latitude', align: 'right', width: 110 },
  { title: 'longitude', dataIndex: 'longitude', align: 'right', width: 110 },
  { title: 'ruleVersion', dataIndex: 'ruleVersion', width: 120 },
  { title: 'contextJson', dataIndex: 'contextJson', width: 280, ellipsis: true }
];

export default function CellAnomalies() {
  const [form] = Form.useForm<AnomalyQueryParams>();
  const [rows, setRows] = useState<AnomalyResultRow[]>([]);
  const [params, setParams] = useState<AnomalyQueryParams>({});
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string>();

  useEffect(() => {
    let active = true;
    setLoading(true);
    fetchCellAnomalies(params)
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
  }, [params]);

  return (
    <Space direction="vertical" size={12} style={{ width: '100%' }}>
      <Typography.Title level={3} style={{ margin: 0 }}>
        小区异常
      </Typography.Title>
      {error ? <Alert type="error" message="小区异常查询失败" description={error} /> : null}
      <Card size="small">
        <Form<AnomalyQueryParams>
          form={form}
          layout="inline"
          onFinish={(values) => setParams(cleanParams(values))}
          style={{ rowGap: 8 }}
        >
          <Form.Item label="startTs" name="startTs">
            <InputNumber placeholder="epoch ms" style={{ width: 150 }} />
          </Form.Item>
          <Form.Item label="endTs" name="endTs">
            <InputNumber placeholder="epoch ms" style={{ width: 150 }} />
          </Form.Item>
          <Form.Item label="siteId" name="siteId">
            <Input placeholder="SITE-001" allowClear style={{ width: 130 }} />
          </Form.Item>
          <Form.Item label="cellId" name="cellId">
            <Input placeholder="CELL-001" allowClear style={{ width: 140 }} />
          </Form.Item>
          <Form.Item label="severity" name="severity">
            <Select
              allowClear
              placeholder="all"
              style={{ width: 120 }}
              options={[
                { value: 'LOW', label: 'LOW' },
                { value: 'MEDIUM', label: 'MEDIUM' },
                { value: 'HIGH', label: 'HIGH' }
              ]}
            />
          </Form.Item>
          <Form.Item label="anomalyType" name="anomalyType">
            <Input placeholder="LOW_SIGNAL" allowClear style={{ width: 150 }} />
          </Form.Item>
          <Form.Item label="limit" name="limit">
            <InputNumber min={1} max={1000} placeholder="100" style={{ width: 100 }} />
          </Form.Item>
          <Form.Item>
            <Space size={8}>
              <Button type="primary" htmlType="submit">
                查询
              </Button>
              <Button
                onClick={() => {
                  form.resetFields();
                  setParams({});
                }}
              >
                重置
              </Button>
            </Space>
          </Form.Item>
        </Form>
      </Card>
      <Table<AnomalyResultRow>
        size="small"
        rowKey={anomalyRowKey}
        loading={loading}
        dataSource={rows}
        columns={columns}
        scroll={{ x: 'max-content' }}
        pagination={{ pageSize: 20, showSizeChanger: true }}
      />
    </Space>
  );
}

export function anomalyRowKey(
  record: Pick<
    AnomalyResultRow,
    'detectionTs' | 'eventTs' | 'siteId' | 'cellId' | 'gridId' | 'anomalyType' | 'severity'
  >
): string {
  return [
    record.detectionTs,
    record.eventTs,
    record.siteId,
    record.cellId,
    record.gridId,
    record.anomalyType,
    record.severity
  ].map(keyPart).join('-');
}

function keyPart(value: unknown): string {
  return String(value ?? NULL_KEY_PART);
}

function cleanParams(values: AnomalyQueryParams): AnomalyQueryParams {
  return Object.fromEntries(
    Object.entries(values).filter(([, value]) => value !== undefined && value !== null && value !== '')
  ) as AnomalyQueryParams;
}

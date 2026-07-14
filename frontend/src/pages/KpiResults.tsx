import { Alert, Button, Card, Form, Input, InputNumber, Select, Space, Table, Typography } from 'antd';
import type { ColumnsType } from 'antd/es/table';
import { useEffect, useState } from 'react';
import { fetchKpiResults } from '../api/client';
import type { KpiResultRow, ResultQueryParams } from '../types/observability';

const NULL_KEY_PART = '<null>';

const columns: ColumnsType<KpiResultRow> = [
  { title: 'windowStartTs', dataIndex: 'windowStartTs', width: 150 },
  { title: 'windowEndTs', dataIndex: 'windowEndTs', width: 150 },
  { title: 'windowKind', dataIndex: 'windowKind', width: 120 },
  { title: 'joinQuality', dataIndex: 'joinQuality', width: 130 },
  { title: 'siteId', dataIndex: 'siteId', width: 130 },
  { title: 'cellId', dataIndex: 'cellId', width: 140 },
  { title: 'gridId', dataIndex: 'gridId', width: 130 },
  { title: 'numChrEvents', dataIndex: 'numChrEvents', align: 'right', width: 130 },
  { title: 'numUsers', dataIndex: 'numUsers', align: 'right', width: 110 },
  { title: 'avgRsrp', dataIndex: 'avgRsrp', align: 'right', width: 110 },
  { title: 'avgSinr', dataIndex: 'avgSinr', align: 'right', width: 110 },
  { title: 'avgPrbUsageDl', dataIndex: 'avgPrbUsageDl', align: 'right', width: 140 },
  { title: 'throughputDlMbpsAvg', dataIndex: 'throughputDlMbpsAvg', align: 'right', width: 180 },
  { title: 'dropRate', dataIndex: 'dropRate', align: 'right', width: 110 },
  { title: 'hoSuccessRate', dataIndex: 'hoSuccessRate', align: 'right', width: 140 },
  { title: 'attachSuccessRate', dataIndex: 'attachSuccessRate', align: 'right', width: 160 }
];

export default function KpiResults({ windowKind }: { windowKind: '1m' | '5m' }) {
  const [form] = Form.useForm<ResultQueryParams>();
  const [rows, setRows] = useState<KpiResultRow[]>([]);
  const [params, setParams] = useState<ResultQueryParams>({});
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string>();

  useEffect(() => {
    let active = true;
    setLoading(true);
    fetchKpiResults(windowKind, params)
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
  }, [params, windowKind]);

  const title = `KPI ${windowKind}`;

  return (
    <Space direction="vertical" size={12} style={{ width: '100%' }}>
      <Typography.Title level={3} style={{ margin: 0 }}>
        {title}
      </Typography.Title>
      {error ? <Alert type="error" message={`${title} 查询失败`} description={error} /> : null}
      <Card size="small">
        <Form<ResultQueryParams>
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
          <Form.Item label="joinQuality" name="joinQuality">
            <Select
              allowClear
              placeholder="all"
              style={{ width: 140 }}
              options={[
                { value: 'JOINED', label: 'JOINED' },
                { value: 'CHR_ONLY', label: 'CHR_ONLY' },
                { value: 'PM_ONLY', label: 'PM_ONLY' }
              ]}
            />
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
      <Table<KpiResultRow>
        size="small"
        rowKey={kpiRowKey}
        loading={loading}
        dataSource={rows}
        columns={columns}
        scroll={{ x: 'max-content' }}
        pagination={{ pageSize: 20, showSizeChanger: true }}
      />
    </Space>
  );
}

export function kpiRowKey(
  record: Pick<KpiResultRow, 'windowStartTs' | 'windowEndTs' | 'cellId' | 'windowKind' | 'joinQuality'>
): string {
  return [
    record.windowStartTs,
    record.windowEndTs,
    record.cellId,
    record.windowKind,
    record.joinQuality
  ].map(keyPart).join('-');
}

function keyPart(value: unknown): string {
  return String(value ?? NULL_KEY_PART);
}

function cleanParams(values: ResultQueryParams): ResultQueryParams {
  return Object.fromEntries(
    Object.entries(values).filter(([, value]) => value !== undefined && value !== null && value !== '')
  ) as ResultQueryParams;
}

import { Alert, Button, Card, Form, Input, InputNumber, Select, Space, Table, Typography } from 'antd';
import type { ColumnsType } from 'antd/es/table';
import type { CSSProperties } from 'react';
import { useEffect, useMemo, useState } from 'react';
import { fetchGridAnomalies } from '../api/client';
import type { AnomalyQueryParams, AnomalyResultRow } from '../types/observability';

const NULL_KEY_PART = '<null>';

const columns: ColumnsType<AnomalyResultRow> = [
  { title: 'detectionTs', dataIndex: 'detectionTs', width: 150 },
  { title: 'eventTs', dataIndex: 'eventTs', width: 150 },
  { title: 'gridId', dataIndex: 'gridId', width: 140 },
  { title: 'anomalyType', dataIndex: 'anomalyType', width: 160 },
  { title: 'severity', dataIndex: 'severity', width: 110 },
  { title: 'latitude', dataIndex: 'latitude', align: 'right', width: 110 },
  { title: 'longitude', dataIndex: 'longitude', align: 'right', width: 110 },
  { title: 'siteId', dataIndex: 'siteId', width: 130 },
  { title: 'cellId', dataIndex: 'cellId', width: 140 },
  { title: 'ruleVersion', dataIndex: 'ruleVersion', width: 120 },
  { title: 'contextJson', dataIndex: 'contextJson', width: 280, ellipsis: true }
];

export default function GridAnomalies() {
  const [form] = Form.useForm<AnomalyQueryParams>();
  const [rows, setRows] = useState<AnomalyResultRow[]>([]);
  const [params, setParams] = useState<AnomalyQueryParams>({});
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string>();

  useEffect(() => {
    let active = true;
    setLoading(true);
    fetchGridAnomalies(params)
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
        栅格异常
      </Typography.Title>
      {error ? <Alert type="error" message="栅格异常查询失败" description={error} /> : null}
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
          <Form.Item label="gridId" name="gridId">
            <Input placeholder="wx4g0e" allowClear style={{ width: 140 }} />
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
            <Input placeholder="COVERAGE_HOLE" allowClear style={{ width: 170 }} />
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
      <GridPreview rows={rows} />
      <Table<AnomalyResultRow>
        size="small"
        rowKey={gridAnomalyRowKey}
        loading={loading}
        dataSource={rows}
        columns={columns}
        scroll={{ x: 'max-content' }}
        pagination={{ pageSize: 20, showSizeChanger: true }}
      />
    </Space>
  );
}

function GridPreview({ rows }: { rows: AnomalyResultRow[] }) {
  const points = useMemo(
    () => rows.filter((row) => typeof row.latitude === 'number' && typeof row.longitude === 'number'),
    [rows]
  );

  if (points.length === 0) {
    return null;
  }

  const latitudes = points.map((row) => row.latitude as number);
  const longitudes = points.map((row) => row.longitude as number);
  const minLat = Math.min(...latitudes);
  const maxLat = Math.max(...latitudes);
  const minLon = Math.min(...longitudes);
  const maxLon = Math.max(...longitudes);

  return (
    <Card size="small" title="坐标预览">
      <div style={previewStyle}>
        {points.map((point) => {
          const latitude = point.latitude as number;
          const longitude = point.longitude as number;
          const { left, top } = gridPointPosition(latitude, longitude, { minLat, maxLat, minLon, maxLon });
          return (
            <span
              key={gridAnomalyRowKey(point)}
              title={`${point.gridId ?? ''} ${latitude}, ${longitude}`}
              style={{
                ...pointStyle,
                left: `${left}%`,
                top: `${top}%`,
                background: point.severity === 'HIGH' ? '#cf1322' : point.severity === 'MEDIUM' ? '#d48806' : '#1677ff'
              }}
            />
          );
        })}
      </div>
    </Card>
  );
}

export function gridAnomalyRowKey(
  record: Pick<
    AnomalyResultRow,
    'detectionTs' | 'eventTs' | 'gridId' | 'anomalyType' | 'severity' | 'latitude' | 'longitude'
  >
): string {
  return [
    record.detectionTs,
    record.eventTs,
    record.gridId,
    record.anomalyType,
    record.severity,
    record.latitude,
    record.longitude
  ].map(keyPart).join('-');
}

export function gridPointPosition(
  latitude: number,
  longitude: number,
  bounds: { minLat: number; maxLat: number; minLon: number; maxLon: number }
): { left: number; top: number } {
  const latSpan = bounds.maxLat - bounds.minLat;
  const lonSpan = bounds.maxLon - bounds.minLon;
  return {
    left: lonSpan === 0 ? 50 : ((longitude - bounds.minLon) / lonSpan) * 92 + 4,
    top: latSpan === 0 ? 50 : 96 - ((latitude - bounds.minLat) / latSpan) * 92
  };
}

function keyPart(value: unknown): string {
  return String(value ?? NULL_KEY_PART);
}

function cleanParams(values: AnomalyQueryParams): AnomalyQueryParams {
  return Object.fromEntries(
    Object.entries(values).filter(([, value]) => value !== undefined && value !== null && value !== '')
  ) as AnomalyQueryParams;
}

const previewStyle: CSSProperties = {
  position: 'relative',
  height: 180,
  overflow: 'hidden',
  border: '1px solid #d9d9d9',
  borderRadius: 4,
  background: '#fafafa'
};

const pointStyle: CSSProperties = {
  position: 'absolute',
  width: 10,
  height: 10,
  borderRadius: 10,
  transform: 'translate(-50%, -50%)',
  border: '1px solid #fff',
  boxShadow: '0 0 0 1px rgba(0,0,0,0.18)'
};

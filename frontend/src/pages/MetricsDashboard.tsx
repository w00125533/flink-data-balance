import { Space, Typography } from 'antd';
import GrafanaEmbedPanel from '../components/GrafanaEmbedPanel';

export default function MetricsDashboard() {
  return (
    <Space direction="vertical" size={16} style={{ width: '100%' }}>
      <Typography.Title level={3} style={{ margin: 0 }}>
        指标面板
      </Typography.Title>
      <GrafanaEmbedPanel />
    </Space>
  );
}
